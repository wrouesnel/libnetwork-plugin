// The orchestrator component of the plugin provides a limited set of container orchestration functionality to allow
// the plugin to automatically manage and resolve IP address conflicts caused by unclean node shutdowns or network
// partitions.

package orchestration

import (
	"fmt"
	"sync"
	"time"
	"path"
	"strings"
	"net"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/etcd"

	"github.com/docker/leadership"
	//osutils "github.com/projectcalico/libnetwork-plugin/utils/os"
	datastoreClient "github.com/projectcalico/libcalico-go/lib/client"
	"github.com/projectcalico/libcalico-go/lib/api"
	"github.com/satori/go.uuid"
	dockerClient "github.com/docker/engine-api/client"
	"github.com/docker/engine-api/types"
	"golang.org/x/net/context"
)

func init() {
	etcd.Register()
}

const (
	// OrchestratorRootKey is the root etcd key the orchestrator uses.
	OrchestratorRootKey = "calico-libnetwork"
	IPSubkey = "ip"
)

// ErrAddressInUse is returned from ChallengeIP when an address can't be assigned.
type ErrAddressInUse struct {
	ip net.IP
}

func (this ErrAddressInUse) Error() string {
	return fmt.Sprintf("Address already in use: %s", this.ip.String())
}

// Orchestrator manages the collection of Calico IP endpoints the plugin has assigned by holding leadership
// on their IP addresses in the backend. It will also (optionally) kill docker workloads for which it loses control
// of the key.
type Orchestrator struct {
	calicoClient *datastoreClient.Client
	// backing store client
	client store.Store
	// IPs we know are launched and managed by us.
	ips map[string]chan<- interface{}
	// maximum time to wait for leadership
	challengeTimeout time.Duration
	// mutex to protect the IP map.
	ipsMtx sync.RWMutex
}

// NewOrchestrator initializes a new orchestrator. endpoints is a list of etcd endpoints, clusterConfig is the libkv
// storeConfig, challengeTimeout is the maximum amount of time to wait for responses to a challenge.
func NewOrchestrator(config *api.CalicoAPIConfig, calicoClient *datastoreClient.Client, challengeTimeout time.Duration) (*Orchestrator, error) {
	logCtx := log.WithField("component", "orchestrator")

	// Can't use the kubernetes backend with libkv yet. Might be an idea to add it.
	if config.Spec.DatastoreType != api.EtcdV2 {
		return nil, errors.New("orchestrator is only supported with etcdv2 backend at the moment.")
	}

	// Convert Calico config spec to endpoints...
	var endpoints []string
	if config.Spec.EtcdEndpoints != "" {
		endpoints = strings.Split(config.Spec.EtcdEndpoints, ",")
	} else if config.Spec.EtcdAuthority != "" {
		endpoints = []string{config.Spec.EtcdAuthority}
	} else {
		return nil, errors.New("no etcd endpoint specified in ETCD_ENDPOINTS or ETCD_AUTHORITY.")
	}

	logCtx.Debugln("etcd endpoints:", endpoints)

	var clusterTLS *store.ClientTLSConfig
	if config.Spec.EtcdScheme == "https" {
		clusterTLS = &store.ClientTLSConfig{
			CertFile: config.Spec.EtcdCertFile,
			KeyFile: config.Spec.EtcdKeyFile,
			CACertFile: config.Spec.EtcdCACertFile,
		}
	}

	// Convert Calico config spec to store.Config
	clusterConfig := &store.Config{
		ClientTLS: clusterTLS,
		Username: config.Spec.EtcdUsername,
		Password: config.Spec.EtcdPassword,
	}

	client, err := libkv.NewStore(store.ETCD, endpoints, clusterConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "Error setting up orchestrator datastore")
	}

	testKey := path.Join(OrchestratorRootKey, uuid.NewV4().String())
	logCtx.Debugln("Writing test key:", testKey)
	if err := client.Put(testKey, []byte{}, &store.WriteOptions{false, challengeTimeout}); err != nil {
		return nil, errors.Wrapf(err, "Error writing a test key to data store.")
	}

	return &Orchestrator{
		calicoClient: calicoClient,
		client: client,
		ips: make(map[string]chan<- interface{}),
		challengeTimeout: challengeTimeout,
	}, nil
}

func (this *Orchestrator) log() *log.Entry {
	return log.WithField("component", "orchestrator")
}

// ChallengeIP informs the orchestrator to start a leadership challenge for an
// IP address. If it succeeds then it returns nil, otherwise an error.
// Successful elections spawn a goroutine which continues to hold the leadership
// until requested to terminate.
func (this *Orchestrator) ChallengeIP(ip net.IP) error {
	// Each IP challenge is functionally a new request, and needs a unique ID
	// for the contender (even if it might be a local one).
	contenderId := uuid.NewV4().String()
	// The store path is a key under the orchestrator key which is just the IP
	// address.
	electionKey := path.Join(OrchestratorRootKey, IPSubkey, ip.String())

	logCtx := this.log().WithField("ip", ip).
		WithField("contenderId", contenderId).
		WithField("electionKey", electionKey)

	// Create a new candidate request
	candidate := leadership.NewCandidate(this.client, electionKey,
		contenderId, this.challengeTimeout)

	logCtx.Infoln("Starting IP address election")
	electedCh, errCh := candidate.RunForElection()
	// Leadership should always produce an initial failure, but we might also
	// get an error. If we get an error then we want to just fail the workload
	// and let whoever wants to start it know right away.
	select {
	case err := <- errCh:
		logCtx.Debugln("Error before election:", err)
		return errors.Wrapf(err, "Error before election for IP: %s", ip.String())
	case firstResult := <- electedCh:
		// Discard since it will always be false.
		logCtx.Debugln("Discarding first election result:", firstResult)
	}

	logCtx.Debugln("Waiting for real election...")
	// Wait for the real election result to see if anyone else wants the IP.
	var electionSuccess bool
	select {
	case err := <- errCh:
		logCtx.Debugln("Error before election:", err)
		return errors.Wrapf(err, "Error before election for IP: %s", ip.String())
	case electionSuccess = <- electedCh:
		logCtx.Debugln("Election Result:", electionSuccess)
	}

	if !electionSuccess {
		logCtx.Infoln("Lost election for IP.")
		return &ErrAddressInUse{
			ip : ip,
		}
	}
	logCtx.Infoln("Won election for IP.")

	doneCh := make(chan interface{})
	go func(ip net.IP) {
		for {
			innerLoop: for {
				select {
				case electionSuccess := <-electedCh:
					if electionSuccess {
						continue
					}
					logCtx.Warnln("Lost election on owned IP.")
					// TODO: Lost leadership. Release IP (if we have it) in Calico.
					dockerCli, err := dockerClient.NewEnvClient()
					if err != nil {
						err = errors.Wrap(err, "Error while attempting to instantiate docker client from env")
						logCtx.Errorln(err)
						return
					}
					// List all containers (since we don't know which containers
					// we were associated with) and kill any which are running
					// with the IP we have.
					containers, err := dockerCli.ContainerList(context.Background(),
						types.ContainerListOptions{})

					if err != nil {
						// TODO: dump these into a list so we can retry until we succeed
						err = errors.Wrap(err, "Error listing containers to remove.")
						logCtx.Errorln(err)
					}
					// This could be cleaned up, but I have no idea how which
					// of these many addresses will be populated, or who thorough
					// we should be. Since this is Calico, we're assuming all
					// containers on this host should be the right.
					for _, container := range containers {
						if container.NetworkSettings == nil {
							continue
						}
						if container.NetworkSettings.Networks == nil {
							continue
						}
						killContainer := false
						for _, nsettings := range container.NetworkSettings.Networks {
							switch ip.String() {
							case nsettings.IPAddress, nsettings.GlobalIPv6Address:
								killContainer = true
								break
							}

							if nsettings.IPAMConfig == nil {
								continue
							}
							switch ip.String() {
							case nsettings.IPAMConfig.IPv4Address, nsettings.IPAMConfig.IPv6Address:
								killContainer = true
								break
							}
						}
						// Container on this host has this IP and it shouldn't
						// kill it.
						if killContainer {
							if err := dockerCli.ContainerKill(context.Background(), container.ID, "SIGKILL"); err != nil {
								log.Errorln(errors.Wrapf(err, "Error sending container kill."))
							}
						}
					}
				case err := <-errCh:
					logCtx.Errorln("Error during election: %v", err)
					break innerLoop
				case <-doneCh:
					logCtx.Infoln("Workload shutting down and stopping contention.")
					candidate.Stop()
					return
				}
			}
			logCtx.Infoln("Restarting workload contention due to backend error")
			electedCh, errCh = candidate.RunForElection()
		}
	}(ip)

	// Add the done channel to the interface so we can signal releases.
	this.ipsMtx.Lock()
	this.ips[ip.String()] = doneCh
	this.ipsMtx.Unlock()
	logCtx.Infoln("Add IP to orchestrator.")

	return nil
}

func (this *Orchestrator) ReleaseIP(ip net.IP) error {
	logCtx := this.log().WithField("ip", ip)
	logCtx.Debugln("Release IP")

	this.ipsMtx.Lock()
	defer this.ipsMtx.Unlock()

	ipStr := ip.String()

	doneCh, found := this.ips[ipStr]
	if !found {
		logCtx.Warnln("Called release on IP we didn't own.")
		return nil
	}
	// Remove the IP
	delete(this.ips, ipStr)
	// Close the channel (signal resign IP contention)
	close(doneCh)

	return nil
}