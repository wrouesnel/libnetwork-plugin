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
	ip string
}

func (this ErrAddressInUse) Error() string {
	return fmt.Sprintf("Address already in use: %s", this.ip)
}

// An orchestrator manages the collection of Calico IP endpoints the plugin has assigned by holding leadership
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
	// if set, the orchestrator will scan for and kill workloads for which it has lost control of an IP globally.
	enableKillWorkloads bool
	// mutex to protect the IP map.
	ipsMtx sync.RWMutex
}

// NewOrchestrator initializes a new orchestrator. endpoints is a list of etcd endpoints, clusterConfig is the libkv
// storeConfig, challengeTimeout is the maximum amount of time to wait for responses to a challenge.
func NewOrchestrator(config *api.CalicoAPIConfig, calicoClient *datastoreClient.Client, challengeTimeout time.Duration, enableKillWorkloads bool) (*Orchestrator, error) {
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
		enableKillWorkloads: enableKillWorkloads,
	}, nil
}

func (this *Orchestrator) log() *log.Entry {
	return log.WithField("component", "orchestrator")
}

// ChallengeIP informs the orchestrator to start a leadership challenge for an IP address. If it succeeds then it
// returns nil, otherwise an error. Successful elections spawn a goroutine which continues to hold the leadership
// until requested to terminate.
func (this *Orchestrator) ChallengeIP(ip net.IP) error {
	logCtx := this.log().WithField("ip", ip)

	defer func() {
		if recover() != nil {
			logCtx.Debugln("Caught panic in ChallengeIP")
		}
	}()

	ipStr := ip.String()
	// Calculate unique IP store path.
	storePath := path.Join(OrchestratorRootKey, IPSubkey, ipStr)
	log.Debugln("Store path:", storePath)

	// Note: could consider doing a load short-circuit here if the IP is local.

	// Create a new candidate request
	candidate := leadership.NewCandidate(this.client, storePath, uuid.NewV4().String(), this.challengeTimeout)

	logCtx.Debugln("Running for election")
	electedCh, errCh := candidate.RunForElection()
	// Leadership should always produce an initial failure, but we might also get an error. If we get an error then
	// we want to just fail the workload.
	select {
	case err := <- errCh:
		logCtx.Debugln("Error before election:", err)
		return errors.Wrapf(err, "Error before election for IP: %s", ipStr)
	case firstResult := <- electedCh:
		// Discard since it will always be false.
		logCtx.Debugln("Discarding first election result:", firstResult)
	}

	logCtx.Debugln("Waiting for leadership challenge")
	// Wait for the real election result to see if anyone else wants the IP.
	var electionSuccess bool
	select {
	case err := <- errCh:
		logCtx.Debugln("Error before election:", err)
		return errors.Wrapf(err, "Error before election for IP: %s", ipStr)
	case electionSuccess = <- electedCh:
		logCtx.Debugln("Election Result:", electionSuccess)
	}

	if !electionSuccess {
		logCtx.Debugln("Lost election.")
		return &ErrAddressInUse{
			ip : ipStr,
		}
	}
	logCtx.Debugln("Won election.")

	// We won the election, but we need to hold onto the IP for the workload duration.
	doneCh := make(chan interface{})
	go func() {
		for {
			select {
			case electionSuccess := <- electedCh:
				// Somehow we lost leadership, which means another orchestrator is going to be trying to start up a
				// workload on this IP address. We need to release the IP and possibly kill our local workload.
				if !electionSuccess {
					logCtx.Warnln("Lost election on owned IP (network partition?).")
				}
			case err := <- errCh:
				logCtx.Errorln("Error during election: %v", err)
			case <- doneCh:
				logCtx.Infoln("Resigning leadership by request and exiting.")
				candidate.Stop()
				return
			}
		}
	}()
	// Add the done channel to the interface so we can signal releases.
	this.ipsMtx.Lock()
	this.ips[ipStr] = doneCh
	this.ipsMtx.Unlock()
	logCtx.Debugln("Added IP to orchestrator.")

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