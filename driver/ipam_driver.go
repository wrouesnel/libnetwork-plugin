package driver

import (
	"fmt"
	"net"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/docker/go-plugins-helpers/ipam"
	"github.com/projectcalico/libcalico-go/lib/api"
	datastoreClient "github.com/projectcalico/libcalico-go/lib/client"
	caliconet "github.com/projectcalico/libcalico-go/lib/net"
	logutils "github.com/projectcalico/libnetwork-plugin/utils/log"
	osutils "github.com/projectcalico/libnetwork-plugin/utils/os"
	"github.com/projectcalico/libnetwork-plugin/orchestration"
)

type IpamDriver struct {
	client *datastoreClient.Client
	orchestrator *orchestration.Orchestrator

	poolIDV4 string
	poolIDV6 string
}

func NewIpamDriver(client *datastoreClient.Client, orchestrator *orchestration.Orchestrator) ipam.Ipam {
	return IpamDriver{
		client: client,
		orchestrator: orchestrator,

		poolIDV4: PoolIDV4,
		poolIDV6: PoolIDV6,
	}
}

func (i IpamDriver) GetCapabilities() (*ipam.CapabilitiesResponse, error) {
	resp := ipam.CapabilitiesResponse{}
	logutils.JSONMessage("GetCapabilities response", resp)
	return &resp, nil
}

func (i IpamDriver) GetDefaultAddressSpaces() (*ipam.AddressSpacesResponse, error) {
	resp := &ipam.AddressSpacesResponse{
		LocalDefaultAddressSpace:  "CalicoLocalAddressSpace",
		GlobalDefaultAddressSpace: CalicoGlobalAddressSpace,
	}
	logutils.JSONMessage("GetDefaultAddressSpace response", resp)
	return resp, nil
}

func (i IpamDriver) RequestPool(request *ipam.RequestPoolRequest) (*ipam.RequestPoolResponse, error) {
	logutils.JSONMessage("RequestPool", request)

	// Calico IPAM does not allow you to request a SubPool.
	if request.SubPool != "" {
		err := errors.New(
			"Calico IPAM does not support sub pool configuration " +
				"on 'docker create network'. Calico IP Pools " +
				"should be configured first and IP assignment is " +
				"from those pre-configured pools.",
		)
		log.Errorln(err)
		return nil, err
	}

	if request.V6 {
		err := errors.New("IPv6 isn't supported")
		log.Errorln(err)
		return nil, err
	}

	// Default the poolID to the fixed value.
	poolID := i.poolIDV4
	pool := "0.0.0.0/0"

	// If a pool (subnet on the CLI) is specified, it must match one of the
	// preconfigured Calico pools.
	if request.Pool != "" {
		poolsClient := i.client.IPPools()
		_, ipNet, err := caliconet.ParseCIDR(request.Pool)
		if err != nil {
			err := errors.New("Invalid CIDR")
			log.Errorln(err)
			return nil, err
		}

		pools, err := poolsClient.List(api.IPPoolMetadata{CIDR: *ipNet})
		if err != nil || len(pools.Items) < 1 {
			err := errors.New("The requested subnet must match the CIDR of a " +
				"configured Calico IP Pool.",
			)
			log.Errorln(err)
			return nil, err
		}
		pool = request.Pool
		poolID = request.Pool
	}

	// We use static pool ID and CIDR. We don't need to signal the
	// The meta data includes a dummy gateway address. This prevents libnetwork
	// from requesting a gateway address from the pool since for a Calico
	// network our gateway is set to a special IP.
	resp := &ipam.RequestPoolResponse{
		PoolID: poolID,
		Pool:   pool,
		Data:   map[string]string{"com.docker.network.gateway": "0.0.0.0/0"},
	}

	logutils.JSONMessage("RequestPool response", resp)

	return resp, nil
}

func (i IpamDriver) ReleasePool(request *ipam.ReleasePoolRequest) error {
	logutils.JSONMessage("ReleasePool", request)
	return nil
}

func (i IpamDriver) RequestAddress(request *ipam.RequestAddressRequest) (*ipam.RequestAddressResponse, error) {
	logutils.JSONMessage("RequestAddress", request)

	hostname, err := osutils.GetHostname()
	if err != nil {
		log.Errorln(err)
		return nil, err
	}

	var IPs []caliconet.IP

	if request.Address == "" {
		// No address requested, so auto assign from our pools.
		log.Println("Auto assigning IP from Calico pools")

		// If the poolID isn't the fixed one then find the pool to assign from.
		// poolV4 defaults to nil to assign from across all pools.
		var poolV4 []caliconet.IPNet
		if request.PoolID != PoolIDV4 {
			poolsClient := i.client.IPPools()
			_, ipNet, err := caliconet.ParseCIDR(request.PoolID)

			if err != nil {
				err = errors.Wrapf(err, "Invalid CIDR - %v", request.PoolID)
				log.Errorln(err)
				return nil, err
			}
			pool, err := poolsClient.Get(api.IPPoolMetadata{CIDR: *ipNet})
			if err != nil {
				err := errors.New("The network references a Calico pool which " +
					"has been deleted. Please re-instate the " +
					"Calico pool before using the network.")
				log.Errorln(err)
				return nil, err
			}
			poolV4 = []caliconet.IPNet{caliconet.IPNet{IPNet: pool.Metadata.CIDR.IPNet}}
			log.Debugln("Using specific pool ", poolV4)
		}

		// Auto assign an IP address.
		// IPv4 pool will be nil if the docker network doesn't have a subnet associated with.
		// Otherwise, it will be set to the Calico pool to assign from.
		IPsV4, IPsV6, err := i.client.IPAM().AutoAssign(
			datastoreClient.AutoAssignArgs{
				Num4:      1,
				Num6:      0,
				Hostname:  hostname,
				IPv4Pools: poolV4,
			},
		)

		// If orchestrator enabled, challenge the IP *before* attempting to assign to give a chance to clear it out.
		if i.orchestrator != nil {
			// Challenge all the IPs we just received
			for _, ip := range append(IPsV4, IPsV6...) {
				if err := i.orchestrator.ChallengeIP(ip.IP); err != nil {
					err = errors.Wrapf(err, "IP address being actively used by another node: %v", ip)
					return nil, err
				}
				// Challenge successful, we are the owner and holding the lock on this IP.
			}
		}

		if err != nil {
			err = errors.Wrapf(err, "IP assignment error")
			log.Errorln(err)
			return nil, err
		}
		IPs = append(IPsV4, IPsV6...)
	} else {
		// Docker allows the users to specify any address.
		// We'll return an error if the address isn't in a Calico pool, but we don't care which pool it's in
		// (i.e. it doesn't need to match the subnet from the docker network).
		log.Debugln("Reserving a specific address in Calico pools")
		ip := net.ParseIP(request.Address)
		ipArgs := datastoreClient.AssignIPArgs{
			IP:       caliconet.IP{IP: ip},
			Hostname: hostname,
		}

		// If orchestrator enabled, challenge the IP *before* attempting to assign to give a chance to clear it out.
		if i.orchestrator != nil {
			if err := i.orchestrator.ChallengeIP(ip); err != nil {
				err = errors.Wrapf(err, "IP address being actively used by another node: %v", ip)
				return nil, err
			}
			// Challenge successful, we are the owner and holding the lock on this IP.
		}

		// TODO: just because we hold the lock, doesn't mean we actually own it yet. Check if we own it, and release
		// if something else is recorded as using it.
		err := i.client.IPAM().AssignIP(ipArgs)
		if err != nil {
			err = errors.Wrapf(err, "IP assignment error, data: %+v", ipArgs)
			log.Errorln(err)
			return nil, err
		}
		IPs = []caliconet.IP{{IP: ip}}
	}

	// We should only have one IP address assigned at this point.
	if len(IPs) != 1 {
		err := errors.New(fmt.Sprintf("Unexpected number of assigned IP addresses. "+
			"A single address should be assigned. Got %v", IPs))
		log.Errorln(err)
		return nil, err
	}

	// Return the IP as a CIDR.
	resp := &ipam.RequestAddressResponse{
		Address: fmt.Sprintf("%v/%v", IPs[0], "32"),
	}

	logutils.JSONMessage("RequestAddress response", resp)

	return resp, nil
}

func (i IpamDriver) ReleaseAddress(request *ipam.ReleaseAddressRequest) error {
	logutils.JSONMessage("ReleaseAddress", request)

	ip := caliconet.IP{IP: net.ParseIP(request.Address)}

	// If orchestrator enabled, resign the IP
	if i.orchestrator != nil {
		// Currently this will never fail.
		i.orchestrator.ReleaseIP(net.ParseIP(request.Address))
	}

	// Unassign the address.  This handles the address already being unassigned
	// in which case it is a no-op.
	_, err := i.client.IPAM().ReleaseIPs([]caliconet.IP{ip})
	if err != nil {
		err = errors.Wrapf(err, "IP releasing error, ip: %v", ip)
		log.Errorln(err)
		return err
	}

	return nil
}
