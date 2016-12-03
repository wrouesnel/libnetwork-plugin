package main

import (
	"os"
	"fmt"
	"flag"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/go-plugins-helpers/ipam"
	"github.com/docker/go-plugins-helpers/network"
	"github.com/projectcalico/libcalico-go/lib/api"
	"github.com/projectcalico/libnetwork-plugin/driver"

	datastoreClient "github.com/projectcalico/libcalico-go/lib/client"

	"github.com/projectcalico/libnetwork-plugin/orchestration"
	"strconv"
)

const (
	ipamPluginName = "calico-ipam"
	networkPluginName = "calico"
)

var (
	config *api.CalicoAPIConfig
	client *datastoreClient.Client
	// Orchestrator configuration
	orchestratorEnable bool = false
	orchestratorTTL time.Duration = time.Second * 30
)

func init() {
  // Output to stderr instead of stdout, could also be a file.
  log.SetOutput(os.Stderr)
}

func initializeClient() {
	var err error

	if config, err = datastoreClient.LoadClientConfig(""); err != nil {
		panic(err)
	}
	if client, err = datastoreClient.New(*config); err != nil {
		panic(err)
	}

	if os.Getenv("CALICO_DEBUG") != "" {
		log.SetLevel(log.DebugLevel)
		log.Debugln("Debug logging enabled")
	}

	if os.Getenv("ORCHESTRATOR") != "" {
		orchestratorEnable, err = strconv.ParseBool(os.Getenv("ORCHESTRATOR"))
		if err != nil {
			panic(err)
		}
	}

	if orchestratorEnable && os.Getenv("ORCHESTRATOR_TTL") != "" {
		orchestratorTTL, err = time.ParseDuration(os.Getenv("ORCHESTRATOR_TTL"))
		if err != nil {
			panic(err)
		}
	}
}

// VERSION is filled out during the build process (using git describe output)
var VERSION string

func main() {
	// Display the version on "-v"
	// Use a new flag set so as not to conflict with existing libraries which use "flag"
	flagSet := flag.NewFlagSet("Calico", flag.ExitOnError)

	version := flagSet.Bool("v", false, "Display version")

	err := flagSet.Parse(os.Args[1:])
	if err != nil {
		log.Fatalln(err)
	}
	if *version {
		fmt.Println(VERSION)
		os.Exit(0)
	}

	initializeClient()

	var orchestrator *orchestration.Orchestrator
	if orchestratorEnable {
		orchestrator, err = orchestration.NewOrchestrator(config, client, orchestratorTTL)
		if err != nil {
			log.Fatalln(err)
		}
		if orchestrator == nil {
			log.Fatalln("Orchestrator initialization failed.")
		}
		log.Infoln("orchestrator is ready.")
	}

	errChannel := make(chan error)
	networkHandler := network.NewHandler(driver.NewNetworkDriver(client, orchestrator))
	ipamHandler := ipam.NewHandler(driver.NewIpamDriver(client, orchestrator))

	go func(c chan error) {
		log.Infoln("calico-net has started.")
		err := networkHandler.ServeUnix("root", networkPluginName)
		log.Infoln("calico-net has stopped working.")
		c <- err
	}(errChannel)

	go func(c chan error) {
		log.Infoln("calico-ipam has started.")
		err := ipamHandler.ServeUnix("root", ipamPluginName)
		log.Infoln("calico-ipam has stopped working.")
		c <- err
	}(errChannel)

	err = <-errChannel

	log.Fatalln(err)
}
