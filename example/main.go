package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	kelips "github.com/euforia/go-kelips"
	"github.com/euforia/gossip"
	"github.com/hexablock/iputil"
)

var (
	advAddr   = flag.String("adv-addr", "127.0.0.1:10000", "Advertise address")
	kgroups   = flag.Int64("k", 3, "number of affinity group")
	joinPeers = flag.String("join", "", "Existing peers to join")
	debug     = flag.Bool("debug", false, "Debug")
)

func makeGossipConfig() *gossip.Config {
	ip, port, err := iputil.SplitHostPort(*advAddr)
	if err != nil {
		fmt.Printf("Failed to parse advertise address=%s: %v\n", *advAddr, err)
		os.Exit(1)
	}

	conf := gossip.DefaultConfig()
	conf.Name = *advAddr
	conf.Debug = *debug
	conf.AdvertiseAddr = ip
	conf.AdvertisePort = port
	conf.BindAddr = ip
	conf.BindPort = port
	return conf
}

func makeKelipsConfig() *kelips.Config {
	conf := kelips.DefaultConfig()
	conf.K = *kgroups
	conf.Transport = kelips.NewHTTPTransport(true)
	conf.Tuples = kelips.NewInmemTuples()
	conf.Logger.EnableDebug(*debug)
	return conf
}

func parseJoinPeers() []string {
	peers := strings.Split(strings.TrimSpace(*joinPeers), ",")

	out := make([]string, 0, len(peers))
	for _, peer := range peers {
		p := strings.TrimSpace(peer)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func main() {
	flag.Parse()

	kelipsConf := makeKelipsConfig()
	gossipConf := makeGossipConfig()

	// Create kelips gossip instance
	kelipsGossip, err := kelips.NewGossip(kelipsConf, gossipConf)
	if err != nil {
		log.Fatal(err)
	}

	// Create kelips instance
	klps := kelips.New(gossipConf.Name, kelipsConf)

	// Register kelips instance with gossip and start
	err = kelipsGossip.Register(klps)
	if err != nil {
		log.Fatal(err)
	}

	// Join exising peers
	peers := parseJoinPeers()
	if len(peers) > 0 {
		n, err := kelipsGossip.Join(peers...)
		if err != nil {
			if n < 1 {
				log.Fatalf("Failed to join: %+v", err)
			}

			log.Printf("Joined a partial set of peers: '%+v' count=%d\n", err, n)
		}
	}

	// Get a non-muxed TCP listener from gossip layer to use for our http server
	ln := kelipsGossip.ListenTCP()

	// Start serving http
	if err := http.Serve(ln, &httpServer{kelips: klps}); err != nil {
		log.Fatal("HTTP server:", err)
	}

}
