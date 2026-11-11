package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cyxabima/minimal-p2p-library-in-go/p2p"
)

// A throwaway harness for testing UDP discovery on its own.
//
// Run it twice (two terminals, or two laptops) and each instance should
// print the other's address within a few seconds.
func main() {
	tcpPort := flag.String("port", "3000", "TCP port to announce (the port we would serve on)")
	interval := flag.Duration("interval", 3*time.Second, "how often to announce")
	override := flag.String("announce", "", "force a specific announce address, e.g. 192.168.1.5:3000")
	local := flag.Bool("local", false, "test on one machine: use the loopback interface")
	flag.Parse()

	announceAddr := *override
	if announceAddr == "" {
		if *local {
			announceAddr = net.JoinHostPort("127.0.0.1", *tcpPort)
		} else {
			ip, err := localIP()
			if err != nil {
				log.Fatalf("cannot determine local IP: %v (use -announce to set it manually)", err)
			}
			announceAddr = net.JoinHostPort(ip.String(), *tcpPort)
		}
	}

	fmt.Printf("this node announces as %s\n", announceAddr)
	fmt.Println("listening for peers... (ctrl-c to stop)")

	d := p2p.NewUDPDiscovery(announceAddr, *interval)

	if *local {
		iface, err := net.InterfaceByName("lo")
		if err != nil {
			log.Fatalf("loopback interface: %v", err)
		}
		d.Iface = iface
	}

	if err := d.Start(); err != nil {
		log.Fatal(err)
	}

	// Clean exit on ctrl-c so we don't leave the terminal hanging.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Println("\nbye")
		os.Exit(0)
	}()

	// Deduplicate: every peer re-announces every few seconds, forever.
	seen := make(map[string]bool)
	for addr := range d.Discover() {
		if seen[addr] {
			continue
		}
		seen[addr] = true
		fmt.Printf("found peer: %s\n", addr)
	}
}

// localIP returns the IPv4 address the OS would use to reach the outside
// world. No packet is actually sent — Dial on UDP only picks a route.
func localIP() (net.IP, error) {
	if c, err := net.Dial("udp", "8.8.8.8:80"); err == nil {
		defer c.Close()
		return c.LocalAddr().(*net.UDPAddr).IP, nil
	}

	// Fallback for networks with no default route (e.g. an isolated hotspot):
	// pick the first usable IPv4 address from the interfaces.
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			if ip4 := ipnet.IP.To4(); ip4 != nil {
				return ip4, nil
			}
		}
	}
	return nil, errors.New("no usable IPv4 interface found")
}
