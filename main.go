package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	tcpPort := flag.String("port", "3000", "TCP port to announce (the port we would serve on)")
	interval := flag.Duration("interval", 3*time.Second, "how often to announce")
	override := flag.String("announce", "", "force a specific announce address, e.g. 192.168.1.5:3000")
	local := flag.Bool("local", false, "test on one machine: use the loopback interface")
	infoHash := flag.String("hash", "9f2c8a1f", "info hash to advertise")
	fileName := flag.String("name", "movie.mp4", "file name to advertise")
	fileSize := flag.Int64("size", 734003200, "file size in bytes to advertise")

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
	fmt.Printf("advertising file %q  hash=%s  size=%s\n", *fileName, *infoHash, humanSize(*fileSize))
	fmt.Println("listening for peers... (ctrl-c to stop)")

	d := NewUDPDiscovery(announceAddr, *interval)

	// This is the only call the application needs to say "here is what I have".
	d.SetFiles([]FileStub{{
		InfoHash: *infoHash,
		FileName: *fileName,
		FileSize: *fileSize,
	}})

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

	// Clean exit on ctrl-c so we don't leave sockets behind.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		d.Stop()
		fmt.Println("\nbye")
		os.Exit(0)
	}()

	// Log each peer address once (it is re-announced every few seconds).
	go func() {
		seen := make(map[string]bool)
		for addr := range d.Discover() {
			if seen[addr] {
				continue
			}
			seen[addr] = true
			fmt.Printf("heard peer: %s\n", addr)
		}
	}()

	// Every 2 seconds, look at the cluster list and print ONLY when it changed.
	// (Discovery announces forever in the background; that is the heartbeat.
	//  Printing the same table 30 times a minute just hides the real events.)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	last := "\x00" // sentinel so the first tick always prints
	for range ticker.C {
		clusters := d.Clusters()
		key := snapshotKey(clusters)
		if key == last {
			continue
		}
		last = key
		printClusters(clusters, *infoHash)
	}
}

// snapshotKey turns the cluster list into one comparable string, so we can
// detect "nothing changed" without comparing slices by hand.
func snapshotKey(clusters []ClusterView) string {
	var b strings.Builder
	for _, c := range clusters {
		fmt.Fprintf(&b, "%s|%s|%d|%v;", c.InfoHash, c.FileName, c.FileSize, c.Peers)
	}
	return b.String()
}

func printClusters(clusters []ClusterView, wanted string) {
	if len(clusters) == 0 {
		fmt.Println("(no files discovered yet)")
		return
	}

	for _, c := range clusters {
		mark := " "
		if c.InfoHash == wanted {
			mark = "*"
		}
		fmt.Printf("%s %s  %-16s %10s  peers=%d\n",
			mark, c.InfoHash, c.FileName, humanSize(c.FileSize), len(c.Peers))
		for _, p := range c.Peers {
			fmt.Printf("      - %s\n", p)
		}
	}
}

func humanSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
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
