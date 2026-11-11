package p2p

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// 239.0.0.0/8 is multicast range for for local networks.
// Port 9999 is just a convention
const MulticastGroup = "239.255.255.250:9999"

// just produce addresses, ID is for recogntion or ignore of our own packet, addr is for dialed IP:port
type Announcement struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

type UDPDiscovery struct {
	Group        string        // multicast address to join and shout into
	AnnounceAddr string        // OUR dialable TCP address, announced to others
	Interval     time.Duration // how often we re-announce

// Iface is the network interface to use. nil means "let the OS pick",
// which is correct for normal LAN use.
//
// Set it to the loopback interface ("lo") to test two processes on a
// single machine. 
	Iface *net.Interface
	peers chan string
	// id identifies THIS process. Generated once at startup.
	id string
}

func NewUDPDiscovery(announceAddr string, interval time.Duration) *UDPDiscovery {
	return &UDPDiscovery{
		Group: MulticastGroup,
		AnnounceAddr: announceAddr,
		Interval: interval,
		peers: make(chan string, 64),
		id: newID(),
	}
}

func (d *UDPDiscovery) Discover() <-chan string {
	return d.peers
}

func (d *UDPDiscovery) Start() error {
	group, err := net.ResolveUDPAddr("udp4", d.Group)
	if err != nil {
		return fmt.Errorf("resolve multicast group: %w", err)
	}

	conn, err := net.ListenMulticastUDP("udp4", d.Iface, group)
	if err != nil {
		return fmt.Errorf("join multicast group: %w", err)
	}

	conn.SetReadBuffer(64 * 1024)

	// Two independent jobs, so two goroutines:
	//   listen   -> receive other nodes' announcements
	//   announce -> send our own announcement on a timer
	go d.listen(conn)
	go d.announce(group)

	return nil
}

func (d *UDPDiscovery) listen(conn *net.UDPConn) {
	defer conn.Close()

	buf := make([]byte, 2048)

	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("discovery read: %v\n", err)
			continue
		}

		var a Announcement
		if err := json.Unmarshal(buf[:n], &a); err != nil {
			continue
		}

		if a.ID == d.id || a.Addr == "" {
			continue
		}

		select {
		case d.peers <- a.Addr:
		default:
		}
	}
}

func (d *UDPDiscovery) announce(group *net.UDPAddr) {
	// Binding the sender to a local address forces the packet out that
	// interface. Needed for the loopback test mode.
	var laddr *net.UDPAddr
	if d.Iface != nil {
		if ip := firstIPv4(d.Iface); ip != nil {
			laddr = &net.UDPAddr{IP: ip}
		}
	}

	conn, err := net.DialUDP("udp4", laddr, group)
	if err != nil {
		fmt.Printf("discovery announce: %v\n", err)
		return
	}
	defer conn.Close()

	payload, _ := json.Marshal(Announcement{ID: d.id, Addr: d.AnnounceAddr})

	ticker := time.NewTicker(d.Interval)
	defer ticker.Stop()

	for {
		if _, err := conn.Write(payload); err != nil {
			fmt.Printf("discovery announce: %v\n", err)
		}
		<-ticker.C
	}
}

func firstIPv4(iface *net.Interface) net.IP {
	addrs, err := iface.Addrs()
	if err != nil {
		return nil
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok {
			if ip4 := ipnet.IP.To4(); ip4 != nil {
				fmt.Printf("      -> returning %v\n", ip4)
				return ip4
			}
		}
	}
	return nil
}

func newID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
