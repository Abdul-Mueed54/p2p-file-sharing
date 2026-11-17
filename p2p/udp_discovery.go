package p2p

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

// 239.0.0.0/8 is multicast range for for local networks.
// Port 9999 is just a convention
const MulticastGroup = "239.255.255.250:9999"

// Announcement is one UDP packet: "I am at Addr, and I have these files".
// Only the small FileStub summaries travel here, never the chunk hashes.
type Announcement struct {
	ID string `json:"id"`
	Addr string `json:"addr"`
	Files []FileStub `json:"files"`
}

type UDPDiscovery struct {
	Group string        
	AnnounceAddr string        
	Interval time.Duration 
	Iface *net.Interface
	peers chan string
	id string
	filesMu sync.RWMutex
	files []FileStub
	registry *Registry
	mu sync.Mutex
	conn *net.UDPConn
	done chan struct{}
	stopOnce sync.Once
}

func NewUDPDiscovery(announceAddr string, interval time.Duration) *UDPDiscovery {
	if interval <= 0 {
		interval = 3 * time.Second
	}

	return &UDPDiscovery{
		Group:        MulticastGroup,
		AnnounceAddr: announceAddr,
		Interval:     interval,
		peers:        make(chan string, 64),
		id:           newID(),
		registry:     NewRegistry(),
		done:         make(chan struct{}),
	}
}


func (d *UDPDiscovery) SetFiles(files []FileStub) {
	d.filesMu.Lock()
	defer d.filesMu.Unlock()

	d.files = make([]FileStub, len(files))
	copy(d.files, files)
}


func (d *UDPDiscovery) Discover() <-chan string {
	return d.peers
}

func (d *UDPDiscovery) Clusters() []ClusterView {
	return d.registry.GetClusters()
}

func (d *UDPDiscovery) ClusterPeers(infoHash string) []string {
	return d.registry.ClusterPeers(infoHash)
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

	d.mu.Lock()
	d.conn = conn
	d.mu.Unlock()

	// Three independent jobs, so three goroutines:
	//   listen   -> receive other nodes' announcements
	//   announce -> send our own announcement on a timer
	//   reap     -> forget peers that went silent
	go d.listen(conn)
	go d.announce(group)
	go d.reap()

	return nil
}

// Stop shuts discovery down cleanly. Safe to call more than once.
func (d *UDPDiscovery) Stop() {
	d.stopOnce.Do(func() {
		close(d.done)

		d.mu.Lock()
		conn := d.conn
		d.mu.Unlock()

		if conn != nil {
			_ = conn.Close()
		}
	})
}

func (d *UDPDiscovery) listen(conn *net.UDPConn) {
	defer conn.Close()

	buf := make([]byte, 2048)

	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-d.done:
				return // normal shutdown
			default:
			}
			fmt.Printf("discovery read: %v\n", err)
			return
		}

		var a Announcement
		if err := json.Unmarshal(buf[:n], &a); err != nil {
			continue 
		}

		if a.ID == d.id || a.Addr == "" {
			continue
		}

		d.registry.RegisterBlueprint(a.Addr, a.Files)

		select {
		case d.peers <- a.Addr:
		default:
		}
	}
}

func (d *UDPDiscovery) announce(group *net.UDPAddr) {
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

	ticker := time.NewTicker(d.Interval)
	defer ticker.Stop()

	for {
		d.announceOnce(conn) // send first, so we are discoverable immediately

		select {
		case <-d.done:
			return
		case <-ticker.C:
		}
	}
}

// announceOnce builds the packet from the CURRENT file list and sends it.
func (d *UDPDiscovery) announceOnce(conn *net.UDPConn) {
	d.filesMu.RLock()
	files := make([]FileStub, len(d.files))
	copy(files, d.files)
	d.filesMu.RUnlock()

	payload, err := json.Marshal(Announcement{
		ID:    d.id,
		Addr:  d.AnnounceAddr,
		Files: files,
	})
	if err != nil {
		return
	}

	if _, err := conn.Write(payload); err != nil {
		fmt.Printf("discovery announce: %v\n", err)
	}
}

// reap drops peers that have been silent for longer than 3 announce intervals.
func (d *UDPDiscovery) reap() {
	ttl := 3 * d.Interval

	ticker := time.NewTicker(d.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-d.done:
			return
		case <-ticker.C:
			d.registry.Sweep(ttl)
		}
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
