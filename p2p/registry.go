package p2p

// This file is the "who has what" memory of the discovery layer.
//
// It is a plain map guarded by a mutex. No sockets, no files, no timeouts:
//   clusters[infoHash] = { FileName, FileSize, peers: { addr -> lastSeen } }
//
// The UDP listener fills it, the reaper empties the old entries, and the
// application reads it through GetClusters().

import (
	"sort"
	"sync"
	"time"
)

// FileStub is the small summary of a shared file that fits in one UDP packet.
// It is intentionally NOT the full blueprint: no chunk hashes travel here.
type FileStub struct {
	InfoHash string `json:"info_hash"`
	FileName string `json:"file_name"`
	FileSize int64  `json:"file_size"`
}

// Cluster is one file plus every peer that claims to have it.
// "jin ke paas same file hai" == one Cluster.
type Cluster struct {
	InfoHash string
	FileName string
	FileSize int64

	// addr ("192.168.1.7:3000") -> the last time we heard from it
	Peers map[string]time.Time
}

// ClusterView is a read-only copy given to callers. We never hand out the
// internal maps, otherwise the caller could read them while we write.
type ClusterView struct {
	InfoHash string
	FileName string
	FileSize int64
	Peers    []string
}

// Registry keeps the in-memory map of clusters.
type Registry struct {
	mu       sync.RWMutex
	clusters map[string]*Cluster

	// now is time.Now by default. Tests replace it with a fake clock so they
	// do not have to sleep for real seconds.
	now func() time.Time
}

func NewRegistry() *Registry {
	return &Registry{
		clusters: make(map[string]*Cluster),
		now:      time.Now,
	}
}

// RegisterBlueprint adds or refreshes every file that `addr` announced.
// Called from the UDP listen goroutine on every announcement packet.
func (r *Registry) RegisterBlueprint(addr string, files []FileStub) {
	if addr == "" {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now()

	for _, f := range files {
		if f.InfoHash == "" {
			continue
		}

		c, ok := r.clusters[f.InfoHash]
		if !ok {
			c = &Cluster{
				InfoHash: f.InfoHash,
				Peers:    make(map[string]time.Time),
			}
			r.clusters[f.InfoHash] = c
		}

		// keep the newest metadata (name/size can be corrected by the owner)
		c.FileName = f.FileName
		c.FileSize = f.FileSize
		c.Peers[addr] = now
	}
}

// UnregisterBlueprint removes one peer from one file. If that was the last
// peer, the whole cluster is deleted.
func (r *Registry) UnregisterBlueprint(addr, infoHash string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	c, ok := r.clusters[infoHash]
	if !ok {
		return
	}

	delete(c.Peers, addr)
	if len(c.Peers) == 0 {
		delete(r.clusters, infoHash)
	}
}

// UnregisterPeer removes a peer from every file it advertised.
func (r *Registry) UnregisterPeer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for infoHash, c := range r.clusters {
		delete(c.Peers, addr)
		if len(c.Peers) == 0 {
			delete(r.clusters, infoHash)
		}
	}
}

// Sweep is the reaper's knife: drop every peer we have not heard from within
// ttl, then drop clusters that no longer have any peer.
//
// Deleting from a map while ranging over it is safe in Go: entries deleted
// before they are reached are simply not visited.
func (r *Registry) Sweep(ttl time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now()

	for infoHash, c := range r.clusters {
		for addr, lastSeen := range c.Peers {
			if now.Sub(lastSeen) > ttl {
				delete(c.Peers, addr)
			}
		}
		if len(c.Peers) == 0 {
			delete(r.clusters, infoHash)
		}
	}
}

// GetClusters returns a copy of everything we know. Safe to call from any
// goroutine. Sorted so the output is stable and easy to print.
func (r *Registry) GetClusters() []ClusterView {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]ClusterView, 0, len(r.clusters))

	for _, c := range r.clusters {
		v := ClusterView{
			InfoHash: c.InfoHash,
			FileName: c.FileName,
			FileSize: c.FileSize,
			Peers:    make([]string, 0, len(c.Peers)),
		}
		for addr := range c.Peers {
			v.Peers = append(v.Peers, addr)
		}
		sort.Strings(v.Peers) // map order is random; make it readable
		out = append(out, v)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].InfoHash < out[j].InfoHash })
	return out
}

// ClusterPeers answers: "which peers have this exact file?"
func (r *Registry) ClusterPeers(infoHash string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	c, ok := r.clusters[infoHash]
	if !ok {
		return nil
	}

	peers := make([]string, 0, len(c.Peers))
	for addr := range c.Peers {
		peers = append(peers, addr)
	}
	sort.Strings(peers)
	return peers
}
