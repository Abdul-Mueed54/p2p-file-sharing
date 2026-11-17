package p2p

// This file is the "who has what" memory of the discovery layer.

import (
	"sort"
	"sync"
	"time"
)

type FileStub struct {
	InfoHash string `json:"info_hash"`
	FileName string `json:"file_name"`
	FileSize int64  `json:"file_size"`
}

type Cluster struct {
	InfoHash string
	FileName string
	FileSize int64
	Peers map[string]time.Time
}

type ClusterView struct {
	InfoHash string
	FileName string
	FileSize int64
	Peers []string
}

type Registry struct {
	mu sync.RWMutex
	clusters map[string]*Cluster
	now func() time.Time
}

func NewRegistry() *Registry {
	return &Registry{
		clusters: make(map[string]*Cluster),
		now: time.Now,
	}
}

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
