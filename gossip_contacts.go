package kelips

import (
	"math/rand"
	"sort"
	"sync"

	"github.com/euforia/gossip/peers"
	"github.com/hexablock/log"
)

type gossipContactStorage struct {
	// affinity group id the store belongs to
	id int64

	// lib containing all peers in the kelips network ie. local and foreign
	// affinity groups
	peers peers.Library

	// contacts from the library part of the affinity group
	mu       sync.RWMutex
	contacts []string

	log *log.Logger
}

func (g *gossipContactStorage) Add(p PeerContact) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	addr := p.Address()
	for _, c := range g.contacts {
		if c == addr {
			return errContactExists
		}
	}

	g.contacts = append(g.contacts, addr)
	g.log.Debugf("group=%d contacts=%v", g.id, g.contacts)
	return nil
}

// Remove is a no-op that implements the ContractStorage interface
func (g *gossipContactStorage) Remove(p PeerContact) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	for i, c := range g.contacts {
		if c == p.Address() {
			g.contacts = append(g.contacts[:i], g.contacts[i+1:]...)
			return nil
		}
	}
	return errContactNotFound
}

// List is a no-op that implements the ContractStorage interface
func (g *gossipContactStorage) List() []PeerContact {
	g.mu.RLock()
	libPeers := g.peers.GetByAddress(g.contacts...)
	g.mu.RUnlock()
	return peersToPeerContacts(libPeers)
}

// GetClosest returns the closest peer excluding self
func (g *gossipContactStorage) GetClosest() (PeerContact, bool) {
	g.mu.RLock()
	lpeers := g.peers.GetByAddress(g.contacts...)
	g.mu.RUnlock()

	sort.Sort(peers.ClosestPeers(lpeers))

	l := len(lpeers)
	switch l {
	case 0:
		return nil, false
	case 1:
		return lpeers[0], true
	}

	return lpeers[1], true
}

// GetRandom returns a random peer from the library and may include self
func (g *gossipContactStorage) GetRandom() (PeerContact, bool) {
	p := g.List()
	if len(p) == 0 {
		return nil, false
	}

	i := rand.Int() % len(p)
	return p[i], true
}
