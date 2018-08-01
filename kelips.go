// Package kelips provides a kelips DHT implementation in go
package kelips

import (
	"context"
	"fmt"
	"hash"
	"net"

	"github.com/pkg/errors"
)

// Request is a lookup or insert request
type Request struct {
	Key        []byte       // Key to lookup or insert
	TTL        int          // number of hops
	Originator GroupContact // Group originating the request
}

// AffinityGroup implements a kelips affinity group
type AffinityGroup interface {
	// IsLocal returns true if this node belongs the affinity group
	IsLocal() bool
	// Contact returns this nodes group contact information
	Contact() GroupContact
	// Insert a key into the affinity group returning the homenode
	Insert(key []byte) (string, error)
	// Lookup a key returning the homenode
	Lookup(*Request) (string, error)
	// Add a peer to the group
	AddPeer(peer PeerContact) error
	// Remove a peer from the group
	RemovePeer(peer PeerContact) error
	// Starts all go-routines for the group
	Start()
}

// Transport implements a kelips transport interface
type Transport interface {
	Insert(contact GroupContact, key []byte) (string, error)
	// Lookup should return the home node of the key
	Lookup(contact GroupContact, req *Request) (string, error)
	// Add a peer to the group
	AddPeer(contact GroupContact, host PeerContact) error
	// Registers the affinity group with the transport
	Register(contact GroupContact, group AffinityGroup)
	// Start the transport.  This should be non-blocking
	Start(net.Listener) error
	// Shutdown the transport
	Shutdown(ctx context.Context) error
}

// Kelips is the user interface to interact with the kelips DHT
type Kelips struct {
	// home group id
	id int64
	// total number of affinity groups
	k int64
	// hash function
	hasher func() hash.Hash
	// list of groups objects
	groups []AffinityGroup
	// kelips transport
	trans Transport
}

// New returns a new Kelips instance based on the advertisable address and
// config. advAddr is the address others will use to connect to this node
func New(host string, conf *Config) *Kelips {
	conf.Validate()

	// Set default contact store
	if conf.Contacts == nil {
		conf.Contacts = &inmemContactsFac{host: host}
	}

	k := &Kelips{
		hasher: conf.HashFunc,
		k:      conf.K,
		groups: make([]AffinityGroup, conf.K),
		trans:  conf.Transport,
	}

	// Set affinity group
	k.id = lookupGroup([]byte(host), k.k, k.hasher())

	for i := int64(0); i < conf.K; i++ {
		gc := &GroupContact{ID: i, Host: host}
		if k.id == i {
			k.groups[i] = newAffinityGroup(gc, conf)
		} else {
			k.groups[i] = newRemoteAffinityGroup(gc, conf)
		}
	}

	k.groups[k.id].AddPeer(&Peer{Host: host})

	return k
}

// RemovePeer removes a peer from a group
func (klp *Kelips) RemovePeer(host PeerContact) (int64, error) {
	key := []byte(host.Address())

	idx := lookupGroup(key, klp.k, klp.hasher())
	group := klp.groups[idx]

	return idx, group.RemovePeer(host)
}

// AddPeer adds the peer as a contact to the affinity group it belongs to
func (klp *Kelips) AddPeer(host PeerContact) (int64, error) {
	key := []byte(host.Address())

	idx := lookupGroup(key, klp.k, klp.hasher())
	group := klp.groups[idx]

	return idx, group.AddPeer(host)
}

// Insert inserts the key into the DHT
func (klp *Kelips) Insert(key []byte) (string, error) {
	idx := lookupGroup(key, klp.k, klp.hasher())
	group := klp.groups[idx]

	host, err := group.Insert(key)
	if err == nil {
		return host, nil
	}

	return "", errors.Wrap(err, fmt.Sprintf("group %d", idx))
}

// Lookup returns known peers for the given key
func (klp *Kelips) Lookup(req *Request) (string, error) {
	idx := lookupGroup(req.Key, klp.k, klp.hasher())
	group := klp.groups[idx]

	return group.Lookup(req)
}

// Start starts listening for connections on the given listener and starts
// all groups.  This is non-blocking
func (klp *Kelips) Start(ln net.Listener) error {
	err := klp.trans.Start(ln)
	if err != nil {
		return err
	}

	// Start all groups
	for _, group := range klp.groups {
		group.Start()
	}

	return err
}

// Shutdown shuts down the kelips node
func (klp *Kelips) Shutdown(ctx context.Context) error {
	return klp.trans.Shutdown(ctx)
}
