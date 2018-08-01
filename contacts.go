package kelips

import "github.com/pkg/errors"

var (
	errContactNotFound = errors.New("contact not found")
	errContactExists   = errors.New("contact exists")
	errNoContacts      = errors.New("no contacts")
)

// PeerContact implements a kelips peer
type PeerContact interface {
	// Returns the ip:port of the peer
	Address() string
}

// ContactStorageFactory implements an interface to create contact storages for groups
type ContactStorageFactory interface {
	// New should return a new ContactStorage.  This is called once per
	// affinity group. Home is true if the contact store will be used for
	// the local affinity group
	New(id int64, home bool) ContactStorage
}

// ContactStorage stores peer contact information.
type ContactStorage interface {
	// Adds a PeerContact to the affinity group
	Add(PeerContact) error
	// Remove a peer from the store
	Remove(PeerContact) error
	// List returns all known peers in the affinity group
	List() []PeerContact
	// GetClosest returns the closest node to the querying node
	GetClosest() (PeerContact, bool)
	// GetRandom returns a random peer from the storage
	GetRandom() (PeerContact, bool)
}

type inmemContactsFac struct {
	// local host
	host string
}

func (fac *inmemContactsFac) New(id int64, home bool) ContactStorage {
	return &inmemContacts{
		id: id, host: fac.host,
		peers: make(map[string]*Peer),
	}
}

type inmemContacts struct {
	id    int64 //group id
	host  string
	peers map[string]*Peer
}

func (c *inmemContacts) Remove(p PeerContact) error {
	host := p.Address()
	if _, ok := c.peers[host]; !ok {
		delete(c.peers, host)
		return nil
	}
	return errContactNotFound
}
func (c *inmemContacts) Add(p PeerContact) error {
	host := p.Address()
	if _, ok := c.peers[host]; !ok {
		c.peers[host] = &Peer{Host: host}
		return nil
	}
	return errContactExists
}

func (c *inmemContacts) List() []PeerContact {
	out := make([]PeerContact, 0, len(c.peers))
	for _, v := range c.peers {
		vv := *v
		out = append(out, &vv)
	}
	return out
}

// TODO: actually get the closest node
func (c *inmemContacts) GetClosest() (PeerContact, bool) {
	for _, v := range c.peers {
		if v.Address() == c.host {
			continue
		}
		return v, true
	}
	return nil, false
}

func (c *inmemContacts) GetRandom() (PeerContact, bool) {
	for _, v := range c.peers {
		return v, true
	}
	return nil, false
}
