package kelips

import (
	"errors"
	"strconv"
	"time"

	"github.com/hexablock/log"

	"github.com/euforia/gossip"
)

var (
	errReqTTLReached = errors.New("request TTL reached")
)

// GroupContact is a contact within a group
type GroupContact struct {
	ID   int64  // Group id
	Host string // Host
}

func (gc GroupContact) String() string {
	return gc.Host + "/" + strconv.Itoa(int(gc.ID))
}

// affinityGroup is a single affinity group view
type affinityGroup struct {
	GroupContact

	// Re-introduce:
	// Affinity group heartbeat count.  This is updated on each tuple or peer
	// update
	// heartbeats int64

	// Subset of peers in this affinity group
	contacts ContactStorage

	// Tuples with key to host mapping.  This is only used for a local
	// group
	tuples TupleStorage

	tupleTTL    time.Duration
	tupleExpMin time.Duration
	tupleExpMax time.Duration

	// Network transport
	trans Transport

	// gossip
	gossip *gossip.Pool

	log *log.Logger
}

func newAffinityGroup(g *GroupContact, conf *Config) *affinityGroup {
	group := &affinityGroup{
		GroupContact: *g,
		trans:        conf.Transport,
		contacts:     conf.Contacts.New(g.ID, true),
		tuples:       conf.Tuples,
		tupleTTL:     conf.TupleTTL,
		tupleExpMin:  conf.TupleExpireMinInt,
		tupleExpMax:  conf.TupleExpireMaxInt,
		log:          conf.Logger,
	}

	group.trans.Register(group.GroupContact, group)

	return group
}

func (group *affinityGroup) Start() {
	group.log.Infof("Tuple expiration group=%d min=%v max=%v",
		group.ID, group.tupleExpMin, group.tupleExpMax)

	go group.expireTuples()
}

func (group *affinityGroup) expireTuples() {
	for {
		sleepFor := randExpire(group.tupleExpMin, group.tupleExpMax)
		// fmt.Println(sleepFor)
		time.Sleep(sleepFor)

		if c := group.tuples.Expire(group.tupleTTL); c > 0 {
			group.log.Infof("Expired group=%d tuples=%d", group.ID, c)
		}
	}
}

func (group *affinityGroup) Contact() GroupContact {
	return group.GroupContact
}

func (group *affinityGroup) IsLocal() bool {
	return true
}

func (group *affinityGroup) Lookup(req *Request) (string, error) {
	// Try local first
	if tuple := group.tuples.Lookup(req.Key); tuple != nil {
		return tuple.Host, nil
	}

	// Check ttl before trying another peer
	if req.TTL == 0 {
		return "", errReqTTLReached
	}

	// Try the next closest node in our group
	p, ok := group.contacts.GetClosest()
	if !ok {
		// TODO:
		return "", errNoContacts
	}

	if p.Address() == req.Originator.Host {
		group.log.Errorf("TODO: Local selected=originator local=%s %s", group.Host, p.Address())
	}

	c := GroupContact{ID: group.ID, Host: p.Address()}
	nreq := &Request{
		Key:        make([]byte, len(req.Key)),
		TTL:        req.TTL - 1, // Decrement ttl
		Originator: group.GroupContact,
	}
	copy(nreq.Key, req.Key)

	return group.trans.Lookup(c, nreq)
}

func (group *affinityGroup) AddPeer(host PeerContact) error {
	return group.contacts.Add(host)
}

func (group *affinityGroup) RemovePeer(host PeerContact) error {
	return group.contacts.Remove(host)
}

func (group *affinityGroup) Insert(key []byte) (string, error) {
	p, ok := group.contacts.GetRandom()
	if !ok {
		return "", errNoContacts
	}

	tuple := &Tuple{Key: key, Host: p.Address()}
	group.tuples.Insert(tuple)

	return p.Address(), nil
}

type remoteAffinityGroup struct {
	GroupContact

	// Affinity group heartbeat count.  This is updated on each tuple or peer
	// update
	heartbeats int64

	// Subset of peers in this affinity group
	contacts ContactStorage

	// Network transport
	trans Transport

	log *log.Logger
}

func newRemoteAffinityGroup(gc *GroupContact, conf *Config) *remoteAffinityGroup {
	g := &remoteAffinityGroup{
		GroupContact: *gc,
		trans:        conf.Transport,
		contacts:     conf.Contacts.New(gc.ID, false),
		log:          conf.Logger,
	}
	return g
}

func (group *remoteAffinityGroup) Contact() GroupContact {
	return group.GroupContact
}

func (group *remoteAffinityGroup) IsLocal() bool {
	return false
}

func (group *remoteAffinityGroup) beat() {
	group.heartbeats++
}

func (group *remoteAffinityGroup) RemovePeer(host PeerContact) error {
	return group.contacts.Remove(host)
}

func (group *remoteAffinityGroup) AddPeer(host PeerContact) error {
	return group.contacts.Add(host)
}

func (group *remoteAffinityGroup) Lookup(req *Request) (string, error) {
	peer, ok := group.contacts.GetClosest()
	if !ok {
		return "", errNoContacts
	}

	if peer.Address() == req.Originator.Host {
		group.log.Error("TODO: Remote selected=originator", peer.Address())
	}

	req.Originator = group.GroupContact
	host, err := group.trans.Lookup(GroupContact{ID: group.ID, Host: peer.Address()}, req)
	if err == nil {
		group.beat()
	}

	return host, err
}

func (group *remoteAffinityGroup) Insert(key []byte) (string, error) {
	peer, ok := group.contacts.GetClosest()
	if !ok {
		return "", errNoContacts
	}

	host, err := group.trans.Insert(GroupContact{ID: group.ID, Host: peer.Address()}, key)
	if err == nil {
		group.beat()
	}

	return host, err
}

func (group *remoteAffinityGroup) Start() {}
