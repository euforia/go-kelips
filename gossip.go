package kelips

import (
	"hash"
	"net"

	"strconv"

	"github.com/euforia/gossip"
	"github.com/hexablock/log"
)

const globalGossipPoolID uint16 = 123

// Gossip is the gossip component of kelips.  It handles all contact and tuple
// liveliness, pinging as messaging arrive on the gossip network
type Gossip struct {
	// underlying gossip instance
	gossip *gossip.Gossip

	// actual tuple store
	tuples TupleStorage
	// above tuple store augmented with gossip
	gtuples *gossipTupleStorage

	// Inter affinity group gossip pool. This is used by all groups
	inter *gossip.Pool
	// Inter affinity group gossip delegate
	delegate *kelipsGossipDelegate

	host   string           // node host used for new contact stores
	id     int64            // home group id used when joining
	hasher func() hash.Hash // hash function
	k      int64            // total affinity groups

	log *log.Logger
}

// NewGossip returns a new Gossip instance
func NewGossip(kconf *Config, conf *gossip.Config) (*Gossip, error) {
	// Gossip instance
	gsp, err := gossip.New(conf)
	if err != nil {
		return nil, err
	}

	gs := &Gossip{
		gossip: gsp,
		id:     -1,
		tuples: kconf.Tuples,
		gtuples: &gossipTupleStorage{
			TupleStorage: kconf.Tuples,
			log:          kconf.Logger,
		},
		host:   conf.AdvertiseAddr + ":" + strconv.Itoa(conf.AdvertisePort),
		hasher: kconf.HashFunc,
		k:      kconf.K,
		delegate: &kelipsGossipDelegate{
			log: kconf.Logger,
		},
		log: kconf.Logger,
	}

	// Set the parent tuple store to the one with gossip.
	kconf.Tuples = gs.gtuples
	// Set contact store to the gossip instance
	kconf.Contacts = gs

	// Inter affinity group gossip (global)
	poolConf := gossip.DefaultLANPoolConfig(int32(globalGossipPoolID))
	poolConf.Events = gs.delegate

	gs.inter = gsp.RegisterPool(poolConf)

	return gs, nil
}

// Register registers the kelips instance and starts all go-routines.  This should be the last
// call once eveything has been initialized.  Join can be called after this one
func (st *Gossip) Register(k *Kelips) error {
	st.delegate.kelips = k
	return st.start()
}

// Join joins the inter-group gossip pool and the home gossip group assuming a home node
// has been provided
func (st *Gossip) Join(peers ...string) (int, error) {
	// Join global gossip pool
	n, err := st.inter.Join(peers)
	if err != nil {
		return 0, err
	}

	// Add peers to affinity group/s
	for _, peer := range peers {
		_, er := st.delegate.kelips.AddPeer(&Peer{Host: peer})
		if er != nil {
			if er != errContactExists {
				err = er
			}
			continue
		}
	}

	err = st.joinGroup()
	if err != nil {
		return 0, err
	}

	return n, err
}

// ListenMux returns a muxed listener with the given id. This must be called before
// starting gossip
func (st *Gossip) ListenMux(m uint16) (net.Listener, error) {
	return st.gossip.Listen(m)
}

// ListenTCP returns a non-muxed native tcp listener
func (st *Gossip) ListenTCP() net.Listener {
	return st.gossip.ListenTCP()
}

// New returns a new contact store backed by gossip depending on whether this
// is a home group.  It implements the ContactStorageFactory interface
func (st *Gossip) New(id int64, homeNode bool) ContactStorage {
	cs := &gossipContactStorage{
		id:       id,
		contacts: make([]string, 0, 1),
		log:      st.log,
	}

	if !homeNode {
		cs.peers = st.inter.Peers()
		return cs
	}

	delegate := &tuplesGossipDelegate{
		tuples: st.tuples,
		host:   st.host,
		log:    st.log,
	}

	conf := gossip.DefaultLANPoolConfig(int32(id))
	conf.Events = delegate
	conf.Delegate = delegate

	st.id = id
	st.gtuples.pool = st.gossip.RegisterPool(conf)

	cs.peers = conf.Peers

	return cs
}

// Start starts gossip and the underlying kelips instance
func (st *Gossip) start() error {
	// Kelips Transport listener
	ln, err := st.gossip.Listen(kelipsMagic)
	if err != nil {
		return err
	}

	err = st.gossip.Start()
	if err == nil {
		err = st.delegate.kelips.Start(ln)
	}
	return err
}

func (st *Gossip) joinGroup() (err error) {
	// Get all peers from the global pool
	peers := st.inter.Peers().List()

	for _, peer := range peers {
		// Get affinity group for peer
		addr := peer.Address()
		id := lookupGroup([]byte(addr), st.k, st.hasher())

		// Only join home pool if this is our home group and the host
		// is not ourself
		if id == st.id && addr != st.host {
			_, err = st.gtuples.pool.Join([]string{addr})
			if err == nil {
				return nil
			}

		}

	}

	return err
}
