package kelips

import (
	"bytes"
	"net"

	"github.com/euforia/gossip/peers/peerspb"
	"github.com/hexablock/log"
)

// kelipsGossipDelegate is the global gossip pool i.e. inter group gossip
type kelipsGossipDelegate struct {
	kelips *Kelips
	log    *log.Logger
}

func (g *kelipsGossipDelegate) NotifyJoin(peer *peerspb.Peer) {
	// Add peer to appropriate group
	gid, err := g.kelips.AddPeer(peer)
	if err != nil {
		g.log.Errorf("Failed to add peer: %s %v", peer.Address(), err)
	} else {
		g.log.Infof("New peer=%s group=%d", peer.Address(), gid)
	}
}

func (g *kelipsGossipDelegate) NotifyUpdate(peer *peerspb.Peer) {}

func (g *kelipsGossipDelegate) NotifyLeave(peer *peerspb.Peer) {
	// Remove from appropriate group
	gid, err := g.kelips.RemovePeer(peer)
	if err != nil {
		g.log.Errorf("Failed to remove peer: %s %v", peer.Address(), err)
	} else {
		g.log.Infof("Removed peer=%s group=%d", peer.Address(), gid)
	}
}

// tuplesGossipDelegate is the gossip pool for a home group i.e. a local group with
// tuples
type tuplesGossipDelegate struct {
	// local host used for the state exchange header
	host string
	// local tuples
	tuples TupleStorage
	// logger
	log *log.Logger
}

func (g *tuplesGossipDelegate) NotifyJoin(peer *peerspb.Peer) {}

func (g *tuplesGossipDelegate) NotifyUpdate(peer *peerspb.Peer) {}

func (g *tuplesGossipDelegate) NotifyLeave(peer *peerspb.Peer) {
	addr := peer.Address()
	c := g.tuples.ExpireHost(addr)
	g.log.Infof("Peer left peer=%s tuples-expired=%d", addr, c)
}

func (g *tuplesGossipDelegate) NotifyMsg(msg []byte) {
	// TODO:
	host := hostBytesToString(msg[:18])
	tuples, err := readTuples(bytes.NewBuffer(msg[18:]))
	if err != nil {
		g.log.Error("Failed to parse tuples: ", err)
		return
	}
	inserted := g.tuples.Insert(tuples...)
	g.log.Infof("Inserted tuples: %d/%d from=%s", inserted, len(tuples), host)
}

func (g *tuplesGossipDelegate) MergeRemoteState(remote *net.TCPAddr, buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}

	tuples, err := readTuples(bytes.NewBuffer(buf))
	if err != nil {
		g.log.Error("Failed to parse tuples: ", err)
		return
	}

	if join {
		// Insert tuples received from a peer on join
		inserted := g.tuples.Insert(tuples...)
		g.log.Infof("Seeded tuples: %d/%d from=%s", inserted, len(tuples), remote.String())
	} else {
		g.pingRemoteTuples(remote, tuples)
	}
}

func (g *tuplesGossipDelegate) pingRemoteTuples(remote *net.TCPAddr, tuples []*Tuple) {
	// Ping tuples received from a peer
	keys := make([][]byte, 0, len(tuples))
	for _, tuple := range tuples {
		// Do not ping tuples that do not belong to the remote node
		if tuple.Host != remote.String() {
			continue
		}
		keys = append(keys, tuple.Key)
	}

	pinged := g.tuples.Ping(keys...)
	g.log.Debugf("Pinged remote tuples=%d/%d from=%s", pinged, len(keys), remote.String())
}

func (g *tuplesGossipDelegate) pingLocalTuples() []*Tuple {
	all := g.tuples.List()

	// Collect local tuples and keys
	tuples := make([]*Tuple, 0, len(all))
	keys := make([][]byte, 0, len(tuples))
	for _, a := range all {
		if a.Host == g.host {
			tuples = append(tuples, a)
			keys = append(keys, a.Key)
		}
	}

	// TODO: ? Actually check the file.  Move the ping logic out
	// so we're simply sending tuples
	pinged := g.tuples.Ping(keys...)
	g.log.Debugf("Pinged local tuples=%d/%d", pinged, len(keys))

	return tuples
}

func (g *tuplesGossipDelegate) LocalState(join bool) []byte {
	// Get and ping all tuples owned by this node
	tuples := g.pingLocalTuples()

	// Send local tuples to remote
	buf := bytes.NewBuffer(nil)
	err := writeTuples(buf, tuples)
	if err != nil {
		g.log.Error("Failed to get tuple snapshot: ", err)
		return nil
	}

	g.log.Debugf("Sending tuples=%d", len(tuples))
	return buf.Bytes()
}
