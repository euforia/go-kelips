package kelips

import (
	"time"
)

// Peer node
type Peer struct {
	Host       string
	rtt        time.Duration
	heartbeats int64
}

// Address satisfies the PeerContact interface
func (p *Peer) Address() string {
	return p.Host
}

// ping updates the rtt and increments the heartbeat count
// func (p *peer) ping(rtt time.Duration) {
// 	p.rtt = rtt
// 	p.heartbeats++
// }

// sortPeers implements the sort interface to sort peers by rtt
// type sortPeers []*peer

// func (p sortPeers) Len() int {
// 	return len(p)
// }

// func (p sortPeers) Less(i, j int) bool {
// 	return p[i].rtt < p[j].rtt
// }

// func (p sortPeers) Swap(i, j int) {
// 	p[i], p[j] = p[j], p[i]
// }
