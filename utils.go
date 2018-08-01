package kelips

import (
	"encoding/binary"
	"hash"
	"io"
	"math/big"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/euforia/gossip/peers"
)

// lookupGroup returns the affinity group id a peer belongs to. It is calculated by
// hashing the host and applying modulo k (total affinity groups) to the int
// value of the host hash
func lookupGroup(key []byte, k int64, h hash.Hash) int64 {
	h.Write(key)
	sh := h.Sum(nil)
	bi := (&big.Int{}).SetBytes(sh[:])
	m := (&big.Int{}).Mod(bi, big.NewInt(k))
	return m.Int64()
}

func randExpire(min, max time.Duration) time.Duration {
	r := rand.Float64()
	return time.Duration((r * float64(max-min)) + float64(min))
}

func parseGroupContact(s string) (GroupContact, error) {
	var g GroupContact
	i := strings.LastIndex(s, "/")
	if i < 0 {
		g.Host = s
		return g, nil
	}

	g.Host = s[:i]
	id, err := strconv.Atoi(s[i+1:])
	if err != nil {
		return g, err
	}
	g.ID = int64(id)

	return g, nil
}

func peersToPeerContacts(libPeers []*peers.Peer) []PeerContact {
	peers := make([]PeerContact, 0, len(libPeers))
	for _, peer := range libPeers {
		if peer == nil {
			continue
		}
		peers = append(peers, peer)
	}
	return peers
}

func hostStringToBytes(host string) []byte {
	addr, _ := net.ResolveTCPAddr("tcp", host)
	pb := make([]byte, 2)
	binary.BigEndian.PutUint16(pb, uint16(addr.Port))
	return append(addr.IP, pb...)
}

func hostBytesToString(host []byte) string {
	ip := net.IP(host[:16])
	port := binary.BigEndian.Uint16(host[16:18])
	return ip.String() + ":" + strconv.Itoa(int(port))
}

func readTuples(r io.Reader) ([]*Tuple, error) {
	p := make([]byte, 1)
	out := make([]*Tuple, 0)

	for {
		_, err := r.Read(p)
		if err != nil {
			if err == io.EOF {
				return out, nil
			}
			return nil, err
		}

		line := make([]byte, p[0])
		_, err = io.ReadFull(r, line)
		if err != nil {
			return out, err
		}

		out = append(out, &Tuple{
			Key:  line[18:],
			Host: hostBytesToString(line[:18]),
		})
	}
}

// Snapshot writes the keys and associated host to the writer.  The key size is limited
// to 237 chars (255 - 18 byte host addr)
func writeTuples(w io.Writer, tuples []*Tuple) error {
	if len(tuples) == 0 {
		return nil
	}

	for _, t := range tuples {
		line := append(hostStringToBytes(t.Host), t.Key...)
		_, err := w.Write(append([]byte{uint8(len(line))}, line...))
		if err != nil {
			return err
		}
	}

	return nil
}
