package kelips

import (
	"bytes"

	"github.com/euforia/gossip"
	"github.com/hexablock/log"
)

type gossipTupleStorage struct {
	pool *gossip.Pool
	log  *log.Logger
	TupleStorage
}

func (g *gossipTupleStorage) Insert(tuples ...*Tuple) int {
	n := g.TupleStorage.Insert(tuples...)

	local := g.pool.LocalNode()
	buf := bytes.NewBuffer(hostStringToBytes(local.Address()))

	for _, t := range tuples {
		line := append(hostStringToBytes(t.Host), t.Key...)
		_, err := buf.Write(append([]byte{uint8(len(line))}, line...))
		if err != nil {
			g.log.Error("Failed to write insert buffer: ", err)
			return n
		}
	}

	err := g.pool.Broadcast(buf.Bytes())
	if err != nil {
		g.log.Error("Failed to broadcast: ", err)
	}

	return n
}
