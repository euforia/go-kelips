package kelips

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hexablock/log"

	"github.com/euforia/gossip"
	"github.com/stretchr/testify/assert"
)

type mockTransport struct {
	groups []AffinityGroup
}

func newMockTransport(n int) *mockTransport {
	return &mockTransport{
		groups: make([]AffinityGroup, n),
	}
}

func (trans *mockTransport) groupIndex() int64 {
	for i, g := range trans.groups {
		if g != nil {
			return int64(i)
		}
	}
	return -1
}

func (trans *mockTransport) groupCount() int {
	var c int
	for _, g := range trans.groups {
		if g != nil {
			c++
		}
	}
	return c
}

func (trans *mockTransport) AddPeer(c GroupContact, host PeerContact) error {
	g := trans.groups[c.ID]
	if g != nil {
		return g.AddPeer(host)
	}
	return nil
}

func (trans *mockTransport) Insert(c GroupContact, key []byte) (string, error) {
	return trans.groups[c.ID].Insert(key)
}

func (trans *mockTransport) Lookup(c GroupContact, req *Request) (string, error) {
	return trans.groups[c.ID].Lookup(req)
}

func (trans *mockTransport) Register(c GroupContact, g AffinityGroup) {
	trans.groups[c.ID] = g
}

func (trans *mockTransport) Start(ln net.Listener) error {
	return nil
}

func (trans *mockTransport) Shutdown(context.Context) error {
	return nil
}

var (
	testKeys = [][]byte{
		[]byte("foo/bar/bas"),
		[]byte("one-tw-three"),
		[]byte("key"),
		[]byte("abc123948deaff2"),
		[]byte("kelips"),
	}
)

func testKelipsNew(ip string, port, k int, trans Transport) *Kelips {
	conf := DefaultConfig()
	conf.K = int64(k)
	// conf.AdvertiseAddr = ip
	// conf.BindAddr = ip
	// conf.AdvertisePort = port
	// conf.BindPort = port
	conf.Transport = trans
	return New(fmt.Sprintf("%s:%d", ip, port), conf)
}

func Test_New(t *testing.T) {
	klp := testKelipsNew("127.0.0.1", 9999, 3, newMockTransport(3))
	// klp := New(&Config{
	// 	K: 3,
	// 	// AdvAddr:   "127.0.0.1:9999",
	// 	HashFunc:  sha256.New,
	// 	Transport: newMockTransport(3),
	// })
	assert.Equal(t, 3, len(klp.groups))

	klp.AddPeer(&Peer{Host: "127.0.0.1:10000"})
	klp.AddPeer(&Peer{Host: "127.0.0.1:8914"})

	var local, remote int
	for _, grp := range klp.groups {

		if grp.IsLocal() {
			local++
			g := grp.(*affinityGroup)
			assert.NotNil(t, g.trans)
		} else {
			remote++
			g := grp.(*remoteAffinityGroup)
			trans := g.trans.(*mockTransport)
			var a int
			for _, g := range trans.groups {
				if g != nil {
					a++
				}
			}
			assert.Equal(t, 1, a)
		}
	}

	assert.Equal(t, 1, local)
	assert.Equal(t, 2, remote)
}

func makeMockNetwork(start, k int) []*Kelips {
	knet := make([]*Kelips, k)
	for i := 0; i < k; i++ {
		// addr := fmt.Sprintf("127.0.0.1:%d", start+i)
		knet[i] = testKelipsNew("127.0.0.1", start+i, k, newMockTransport(k))
	}

	for i, kn := range knet {

		for j, kns := range knet {
			if i == j {
				continue
			}

			_, err := kn.AddPeer(&Peer{Host: kns.groups[kns.id].Contact().Host})
			if err != nil {
				panic(err)
			}
		}
	}

	return knet
}

func Test_Kelips_assignments(t *testing.T) {
	knet := makeMockNetwork(44400, 3)

	// Check group assignment
	for _, kn := range knet {
		for _, group := range kn.groups {
			var trans *mockTransport

			if group.IsLocal() {
				gc := group.Contact()
				g := group.(*affinityGroup)
				trans = g.trans.(*mockTransport)
				assert.Equal(t, gc.ID, trans.groupIndex())
			} else {
				g := group.(*remoteAffinityGroup)
				trans = g.trans.(*mockTransport)
			}
			assert.Equal(t, 1, trans.groupCount())
		}
	}

	// Check peer assignments
	all := make([]map[int]int, 0, 3)

	for ni, kn := range knet {
		fmt.Println("Node", ni)
		count := make(map[int]int)
		for i, group := range kn.groups {
			if group.IsLocal() {
				grp := group.(*affinityGroup)
				fmt.Println(" local", grp.contacts.List())
				count[i] = len(grp.contacts.List())
			} else {
				grp := group.(*remoteAffinityGroup)
				fmt.Println(" remote", grp.contacts.List())
				count[i] = len(grp.contacts.List())
			}
		}
		all = append(all, count)
	}

	fmt.Println(all)
}

func makeTestNetwork(start, k int) []*Kelips {
	knet := make([]*Kelips, k)
	for i := 0; i < k; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", start+i)
		conf := &Config{
			K:                 int64(k),
			Transport:         NewHTTPTransport(false),
			TupleTTL:          1 * time.Second,
			TupleExpireMinInt: 750 * time.Millisecond,
			TupleExpireMaxInt: 1 * time.Second,
			Logger:            log.NewDefaultLogger(),
		}
		// conf.BindAddr = "127.0.0.1"
		// conf.AdvertiseAddr = conf.BindAddr
		// conf.BindPort = start + i
		// conf.AdvertisePort = conf.BindPort
		knet[i] = New(addr, conf)

		ln, err := net.Listen("tcp", addr)
		if err != nil {
			panic(err)
		}
		knet[i].Start(ln)
	}

	for i, kn := range knet {

		for j, kns := range knet {
			if i == j {
				continue
			}
			kn.AddPeer(&Peer{Host: kns.groups[kns.id].Contact().Host})
		}
	}

	return knet
}

func testRequest(key string) *Request {
	return &Request{
		Key: []byte(key),
		TTL: 1,
	}
}

func Test_Kelips(t *testing.T) {
	knet := makeTestNetwork(55500, 3)

	hi, err := knet[0].Insert([]byte("foobar"))
	assert.Nil(t, err)

	h1, err := knet[0].Lookup(testRequest("foobar"))
	assert.Nil(t, err)

	h2, err := knet[1].Lookup(testRequest("foobar"))
	assert.Nil(t, err)

	h3, err := knet[2].Lookup(testRequest("foobar"))
	assert.Nil(t, err)

	assert.Equal(t, hi, h1)
	assert.Equal(t, hi, h2)
	assert.Equal(t, hi, h3)

	results := make([]string, 0, len(testKeys))
	for i, k := range testKeys {
		n := i % 3
		host, err := knet[n].Insert(k)
		if err != nil {
			if strings.Contains(err.Error(), "no contacts") {
				continue
			}
			t.Fatal(err)
		}

		t.Logf("%s -> %s", k, host)
		results = append(results, host)

		// Check inserted on all peers
		for _, kn := range knet {
			lhost, err := kn.Lookup(&Request{Key: k, TTL: 1})
			assert.Nil(t, err)
			assert.Equal(t, host, lhost)
		}

	}

	<-time.After(2 * time.Second)
	if len(results) == 0 {
		t.Fatal("all should not fail")
	}

	for i, kn := range knet {
		for _, key := range testKeys {
			host, err := kn.Lookup(&Request{Key: key, TTL: 2})
			assert.NotNil(t, err, "node=%d host=%s", i, host)
		}
	}

	// TODO: check they expired

	ctx := context.Background()
	for _, kn := range knet {
		kn.Shutdown(ctx)
	}
}

func makeTestKelipsGossip(port int, k int64) (*Kelips, *Gossip, error) {
	ip := "127.0.0.1"
	addr := fmt.Sprintf("%s:%d", ip, port)
	conf := &Config{
		K:                 k,
		Transport:         NewHTTPTransport(true),
		TupleTTL:          1 * time.Second,
		TupleExpireMinInt: 750 * time.Millisecond,
		TupleExpireMaxInt: 1 * time.Second,
		Tuples:            NewInmemTuples(),
		Logger:            log.NewDefaultLogger(),
	}

	gossipConf := gossip.DefaultConfig()
	gossipConf.Name = addr
	gossipConf.AdvertiseAddr = ip
	gossipConf.AdvertisePort = port
	gossipConf.BindAddr = ip
	gossipConf.BindPort = port
	g, err := NewGossip(conf, gossipConf)
	if err != nil {
		return nil, nil, err
	}
	conf.Contacts = g

	return New(addr, conf), g, nil
}
