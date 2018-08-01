package kelips

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var testTuples = []*Tuple{
	&Tuple{
		Key:  []byte("foo"),
		Host: "127.0.0.1:8902",
	},
	&Tuple{
		Key:  []byte("parent/child/grandchild"),
		Host: "127.0.0.1:65432",
	},
	&Tuple{
		Key:  []byte("parent/grandparent/greatgrandparent"),
		Host: "127.0.0.1:3741",
	},
	&Tuple{
		Key:  []byte("database/table/key"),
		Host: "127.0.0.1:12345",
	},
	&Tuple{
		Key:  []byte("cluster/group/node"),
		Host: "127.0.0.1:23456",
	},
	&Tuple{
		Key:  []byte("group.subgroup"),
		Host: "127.0.0.1:34567",
	},
	&Tuple{
		Key:  []byte("key-subkey"),
		Host: "127.0.0.1:3741",
	},
	&Tuple{
		Key:  []byte("value-sub/value"),
		Host: "127.0.0.1:8673",
	},
	&Tuple{
		Key:  []byte("sub/value-"),
		Host: "127.0.0.1:3741",
	},
	&Tuple{
		Key:  []byte("abcdefghijklmnopqrstuvwxyz"),
		Host: "127.0.0.1:9107",
	},
}

func Test_inmemTuples(t *testing.T) {
	testTupleStore := NewInmemTuples()

	// Insert
	for _, tpl := range testTuples {
		testTupleStore.Insert(tpl)
	}
	for _, tpl := range testTuples {
		rt := testTupleStore.Lookup(tpl.Key)
		assert.NotNil(t, rt)
		assert.EqualValues(t, 0, rt.heartbeats, "heartbeats")
		assert.EqualValues(t, 0, rt.lastseen, "lastseen")
	}

	// Ping
	for _, tpl := range testTuples {
		assert.Equal(t, 1, testTupleStore.Ping(tpl.Key), "ping", tpl.Host)
	}
	for _, tpl := range testTuples {
		rt := testTupleStore.Lookup(tpl.Key)
		assert.EqualValues(t, 1, rt.heartbeats, "heartbeats")
		assert.NotEmpty(t, rt.lastseen, "lastseen")
	}

	// Delete
	delKey := testTuples[0].Key
	assert.EqualValues(t, 1, testTupleStore.Delete(delKey))
	assert.Nil(t, testTupleStore.Lookup(delKey))

	// ExpireHost
	assert.EqualValues(t, 3, testTupleStore.ExpireHost("127.0.0.1:3741"))
	for _, tpl := range testTuples {
		if tpl.Host != "127.0.0.1:3741" {
			continue
		}
		assert.Nil(t, testTupleStore.Lookup(tpl.Key))
	}
}

// func Test_Tuple_Marshal_Unmarshal(t *testing.T) {
// 	t1 := &Tuple{Key: []byte("foo")}
// 	b1, err := t1.MarshalBinary()
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	t1out := &Tuple{}
// 	err = t1out.UnmarshalBinary(b1)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	assert.Equal(t, t1.Key, t1out.Key)
// 	assert.Equal(t, t1.Host, t1out.Host)
// 	assert.Equal(t, t1.heartbeats, t1out.heartbeats)
// 	assert.Equal(t, t1.lastseen, t1out.lastseen)

// 	t1.Host = "127.0.0.1:65432"
// 	b2, _ := t1.MarshalBinary()
// 	tout := &Tuple{}
// 	err = tout.UnmarshalBinary(b2)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	assert.Equal(t, t1.Key, tout.Key)
// 	assert.Equal(t, t1.Host, tout.Host)
// 	assert.Equal(t, t1.heartbeats, tout.heartbeats)
// 	assert.Equal(t, t1.lastseen, tout.lastseen)
// }
