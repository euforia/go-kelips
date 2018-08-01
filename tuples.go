package kelips

import (
	"sync"
	"time"
)

// Tuple holds a key to host mapping along with the heartbeat count
type Tuple struct {
	// Tuple key
	Key []byte
	// Host on which the data associated to the key lives
	Host string
	// heartbeats associated with tuples
	heartbeats int64
	// last time heart beat was update
	lastseen int64
}

// Clone returns a clone of the tuple
func (t *Tuple) Clone() *Tuple {
	tuple := &Tuple{
		Key:        make([]byte, len(t.Key)),
		Host:       t.Host,
		heartbeats: t.heartbeats,
		lastseen:   t.lastseen,
	}
	copy(tuple.Key, t.Key)
	return t
}

// ping increments the heartbeat count
func (t *Tuple) ping() {
	t.heartbeats++
	t.lastseen = time.Now().UnixNano()
}

// TupleStorage implements a tuple storage interface
type TupleStorage interface {
	// Ping should increment the tuple counter and the last seen time
	Ping(key ...[]byte) int
	// Remove all tuples not seen in the last d time.Duration
	Expire(d time.Duration) int
	// Remove all tuples with the given host
	ExpireHost(host string) int
	// Insert the tuple
	Insert(...*Tuple) int
	// Delete all given keys returning the number of keys deleted
	Delete(keys ...[]byte) int
	// Returns nil if a tuple for the key is not found
	Lookup(key []byte) *Tuple
	// List all tuples in the store
	List() []*Tuple
}

// InmemTuples implements an inmemory TupleStorage interface
type InmemTuples struct {
	mu sync.RWMutex
	m  map[string]*Tuple
}

// NewInmemTuples returns a new instance of InmemTuples
func NewInmemTuples() *InmemTuples {
	return &InmemTuples{m: make(map[string]*Tuple)}
}

// ExpireHost satisfies the TupleStorage interface
func (tuples *InmemTuples) ExpireHost(host string) int {
	var c int
	tuples.mu.Lock()
	for k, v := range tuples.m {
		if v.Host == host {
			delete(tuples.m, k)
			c++
		}
	}
	tuples.mu.Unlock()
	return c
}

// Expire satisfies the TupleStorage interface
func (tuples *InmemTuples) Expire(d time.Duration) int {
	var c int
	tuples.mu.Lock()
	marker := time.Now().UnixNano() - d.Nanoseconds()
	for k, v := range tuples.m {
		if v.lastseen < marker {
			delete(tuples.m, k)
			c++
		}
	}
	tuples.mu.Unlock()
	return c
}

// Delete satisfies the TupleStorage interface
func (tuples *InmemTuples) Delete(keys ...[]byte) int {
	var c int
	tuples.mu.Lock()
	for _, k := range keys {
		key := string(k)
		if _, ok := tuples.m[key]; ok {
			delete(tuples.m, key)
			c++
		}
	}
	tuples.mu.Unlock()
	return c
}

// Ping satisfies the TupleStorage interface
func (tuples *InmemTuples) Ping(keys ...[]byte) int {
	var c int
	tuples.mu.Lock()
	for _, key := range keys {
		val, ok := tuples.m[string(key)]
		if ok {
			val.ping()
			c++
		}
	}
	tuples.mu.Unlock()
	return c
}

// Insert satisfies the TupleStorage interface
func (tuples *InmemTuples) Insert(tpls ...*Tuple) int {

	var c int

	tuples.mu.Lock()
	for _, tpl := range tpls {
		k := string(tpl.Key)
		if _, ok := tuples.m[k]; !ok {
			tpl.lastseen = time.Now().UnixNano()
			tuples.m[k] = tpl
			// log.Printf("Tuple added: %q", tpl.Key)
			c++
		}
	}
	tuples.mu.Unlock()
	return c
}

// Lookup satisfies the TupleStorage interface
func (tuples *InmemTuples) Lookup(key []byte) *Tuple {
	tuples.mu.RLock()
	defer tuples.mu.RUnlock()

	if val, ok := tuples.m[string(key)]; ok {
		return val.Clone()
	}
	return nil
}

// List satisfies the TupleStorage interface
func (tuples *InmemTuples) List() []*Tuple {
	tuples.mu.RLock()
	defer tuples.mu.RUnlock()

	out := make([]*Tuple, 0, len(tuples.m))
	for _, t := range tuples.m {
		out = append(out, t.Clone())
	}
	return out
}
