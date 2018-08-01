package kelips

import (
	"crypto/sha256"
	"hash"
	"time"

	"github.com/hexablock/log"
)

// Config holds the kelips config to init a new instance
type Config struct {
	K                 int64                 // Number of affinity groups
	HashFunc          func() hash.Hash      // Hash function
	TupleTTL          time.Duration         // TTL from last seen before removing
	TupleExpireMinInt time.Duration         // Interval min to check for expirations
	TupleExpireMaxInt time.Duration         // Interval max to check for expirations
	Transport         Transport             // Network transport
	Tuples            TupleStorage          // Tuple store
	Contacts          ContactStorageFactory // Contact store
	Logger            *log.Logger
}

// DefaultConfig returns a sane default Kelips config
func DefaultConfig() *Config {
	return &Config{
		HashFunc:          sha256.New,
		TupleTTL:          45 * time.Second,
		TupleExpireMinInt: 30 * time.Second,
		TupleExpireMaxInt: 40 * time.Second,
		Logger:            log.NewDefaultLogger(),
	}
}

// Validate validates the config and sets defaults
func (conf *Config) Validate() {
	if conf.Tuples == nil {
		conf.Tuples = NewInmemTuples()
	}

	if conf.HashFunc == nil {
		conf.HashFunc = sha256.New
	}

	if conf.TupleTTL == 0 {
		conf.TupleTTL = 30 * time.Second
	}

	if conf.TupleExpireMaxInt == 0 {
		conf.TupleExpireMaxInt = 30 * time.Second
		conf.TupleExpireMinInt = 20 * time.Second
	} else if conf.TupleExpireMinInt == 0 {
		conf.TupleExpireMinInt = 20 * time.Second
		conf.TupleExpireMaxInt = 30 * time.Second
	}
}
