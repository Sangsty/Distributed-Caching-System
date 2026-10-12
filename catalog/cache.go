package catalog

import "time"

// Cache is what the Service needs from the distributed cache. *client.Client
// satisfies it, which keeps this package free of any network code (and easy to
// test with a fake).
type Cache interface {
	Get(key string) (val []byte, found bool, err error)
	Set(key string, val []byte, ttl time.Duration) error
	Del(key string) (existed bool, err error)
	Owner(key string) (nodeID string, ok bool) // which node owns key (for the X-Cache-Node header)
	Nodes() []string                           // nodes currently on the ring
	Version() uint64                           // ring membership version
}

// NopCache is a Cache that never stores anything: every Get is a miss. Running
// the demo without a coordinator uses it, which gives a "no cache" baseline for
// benchmarks.
type NopCache struct{}

func (NopCache) Get(string) ([]byte, bool, error)        { return nil, false, nil }
func (NopCache) Set(string, []byte, time.Duration) error { return nil }
func (NopCache) Del(string) (bool, error)                { return false, nil }
func (NopCache) Owner(string) (string, bool)             { return "", false }
func (NopCache) Nodes() []string                         { return nil }
func (NopCache) Version() uint64                         { return 0 }
