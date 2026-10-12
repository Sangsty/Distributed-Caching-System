package ring

// Hash maps a string to a position on the ring (XXH64, seed 0).
// It is exported so experiments can compare against modulo hashing using the
// exact same hash function.
func Hash(s string) uint64 { return sum64([]byte(s)) }
