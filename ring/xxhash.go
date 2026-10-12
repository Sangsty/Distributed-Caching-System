package ring

import (
	"encoding/binary"
	"math/bits"
)

// XXH64 (seed 0), implemented here so the project has zero external
// dependencies. It is a fast, well-distributed non-cryptographic hash, which
// is exactly what a hash ring needs. The reference algorithm is by Yann Collet
// (https://github.com/Cyan4973/xxHash). hash_test.go checks this against
// known reference vectors.

const (
	prime1 uint64 = 11400714785074694791
	prime2 uint64 = 14029467366897019727
	prime3 uint64 = 1609587929392839161
	prime4 uint64 = 9650029242287828579
	prime5 uint64 = 2870177450012600261
)

func round(acc, input uint64) uint64 {
	acc += input * prime2
	acc = bits.RotateLeft64(acc, 31)
	return acc * prime1
}

func mergeRound(acc, val uint64) uint64 {
	val = round(0, val)
	acc ^= val
	return acc*prime1 + prime4
}

// sum64 returns the XXH64 hash (seed 0) of b.
func sum64(b []byte) uint64 {
	n := len(b)
	var h uint64

	if n >= 32 {
		// Variables (not constants) so the arithmetic wraps modulo 2^64 as
		// the algorithm requires; constant expressions would overflow.
		p1, p2 := prime1, prime2
		v1 := p1 + p2 // seed 0
		v2 := p2
		v3 := uint64(0)
		v4 := -p1
		for len(b) >= 32 {
			v1 = round(v1, binary.LittleEndian.Uint64(b[0:8]))
			v2 = round(v2, binary.LittleEndian.Uint64(b[8:16]))
			v3 = round(v3, binary.LittleEndian.Uint64(b[16:24]))
			v4 = round(v4, binary.LittleEndian.Uint64(b[24:32]))
			b = b[32:]
		}
		h = bits.RotateLeft64(v1, 1) + bits.RotateLeft64(v2, 7) +
			bits.RotateLeft64(v3, 12) + bits.RotateLeft64(v4, 18)
		h = mergeRound(h, v1)
		h = mergeRound(h, v2)
		h = mergeRound(h, v3)
		h = mergeRound(h, v4)
	} else {
		h = prime5 // seed 0
	}

	h += uint64(n)

	for len(b) >= 8 {
		k1 := round(0, binary.LittleEndian.Uint64(b[:8]))
		h ^= k1
		h = bits.RotateLeft64(h, 27)*prime1 + prime4
		b = b[8:]
	}
	if len(b) >= 4 {
		h ^= uint64(binary.LittleEndian.Uint32(b[:4])) * prime1
		h = bits.RotateLeft64(h, 23)*prime2 + prime3
		b = b[4:]
	}
	for _, c := range b {
		h ^= uint64(c) * prime5
		h = bits.RotateLeft64(h, 11) * prime1
	}

	h ^= h >> 33
	h *= prime2
	h ^= h >> 29
	h *= prime3
	h ^= h >> 32
	return h
}
