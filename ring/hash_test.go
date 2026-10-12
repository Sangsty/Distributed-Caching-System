package ring

import "testing"

// content(n) is a deterministic byte pattern; the expected values below were
// produced by the reference libxxhash (XXH64, seed 0), so this test pins our
// pure-Go implementation to the reference algorithm across every code path
// (empty, <4, 4-7, 8-31, and the 32-byte stripe loop with tails).
func content(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	return b
}

func TestXXH64ReferenceVectors(t *testing.T) {
	known := []struct {
		in   string
		want uint64
	}{
		{"", 0xef46db3751d8e999},
		{"a", 0xd24ec4f1a98c6e5b},
		{"abc", 0x44bc2cf5ad770999},
		{"node-1#0", 9739388931511830626},
		{"user:42", 15861654238046376386},
		{"hello world", 5020219685658847592},
	}
	for _, k := range known {
		if got := Hash(k.in); got != k.want {
			t.Errorf("Hash(%q) = %d, want %d", k.in, got, k.want)
		}
	}

	patterned := []struct {
		n    int
		want uint64
	}{
		{0, 17241709254077376921},
		{1, 12208272383309036471},
		{3, 6261856666793576441},
		{4, 14271086647985540868},
		{7, 12663836542954871438},
		{8, 4442176141076628448},
		{9, 5410980096290937865},
		{15, 12549904796550154919},
		{16, 11644853062034244627},
		{31, 5365180931665220769},
		{32, 10184845083914585149},
		{33, 7118499008196474468},
		{63, 6643383451930527103},
		{64, 8915644864283660670},
		{65, 17552792968290000321},
		{100, 17266991381171192145},
		{255, 3187902827951256350},
	}
	for _, p := range patterned {
		if got := sum64(content(p.n)); got != p.want {
			t.Errorf("sum64(content(%d)) = %d, want %d", p.n, got, p.want)
		}
	}
}
