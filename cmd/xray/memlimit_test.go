package main

import "testing"

func TestGoMemLimit(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int64
		ok   bool
	}{
		{"268435456\n", 214748364, true},    // 256Mi -> 80%
		{"max\n", 0, false},                 // cgroup v2, unlimited
		{"9223372036854771712\n", 0, false}, // cgroup v1, unlimited
		{"", 0, false},
		{"0", 0, false},
		{"-1", 0, false},
		{"1", 0, false},              // scales to zero
		{"67108864", 0, false},       // 64Mi -> 51Mi, under the floor
		{"83886080", 67108864, true}, // 80Mi -> exactly the floor
	} {
		got, ok := goMemLimit(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("goMemLimit(%q) = %d, %v; want %d, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}
