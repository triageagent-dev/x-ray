package main

import (
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

// memLimitShare is the part of the container's memory limit the Go runtime
// aims to stay under. The rest is headroom for what the collector does not
// count: goroutine stacks, cgo (tree-sitter) allocations and the runtime.
const memLimitShare = 0.8

// minGoMemLimit is the smallest limit worth setting: below it the collector
// would run almost continuously, and a limit that scales to zero would be
// read as "collect always".
const minGoMemLimit = 64 << 20

// cgroupMemoryFiles hold the container memory limit: cgroup v2, then v1.
var cgroupMemoryFiles = []string{
	"/sys/fs/cgroup/memory.max",
	"/sys/fs/cgroup/memory/memory.limit_in_bytes",
}

// limitMemoryFromCgroup sets the Go soft memory limit to memLimitShare of the
// container's memory limit. Without one the collector waits for the heap to
// double, which in a small container can be past the limit, so the kernel
// kills the pod before a collection runs. An explicit GOMEMLIMIT wins, and
// nothing is set outside a limited container.
func limitMemoryFromCgroup() {
	if os.Getenv("GOMEMLIMIT") != "" {
		return
	}
	for _, f := range cgroupMemoryFiles {
		raw, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		if limit, ok := goMemLimit(string(raw)); ok {
			debug.SetMemoryLimit(limit)
			log.Info().Int64("bytes", limit).Msg("go memory limit set from the container limit")
		}
		return
	}
}

// goMemLimit turns the text of a cgroup memory limit file into the Go limit,
// false when the file says the container is unlimited ("max", or the huge
// value cgroup v1 reports) or is not a number.
func goMemLimit(raw string) (int64, bool) {
	raw = strings.TrimSpace(raw)
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 || n >= 1<<60 {
		return 0, false
	}
	limit := int64(float64(n) * memLimitShare)
	if limit < minGoMemLimit {
		return 0, false
	}
	return limit, true
}
