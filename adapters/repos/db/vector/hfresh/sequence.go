package spfresh

import (
	"sync/atomic"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

// Sequence represents a monotonic id generator used by SPFresh to
// generate unique ids for stored postings and other entities.
// It is inspired by Postgres Sequences, although much simpler
// and it's designed to ensure the following properties:
// 1. Monotonicity: Each generated id is guaranteed to be greater than
// the previously generated id.
// 2. High Throughput: The generator is optimized for high performance
// 3. Persistence: The state of the generator is persisted to disk.
// 4. Concurrency: The generator is safe for concurrent access.
// However because of its design, it does not guarantee gapless ids.
//
// A Sequence first allocates a range of ids and reserves them in the
// persistent storage. It then serves those ids from memory without
// touching the disk.
// If the sequence is closed gracefully, if persists the last used id, however
// in case of a crash, the sequence will start from
// the upper bound of the reserved range, potentially leaving gaps.
type Sequence struct {
	counter    common.MonotonicCounter
	upperBound atomic.Uint64
}
