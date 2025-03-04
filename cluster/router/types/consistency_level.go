package types

// ConsistencyLevel is an enum of all possible consistency level
type ConsistencyLevel string

const (
	ConsistencyLevelOne    ConsistencyLevel = "ONE"
	ConsistencyLevelQuorum ConsistencyLevel = "QUORUM"
	ConsistencyLevelAll    ConsistencyLevel = "ALL"
)

// ToInt returns the minimum number needed to satisfy consistency level l among N
func (l ConsistencyLevel) ToInt(n int) int {
	switch l {
	case ConsistencyLevelAll:
		return n
	case ConsistencyLevelQuorum:
		return n/2 + 1
	default:
		return 1
	}
}
