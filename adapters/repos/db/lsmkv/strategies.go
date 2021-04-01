package lsmkv

const (
	// StrategyReplace allows for idem-potent PUT where the latest takes presence
	StrategyReplace    = "replace"
	StrategyCollection = "collection"
)

type SegmentStrategy uint16

const (
	SegmentStrategyReplace SegmentStrategy = iota
	SegmentStrategyCollection
)

func SegmentStrategyFromString(in string) SegmentStrategy {
	switch in {
	case StrategyReplace:
		return SegmentStrategyReplace
	case StrategyCollection:
		return SegmentStrategyCollection
	default:
		panic("unsupport strategy")
	}
}
