package lsmkv

const (
	// StrategyReplace allows for idem-potent PUT where the latest takes presence
	StrategyReplace       = "replace"
	StrategySetCollection = "setcollection"
	StrategyMapCollection = "mapcollection"
)

type SegmentStrategy uint16

const (
	SegmentStrategyReplace SegmentStrategy = iota
	SegmentStrategySetCollection
	SegmentStrategyMapCollection
)

func SegmentStrategyFromString(in string) SegmentStrategy {
	switch in {
	case StrategyReplace:
		return SegmentStrategyReplace
	case StrategySetCollection:
		return SegmentStrategySetCollection
	case StrategyMapCollection:
		return SegmentStrategyMapCollection
	default:
		panic("unsupport strategy")
	}
}
