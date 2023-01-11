package entities

const (
	// StrategyReplace allows for idem-potent PUT where the latest takes presence
	StrategyReplace       = "replace"
	StrategySetCollection = "setcollection"
	StrategyMapCollection = "mapcollection"
	StrategyRoaringSet    = "roaringset"
)

type SegmentStrategy uint16

const (
	SegmentStrategyReplace SegmentStrategy = iota
	SegmentStrategySetCollection
	SegmentStrategyMapCollection
	SegmentStrategyRoaringSet
)

func SegmentStrategyFromString(in string) SegmentStrategy {
	switch in {
	case StrategyReplace:
		return SegmentStrategyReplace
	case StrategySetCollection:
		return SegmentStrategySetCollection
	case StrategyMapCollection:
		return SegmentStrategyMapCollection
	case StrategyRoaringSet:
		return SegmentStrategyRoaringSet
	default:
		panic("unsupported strategy")
	}
}
