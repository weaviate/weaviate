package segmentindex

type Strategy uint16

const (
	StrategyReplace Strategy = iota
	StrategySetCollection
	StrategyMapCollection
	StrategyRoaringSet
)
