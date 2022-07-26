package lsmkv

type bloomFilterMetrics struct {
	trueNegative  TimeObserver
	falsePositive TimeObserver
	truePositive  TimeObserver
}

// newBloomFilterMetrics curries the prometheus metrics just once at
// initialization to prevent further allocs on the hot path
func newBloomFilterMetrics(metrics *Metrics) *bloomFilterMetrics {
	return &bloomFilterMetrics{
		trueNegative:  metrics.BloomFilterObserver("replace", "get_true_negative"),
		falsePositive: metrics.BloomFilterObserver("replace", "get_false_positive"),
		truePositive:  metrics.BloomFilterObserver("replace", "get_true_positive"),
	}
}
