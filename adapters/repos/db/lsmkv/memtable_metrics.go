package lsmkv

type memtableMetrics struct {
	put             NsObserver
	setTombstone    NsObserver
	append          NsObserver
	appendMapSorted NsObserver
	get             NsObserver
	getBySecondary  NsObserver
	getMap          NsObserver
	getCollection   NsObserver
	size            Setter
}

// newMemtableMetrics curries the prometheus-functions just once to make sure
// they don't have to be curried on the hotpath where we this would lead to a
// lot of allocations.
func newMemtableMetrics(metrics *Metrics, path, strategy string) *memtableMetrics {
	return &memtableMetrics{
		put:             metrics.MemtableOpObserver(path, strategy, "put"),
		setTombstone:    metrics.MemtableOpObserver(path, strategy, "setTombstone"),
		append:          metrics.MemtableOpObserver(path, strategy, "append"),
		appendMapSorted: metrics.MemtableOpObserver(path, strategy, "appendMapSorted"),
		get:             metrics.MemtableOpObserver(path, strategy, "get"),
		getBySecondary:  metrics.MemtableOpObserver(path, strategy, "getBySecondary"),
		getMap:          metrics.MemtableOpObserver(path, strategy, "getMap"),
		getCollection:   metrics.MemtableOpObserver(path, strategy, "getCollection"),
		size:            metrics.MemtableSizeSetter(path, strategy),
	}
}
