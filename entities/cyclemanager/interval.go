package cyclemanager

import "time"

const (
	DefaultLSMCompactionInterval = 3 * time.Second
	DefaultMemtableFlushInterval = 100 * time.Millisecond
)
