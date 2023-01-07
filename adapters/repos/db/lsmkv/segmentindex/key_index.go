package segmentindex

// Key is a helper struct that can be used to build the key nodes for the
// segment index. It contains the primary key and an arbitrary number of
// secondary keys, as well as valueStart and valueEnd indicator. Those are used
// to find the correct payload for each key.
type Key struct {
	Key           []byte
	SecondaryKeys [][]byte
	ValueStart    int
	ValueEnd      int
}
