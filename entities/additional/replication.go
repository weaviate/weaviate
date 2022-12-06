package additional

// ReplicationProperties are replication-related handles and configurations which
// allow replication context to pass through different layers of
// abstraction, usually initiated via client requests
type ReplicationProperties struct {
	// ConsistencyLevel indicates how many nodes should
	// respond to a request before it is considered
	// successful. Can be "ONE", "QUORUM", or "ALL"
	//
	// This is only relevant for a replicated
	// class
	ConsistencyLevel string

	// NodeName is the node which is expected to
	// fulfill the request
	NodeName string
}
