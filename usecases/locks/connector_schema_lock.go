package locks

// ConnectorSchemaLock to be implemented through local (mutex) or distributed
// (etcd, redis, ...) means
//
// The returned functions are the respecitve unlock functions
type ConnectorSchemaLock interface {
	LockConnector() (func() error, error)
	LockSchema() (func() error, error)
}
