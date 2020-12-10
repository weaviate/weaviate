package modules

type StorageProvider interface {
	Storage(name string) (Storage, error)
}

type ScanFn func(k, v []byte) (bool, error)

type Storage interface {
	Get(key []byte) ([]byte, error)
	Scan(scan ScanFn) error
	Put(key, value []byte) error
}
