package modules

type StorageProvider interface {
	Storage(name string) (Storage, error)
}

type Storage interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
}
