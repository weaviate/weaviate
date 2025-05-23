package hnsw

const (
	DefaultRQEnabled = false
)

type RQConfig struct {
	Enabled bool `json:"enabled"`
}
