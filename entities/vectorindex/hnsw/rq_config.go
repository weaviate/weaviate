package hnsw

const (
	DefaultRQEnabled = false
	DefaultDataBits  = 1
	DefaultQueryBits = 1
)

type RQConfig struct {
	Enabled   bool  `json:"enabled"`
	DataBits  int16 `json:"dataBits"`
	QueryBits int16 `json:"queryBits"`
}
