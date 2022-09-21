package hnsw

import "math"

const (
	DistanceCosine    = "cosine"
	DistanceDot       = "dot"
	DistanceL2Squared = "l2-squared"
	DistanceManhattan = "manhattan"
	DistanceHamming   = "hamming"
)

const (
	DefaultCleanupIntervalSeconds = 5 * 60
	DefaultMaxConnections         = 64
	DefaultEFConstruction         = 128
	DefaultEF                     = -1 // indicates "let Weaviate pick"
	DefaultDynamicEFMin           = 100
	DefaultDynamicEFMax           = 500
	DefaultDynamicEFFactor        = 8
	DefaultVectorCacheMaxObjects  = math.MaxInt64
	DefaultSkip                   = false
	DefaultFlatSearchCutoff       = 40000
	DefaultDistanceMetric         = DistanceCosine
)

// UserConfig bundles all values settable by a user in the per-class settings
type UserConfig struct {
	Skip                   bool   `json:"skip"`
	CleanupIntervalSeconds int    `json:"cleanupIntervalSeconds"`
	MaxConnections         int    `json:"maxConnections"`
	EFConstruction         int    `json:"efConstruction"`
	EF                     int    `json:"ef"`
	DynamicEFMin           int    `json:"dynamicEfMin"`
	DynamicEFMax           int    `json:"dynamicEfMax"`
	DynamicEFFactor        int    `json:"dynamicEfFactor"`
	VectorCacheMaxObjects  int    `json:"vectorCacheMaxObjects"`
	FlatSearchCutoff       int    `json:"flatSearchCutoff"`
	Distance               string `json:"distance"`
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return "hnsw"
}

// SetDefaults in the user-specifyable part of the config
func (c *UserConfig) SetDefaults() {
	c.MaxConnections = DefaultMaxConnections
	c.EFConstruction = DefaultEFConstruction
	c.CleanupIntervalSeconds = DefaultCleanupIntervalSeconds
	c.VectorCacheMaxObjects = DefaultVectorCacheMaxObjects
	c.EF = DefaultEF
	c.DynamicEFFactor = DefaultDynamicEFFactor
	c.DynamicEFMax = DefaultDynamicEFMax
	c.DynamicEFMin = DefaultDynamicEFMin
	c.Skip = DefaultSkip
	c.FlatSearchCutoff = DefaultFlatSearchCutoff
	c.Distance = DefaultDistanceMetric
}
