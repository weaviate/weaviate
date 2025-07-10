package spfresh

// UserConfig defines the configuration options for the SPFresh index.
type UserConfig struct {
	MaxPostingSize int `json:"maxPostingSize,omitempty"` // Maximum number of vectors in a posting
	Workers        int `json:"workers,omitempty"`        // Number of concurrent workers for background operations
}
