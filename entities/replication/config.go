package replication

// GlobalConfig represents system-wide config that may restrict settings of an
// individual class
type GlobalConfig struct {
	// MinimumFactor can enforce replication. For example, with MinimumFactor set
	// to 2, users can no longer create classes with a factor of 1, therefore
	// forcing them to have replicated classes.
	MinimumFactor int `json:"minimum_factor" yaml:"minimum_factor"`
}
