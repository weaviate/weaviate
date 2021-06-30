package sharding

import "github.com/pkg/errors"

func ValidateConfigUpdate(old, updated Config) error {
	if old.DesiredCount != updated.DesiredCount {
		return errors.Errorf("re-sharding not supported yet: shard count is immutable: "+
			"attempted change from \"%d\" to \"%d\"", old.DesiredCount,
			updated.DesiredCount)
	}

	if old.VirtualPerPhysical != updated.VirtualPerPhysical {
		return errors.Errorf("virtual shards per physical is immutable: "+
			"attempted change from \"%d\" to \"%d\"", old.VirtualPerPhysical,
			updated.VirtualPerPhysical)
	}

	return nil
}
