package replication

import "testing"

func TestReplication(t *testing.T) {
	t.Run("immediate replica CRUD", immediateReplicaCRUD)
}
