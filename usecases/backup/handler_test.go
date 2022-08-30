package backup

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/snapshots"
)

type fakeSchemaManger struct{}

func (f *fakeSchemaManger) RestoreClass(context.Context, *models.Principal,
	*models.Class, *snapshots.Snapshot,
) error {
	return nil
}

type fakeAuthorizer struct{}

func (f *fakeAuthorizer) Authorize(principal *models.Principal, verb, resource string) error {
	return nil
}
