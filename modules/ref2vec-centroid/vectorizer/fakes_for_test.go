package vectorizer

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/mock"
)

type fakeClassConfig map[string]interface{}

func (cfg fakeClassConfig) Class() map[string]interface{} {
	return cfg
}

func (cfg fakeClassConfig) Property(string) map[string]interface{} {
	return nil
}

type fakeRefVecRepo struct {
	mock.Mock
}

func (r *fakeRefVecRepo) ReferenceVectorSearch(ctx context.Context, obj *models.Object,
	refProps map[string]struct{},
) ([][]float32, error) {
	args := r.Called(ctx, obj, refProps)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([][]float32), args.Error(1)
}
