package vectorizer

import (
	"context"

	"github.com/pkg/errors"
)

func (v *Vectorizer) Texts(ctx context.Context, inputs []string,
	settings ClassSettings) ([]float32, error) {
	return nil, errors.Errorf("vectorizers.Texts not implemented yet")
}
