package common

import "context"

type VectorSlice struct {
	Slice []float32
	Mem   []float32
	Buff8 []byte
	Buff  []byte
}

type (
	VectorForID[T float32 | byte] func(ctx context.Context, id uint64) ([]T, error)
	TempVectorForID               func(ctx context.Context, id uint64, container *VectorSlice) ([]float32, error)
	MultiVectorForID              func(ctx context.Context, ids []uint64) ([][]float32, []error)
)
