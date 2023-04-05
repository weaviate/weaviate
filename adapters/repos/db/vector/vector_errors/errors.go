package vector_errors

import (
	"errors"
)

var (
	ErrVectorLengthDoesNotMatch = errors.New("cannot vector-search with vectors of different length")
	ErrNoVectorSearch           = errors.New("cannot vector-search on a class not vector-indexed")
	ErrNoValidShard             = errors.New("no shard returned a valid result. Does any class have the vector index enabled")
)
