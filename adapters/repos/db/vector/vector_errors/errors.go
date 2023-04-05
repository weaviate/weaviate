package vector_errors

import (
	"errors"
)

var (
	ErrVectorLengthDoesNotMatch = errors.New("cannot vector-search with vectors of different length")
	ErrNoVectorSearch           = errors.New("cannot vector-search on a class not vector-indexed")
)
