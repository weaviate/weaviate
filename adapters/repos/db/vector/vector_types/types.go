package vector_types

// RQEncoding represents a compressed vector encoding
type RQEncoding struct {
	CenterIdx              int     // Index of the center in the centers slice
	CenterDotRaw           float32 // Dot product between center and raw vector
	CenterDistRaw          float32 // Euclidean distance between center and raw vector
	RawNormSquared         float32 // Euclidean norm squared of the raw vector
	EstimatorNormalization float32 // Normalization term for cosine similarity estimator
	Code                   []uint8 // Byte encoding of the centered and normalized vector
}
