package hnsw

type CommitlogOption func(l *hnswCommitLogger) error

func WithCommitlogThreshold(size int64) CommitlogOption {
	return func(l *hnswCommitLogger) error {
		l.maxSizeIndividual = size
		return nil
	}
}

func WithCommitlogThresholdForCombining(size int64) CommitlogOption {
	return func(l *hnswCommitLogger) error {
		l.maxSizeCombining = size
		return nil
	}
}
