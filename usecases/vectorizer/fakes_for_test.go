package vectorizer

import "context"

type fakeClient struct {
	lastInput []string
}

func (c *fakeClient) VectorForCorpi(ctx context.Context, corpi []string, overrides map[string]string) ([]float32, error) {
	c.lastInput = corpi
	return []float32{0, 1, 2, 3}, nil
}

func (c *fakeClient) VectorForWord(ctx context.Context, word string) ([]float32, error) {
	c.lastInput = []string{word}
	return []float32{3, 2, 1, 0}, nil

}
func (c *fakeClient) NearestWordsByVector(ctx context.Context,
	vector []float32, n int, k int) ([]string, []float32, error) {
	return []string{"word1", "word2"}, []float32{0.1, 0.2}, nil
}

func (c *fakeClient) IsWordPresent(ctx context.Context, word string) (bool, error) {
	return true, nil
}
