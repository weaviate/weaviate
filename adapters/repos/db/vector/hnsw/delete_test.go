package hnsw

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDelete(t *testing.T) {
	vectors := vectorsForDeleteTest()
	var vectorIndex *hnsw

	t.Run("import the test vectors", func(t *testing.T) {
		cl := &noopCommitLogger{}
		makeCL := func() CommitLogger {
			return cl
		}

		index, err := New(
			"doesnt-matter-as-committlogger-is-mocked-out",
			"delete-test",
			makeCL, 30, 128,
			func(ctx context.Context, id int32) ([]float32, error) {
				return vectors[int(id)], nil
			})
		require.Nil(t, err)
		vectorIndex = index

		for i, vec := range vectors {
			err := vectorIndex.Add(i, vec)
			require.Nil(t, err)
		}
	})

	var control []int

	t.Run("doing a control search before delete with the respective allow list", func(t *testing.T) {
		allowList := inverted.AllowList{}
		for i := range vectors {
			if i%2 == 0 {
				continue
			}

			allowList.Insert(uint32(i))
		}

		res, err := vectorIndex.SearchByVector([]float32{0.1, 0.1, 0.1}, 20, allowList)
		require.Nil(t, err)
		require.True(t, len(res) > 0)
		control = res
	})

	t.Run("deleting every even element", func(t *testing.T) {
		for i := range vectors {
			if i%2 != 0 {
				continue
			}

			err := vectorIndex.Delete(i)
			require.Nil(t, err)
		}
	})

	t.Run("start a search that should only contain the remaining elements", func(t *testing.T) {
		res, err := vectorIndex.SearchByVector([]float32{0.1, 0.1, 0.1}, 20, nil)
		require.Nil(t, err)
		require.True(t, len(res) > 0)

		for _, elem := range res {
			if elem%2 == 0 {
				t.Errorf("search result contained an even element: %d", elem)
			}
		}

		assert.Equal(t, control, res)
	})

}

// we need a certain number of elements so that we can make sure that nodes
// from all layers will eventually be deleted, otherwise our test only tests
// edge cases which aren't very common in real life, but ignore the most common
// deletes
func vectorsForDeleteTest() [][]float32 {
	return [][]float32{
		[]float32{0.6046603, 0.9405091, 0.6645601},
		[]float32{0.4377142, 0.4246375, 0.68682307},
		[]float32{0.06563702, 0.15651925, 0.09696952},
		[]float32{0.30091187, 0.51521266, 0.81363994},
		[]float32{0.21426387, 0.3806572, 0.31805816},
		[]float32{0.46888983, 0.28303415, 0.29310185},
		[]float32{0.67908466, 0.21855305, 0.20318687},
		[]float32{0.3608714, 0.5706733, 0.8624914},
		[]float32{0.29311424, 0.29708257, 0.752573},
		[]float32{0.20658267, 0.865335, 0.69671917},
		[]float32{0.5238203, 0.028303083, 0.15832828},
		[]float32{0.60725343, 0.9752416, 0.079453625},
		[]float32{0.5948086, 0.05912065, 0.6920246},
		[]float32{0.30152267, 0.17326623, 0.54109985},
		[]float32{0.5441556, 0.27850762, 0.4231522},
		[]float32{0.5305857, 0.2535405, 0.282081},
		[]float32{0.7886049, 0.36180547, 0.8805431},
		[]float32{0.29711226, 0.89436173, 0.097454615},
		[]float32{0.97691685, 0.074291, 0.22228941},
		[]float32{0.6810783, 0.24151509, 0.31152245},
		[]float32{0.9328464, 0.74184895, 0.801055},
		[]float32{0.73023146, 0.18292491, 0.4283571},
		[]float32{0.89699197, 0.6826535, 0.97892934},
		[]float32{0.92221224, 0.09083728, 0.493142},
		[]float32{0.9269868, 0.95494545, 0.34795398},
		[]float32{0.6908388, 0.7109072, 0.5637796},
		[]float32{0.64948946, 0.551765, 0.7558235},
		[]float32{0.4038033, 0.13065112, 0.9859647},
		[]float32{0.89634174, 0.32208398, 0.7211478},
		[]float32{0.6445398, 0.085520506, 0.6695753},
		[]float32{0.6227283, 0.36969283, 0.23682255},
		[]float32{0.5352819, 0.1872461, 0.2388407},
		[]float32{0.6280982, 0.12675293, 0.2813303},
		[]float32{0.41032284, 0.43491247, 0.625095},
		[]float32{0.55014694, 0.6236088, 0.72918075},
		[]float32{0.8305339, 0.0005138155, 0.7360686},
		[]float32{0.39998376, 0.49786812, 0.6039781},
		[]float32{0.4096183, 0.029671282, 0.0019038945},
		[]float32{0.0028430412, 0.9158213, 0.5898342},
		[]float32{0.55939245, 0.8154052, 0.87801176},
	}
}
