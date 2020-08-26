package hnsw

import (
	"context"
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDelete_WithoutCleaningUpTombstones(t *testing.T) {
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

func TestDelete_WithCleaningUpTombstonesOnce(t *testing.T) {
	// there is a single bulk clean event after all the deletes
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

	fmt.Printf("entrypoint before %d\n", vectorIndex.entryPointID)
	t.Run("deleting every even element", func(t *testing.T) {
		for i := range vectors {
			if i%2 != 0 {
				continue
			}

			err := vectorIndex.Delete(i)
			require.Nil(t, err)
		}
	})

	t.Run("runnign the cleanup", func(t *testing.T) {
		err := vectorIndex.CleanUpTombstonedNodes()
		require.Nil(t, err)
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

	t.Run("verify the graph no longer has any tombstones", func(t *testing.T) {
		assert.Len(t, vectorIndex.tombstones, 0)
	})

}

func TestDelete_WithCleaningUpTombstonesInBetween(t *testing.T) {
	// there is a single bulk clean event after all the deletes
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

	fmt.Printf("entrypoint before %d\n", vectorIndex.entryPointID)
	t.Run("deleting every even element", func(t *testing.T) {
		for i := range vectors {
			if i%10 == 0 {
				// occassionally run clean up
				err := vectorIndex.CleanUpTombstonedNodes()
				require.Nil(t, err)
			}

			if i%2 != 0 {
				continue
			}

			err := vectorIndex.Delete(i)
			require.Nil(t, err)
		}

		// finally run one final cleanup
		err := vectorIndex.CleanUpTombstonedNodes()
		require.Nil(t, err)
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

	t.Run("verify the graph no longer has any tombstones", func(t *testing.T) {
		assert.Len(t, vectorIndex.tombstones, 0)
	})

	t.Run("delete the remaining elements", func(t *testing.T) {
		for i := range vectors {
			if i%2 == 0 {
				continue
			}

			err := vectorIndex.Delete(i)
			require.Nil(t, err)
		}

		err := vectorIndex.CleanUpTombstonedNodes()
		require.Nil(t, err)
	})

	t.Run("try to insert again and search", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			err := vectorIndex.Add(i, vectors[i])
			require.Nil(t, err)
		}

		res, err := vectorIndex.SearchByVector([]float32{0.1, 0.1, 0.1}, 20, nil)
		require.Nil(t, err)
		assert.ElementsMatch(t, []int{0, 1, 2, 3, 4}, res)
	})

}

// we need a certain number of elements so that we can make sure that nodes
// from all layers will eventually be deleted, otherwise our test only tests
// edge cases which aren't very common in real life, but ignore the most common
// deletes
func vectorsForDeleteTest() [][]float32 {
	return [][]float32{
		[]float32{0.27335858, 0.42670676, 0.12599982},
		[]float32{0.34369454, 0.78510034, 0.78000546},
		[]float32{0.2342731, 0.076864816, 0.6405078},
		[]float32{0.07597838, 0.7752282, 0.87022865},
		[]float32{0.78632426, 0.06902865, 0.7423889},
		[]float32{0.3055758, 0.3901508, 0.9399572},
		[]float32{0.48687622, 0.26338226, 0.06495104},
		[]float32{0.5384028, 0.35410047, 0.8821815},
		[]float32{0.25123185, 0.62722564, 0.86443096},
		[]float32{0.58484185, 0.13103616, 0.4034975},
		[]float32{0.0019696166, 0.46822622, 0.42492124},
		[]float32{0.42401955, 0.8278863, 0.5952888},
		[]float32{0.15367928, 0.70778894, 0.0070928824},
		[]float32{0.95760256, 0.45898128, 0.1541115},
		[]float32{0.9125976, 0.9021616, 0.21607016},
		[]float32{0.9876307, 0.5243228, 0.37294936},
		[]float32{0.8194746, 0.56142205, 0.5130103},
		[]float32{0.805065, 0.62250346, 0.63715476},
		[]float32{0.9969276, 0.5115748, 0.18916714},
		[]float32{0.16419733, 0.15029702, 0.36020836},
		[]float32{0.9660323, 0.35887036, 0.6072966},
		[]float32{0.72765416, 0.27891788, 0.9094314},
		[]float32{0.8626208, 0.3540126, 0.3100354},
		[]float32{0.7153876, 0.17094712, 0.7801294},
		[]float32{0.23180388, 0.107446484, 0.69542855},
		[]float32{0.54731685, 0.8949827, 0.68316746},
		[]float32{0.15049729, 0.1293767, 0.0574729},
		[]float32{0.89379513, 0.67022973, 0.57360715},
		[]float32{0.725353, 0.25326362, 0.44264215},
		[]float32{0.2568602, 0.4986094, 0.9759933},
		[]float32{0.7300015, 0.70019704, 0.49546525},
		[]float32{0.54314494, 0.2004176, 0.63803226},
		[]float32{0.6180191, 0.5260845, 0.9373999},
		[]float32{0.63356537, 0.81430644, 0.78373694},
		[]float32{0.69995105, 0.84198904, 0.17851257},
		[]float32{0.5197941, 0.11502675, 0.95129955},
		[]float32{0.15791401, 0.07516741, 0.113447875},
		[]float32{0.06811827, 0.4450082, 0.98595786},
		[]float32{0.7153448, 0.41833848, 0.06332495},
		[]float32{0.6704102, 0.28931814, 0.031580303},
		[]float32{0.47773632, 0.73334247, 0.6925025},
		[]float32{0.7976896, 0.9499536, 0.6394833},
		[]float32{0.3074854, 0.14025249, 0.35961738},
		[]float32{0.49956197, 0.093575336, 0.790093},
		[]float32{0.4641653, 0.21276893, 0.528895},
		[]float32{0.1021849, 0.9416305, 0.46738508},
		[]float32{0.3790398, 0.50099677, 0.98233247},
		[]float32{0.39650732, 0.020929832, 0.53968865},
		[]float32{0.77604437, 0.8554197, 0.24056046},
		[]float32{0.07174444, 0.28758526, 0.67587185},
		[]float32{0.22292718, 0.66624546, 0.6077909},
		[]float32{0.22090498, 0.36197436, 0.40415043},
		[]float32{0.04838009, 0.120789215, 0.17928012},
		[]float32{0.55166364, 0.3400502, 0.43698996},
		[]float32{0.7638108, 0.47014108, 0.23208627},
		[]float32{0.9239513, 0.8418566, 0.23518613},
		[]float32{0.289589, 0.85010827, 0.055741556},
		[]float32{0.32436147, 0.18756394, 0.4217864},
		[]float32{0.041671168, 0.37824047, 0.66486764},
		[]float32{0.5052222, 0.07982704, 0.64345413},
		[]float32{0.62675995, 0.20138603, 0.8231867},
		[]float32{0.86306876, 0.9698708, 0.11398846},
		[]float32{0.68566775, 0.22026269, 0.13525572},
		[]float32{0.57706076, 0.32325208, 0.6122228},
		[]float32{0.80035216, 0.18560356, 0.6328281},
		[]float32{0.87145543, 0.19380389, 0.8863942},
		[]float32{0.33777508, 0.6056442, 0.9110077},
		[]float32{0.3961719, 0.49714503, 0.14191929},
		[]float32{0.5344362, 0.8166916, 0.75880384},
		[]float32{0.015749464, 0.63223976, 0.5470922},
		[]float32{0.10512444, 0.2212036, 0.24995685},
		[]float32{0.10831311, 0.27044898, 0.8668174},
		[]float32{0.3272971, 0.6659298, 0.87119603},
		[]float32{0.42913893, 0.14528985, 0.69957525},
		[]float32{0.33012474, 0.81964344, 0.092787445},
		[]float32{0.093618214, 0.90637344, 0.94406706},
		[]float32{0.12161567, 0.75131124, 0.40563175},
		[]float32{0.9154454, 0.75925833, 0.8406739},
		[]float32{0.81649286, 0.9025715, 0.3105051},
		[]float32{0.2927649, 0.22649862, 0.9708593},
		[]float32{0.30813727, 0.0079439245, 0.39662006},
		[]float32{0.94943213, 0.36778906, 0.217876},
		[]float32{0.716794, 0.3811725, 0.18448676},
		[]float32{0.66879725, 0.29722908, 0.0031202603},
		[]float32{0.11104216, 0.13094379, 0.0787222},
		[]float32{0.8508966, 0.86416596, 0.15885831},
		[]float32{0.2303136, 0.56660503, 0.17114973},
		[]float32{0.8632685, 0.4229249, 0.1936724},
		[]float32{0.03060897, 0.35226125, 0.8115969},
	}
}
