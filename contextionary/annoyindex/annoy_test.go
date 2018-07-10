/*
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
*/

package annoyindex_test

import (
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/bobvanluijt/annoy-go/annoyindex"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type AnnoyTestSuite struct {
	suite.Suite
}

func Round(f float64) float64 {
	return math.Floor(f + 0.5)
}

func RoundPlus(f float64, places int) float64 {
	shift := math.Pow(10, float64(places))
	return Round(f*shift) / shift
}

func (suite *AnnoyTestSuite) SetupTest() {
}

func (suite *AnnoyTestSuite) TestFileHandling() {
	index := annoyindex.NewAnnoyIndexAngular(3)
	index.AddItem(0, []float32{0, 0, 1})
	index.AddItem(1, []float32{0, 1, 0})
	index.AddItem(2, []float32{1, 0, 0})
	index.Build(10)

	index.Save("go_test.ann")

	info, err := os.Stat("go_test.ann")
	if err != nil {
		assert.Fail(suite.T(), "Failed to create file, file not found")
	}
	if info.Size() == 0 {
		assert.Fail(suite.T(), "Failed to create file, file size zero")
	}

	annoyindex.DeleteAnnoyIndexAngular(index)

	index = annoyindex.NewAnnoyIndexAngular(3)
	if ret := index.Load("go_test.ann"); ret == false {
		assert.Fail(suite.T(), "Failed to load file")
	}
	annoyindex.DeleteAnnoyIndexAngular(index)

	os.Remove("go_test.ann")
}

func (suite *AnnoyTestSuite) TestGetNnsByVector() {
	index := annoyindex.NewAnnoyIndexAngular(3)
	index.AddItem(0, []float32{0, 0, 1})
	index.AddItem(1, []float32{0, 1, 0})
	index.AddItem(2, []float32{1, 0, 0})
	index.Build(10)

	var result []int
	index.GetNnsByVector([]float32{3, 2, 1}, 3, -1, &result)
	assert.Equal(suite.T(), []int{2, 1, 0}, result)

	index.GetNnsByVector([]float32{1, 2, 3}, 3, -1, &result)
	assert.Equal(suite.T(), []int{0, 1, 2}, result)

	index.GetNnsByVector([]float32{2, 0, 1}, 3, -1, &result)
	assert.Equal(suite.T(), []int{2, 0, 1}, result)

	annoyindex.DeleteAnnoyIndexAngular(index)
}

func (suite *AnnoyTestSuite) TestGetNnsByItem() {
	index := annoyindex.NewAnnoyIndexAngular(3)
	index.AddItem(0, []float32{2, 1, 0})
	index.AddItem(1, []float32{1, 2, 0})
	index.AddItem(2, []float32{0, 0, 1})
	index.Build(10)

	var result []int
	index.GetNnsByItem(0, 3, -1, &result)
	assert.Equal(suite.T(), []int{0, 1, 2}, result)

	index.GetNnsByItem(1, 3, -1, &result)
	assert.Equal(suite.T(), []int{1, 0, 2}, result)

	annoyindex.DeleteAnnoyIndexAngular(index)
}

func (suite *AnnoyTestSuite) TestGetItem() {
	index := annoyindex.NewAnnoyIndexAngular(3)
	index.AddItem(0, []float32{2, 1, 0})
	index.AddItem(1, []float32{1, 2, 0})
	index.AddItem(2, []float32{0, 0, 1})
	index.Build(10)

	var result []float32

	index.GetItem(0, &result)
	assert.Equal(suite.T(), []float32{2, 1, 0}, result)

	index.GetItem(1, &result)
	assert.Equal(suite.T(), []float32{1, 2, 0}, result)

	index.GetItem(2, &result)
	assert.Equal(suite.T(), []float32{0, 0, 1}, result)

	annoyindex.DeleteAnnoyIndexAngular(index)
}

func (suite *AnnoyTestSuite) TestGetDistance() {
	index := annoyindex.NewAnnoyIndexAngular(2)
	index.AddItem(0, []float32{0, 1})
	index.AddItem(1, []float32{1, 1})
	index.Build(10)

	assert.Equal(suite.T(), RoundPlus(math.Pow(2*(1.0-math.Pow(2, -0.5)), 0.5), 3), RoundPlus(float64(index.GetDistance(0, 1)), 3))

	annoyindex.DeleteAnnoyIndexAngular(index)
}

func (suite *AnnoyTestSuite) TestLargeEuclideanIndex() {
	index := annoyindex.NewAnnoyIndexEuclidean(10)

	for j := 0; j < 10000; j += 2 {
		p := make([]float32, 0, 10)
		for i := 0; i < 10; i++ {
			p = append(p, rand.Float32())
		}
		x := make([]float32, 0, 10)
		for i := 0; i < 10; i++ {
			x = append(x, 1+p[i]+rand.Float32()*1e-2)
		}
		y := make([]float32, 0, 10)
		for i := 0; i < 10; i++ {
			y = append(y, 1+p[i]+rand.Float32()*1e-2)
		}
		index.AddItem(j, x)
		index.AddItem(j+1, y)
	}
	index.Build(10)
	for j := 0; j < 10000; j += 2 {
		var result []int
		index.GetNnsByItem(j, 2, -1, &result)

		assert.Equal(suite.T(), result, []int{j, j + 1})

		index.GetNnsByItem(j+1, 2, -1, &result)
		assert.Equal(suite.T(), result, []int{j + 1, j})
	}
	annoyindex.DeleteAnnoyIndexEuclidean(index)
}

func TestAnnoyTestSuite(t *testing.T) {
	suite.Run(t, new(AnnoyTestSuite))
}
