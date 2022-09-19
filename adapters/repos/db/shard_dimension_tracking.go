package db

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
)

// FIXME: Debugging code, remove
var transfer string

func doThing(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "Data: %v\n", transfer)
}

func init() {
	http.HandleFunc("/dothing", doThing)

	go http.ListenAndServe("0.0.0.0:64000", nil)
}

func (s *Shard) Dimensions() int {
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return 0
	}

	c := b.MapCursor()
	sum := 0
	for k, v := c.First(); k != nil; k, v = c.Next() {
		dimLength := binary.LittleEndian.Uint32(k)
		sum += int(dimLength) * len(v)
	}
	c.Close()

	transfer = fmt.Sprintf("Dimensions: %d,\n\nConfig: %+v\n", sum, s.config)

	return sum
}

func (s *Shard) initDimensionTracking() {
	fmt.Println("!!!initting track vec dimensions", s.config.TrackVectorDimensions)

	if !s.config.TrackVectorDimensions {
		return
	}
	go func() {
		fmt.Println("!!!Starting track vec dimensions counter")
		t := time.NewTicker(5 * time.Second) // 5 minutes

		for {

			if s.stopMetrics {
				return
			}
			fmt.Println("!!!Counting  dimensions ")
			dimCount := s.Dimensions()

			if s.promMetrics != nil {
				metric, err := s.promMetrics.DimensionSum.GetMetricWithLabelValues(s.index.Config.ClassName.String(), s.name)
				if err == nil {
					metric.Set(float64(dimCount))
				} else {
					panic("ick")
				}
			} else {
				panic("ick")
			}
			<-t.C
		}
	}()
}
