package helper

import (
	"testing"
	"time"
)

func EnablePQ(t *testing.T, className string, pq map[string]interface{}) {
	class := GetClass(t, className)
	cfg := class.VectorIndexConfig.(map[string]interface{})
	cfg["pq"] = pq
	class.VectorIndexConfig = cfg
	UpdateClass(t, class)
	// Time for compression to complete
	time.Sleep(2 * time.Second)
}
