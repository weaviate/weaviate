package commitlog

import (
	"os"
	"testing"
)

func BenchmarkSetEntryPoint(b *testing.B) {
	defer os.Remove("./testfile")
	ids := make([]uint64, 100)
	levels := make([]int, 100)

	l := NewLogger("./testfile")

	b.ReportAllocs()

	for j := 0; j < b.N; j++ {
		for i := 0; i < 100; i++ {
			l.SetEntryPointWithMaxLayer(ids[i], levels[i])
		}
	}
}

func BenchmarkAddNode(b *testing.B) {
	defer os.Remove("./testfile")
	ids := make([]uint64, 100)
	levels := make([]int, 100)

	l := NewLogger("./testfile")

	b.ReportAllocs()

	for j := 0; j < b.N; j++ {
		for i := 0; i < 100; i++ {
			l.AddNode(ids[i], levels[i])
		}
	}
}

func BenchmarkAddLinkAtLevel(b *testing.B) {
	defer os.Remove("./testfile")
	ids := make([]uint64, 100)
	levels := make([]int, 100)
	links := make([]uint64, 100)

	l := NewLogger("./testfile")

	b.ReportAllocs()

	for j := 0; j < b.N; j++ {
		for i := 0; i < 100; i++ {
			l.AddLinkAtLevel(ids[i], levels[i], links[i])
		}
	}
}

func BenchmarkReplaceLinksAtLevel32(b *testing.B) {
	defer os.Remove("./testfile")
	ids := make([]uint64, 100)
	levels := make([]int, 100)
	links := make([][]uint64, 100)
	for i := range links {
		links[i] = make([]uint64, 32)
	}

	l := NewLogger("./testfile")

	b.ReportAllocs()

	for j := 0; j < b.N; j++ {
		for i := 0; i < 100; i++ {
			l.ReplaceLinksAtLevel(ids[i], levels[i], links[i])
		}
	}
}

func BenchmarkReplaceLinksAtLevel33(b *testing.B) {
	defer os.Remove("./testfile")
	ids := make([]uint64, 100)
	levels := make([]int, 100)
	links := make([][]uint64, 100)
	for i := range links {
		links[i] = make([]uint64, 33)
	}

	l := NewLogger("./testfile")

	b.ReportAllocs()

	for j := 0; j < b.N; j++ {
		for i := 0; i < 100; i++ {
			l.ReplaceLinksAtLevel(ids[i], levels[i], links[i])
		}
	}
}

func BenchmarkAddTombstone(b *testing.B) {
	defer os.Remove("./testfile")
	ids := make([]uint64, 100)

	l := NewLogger("./testfile")

	b.ReportAllocs()

	for j := 0; j < b.N; j++ {
		for i := 0; i < 100; i++ {
			l.AddTombstone(ids[i])
		}
	}
}

func BenchmarkRemoveTombstone(b *testing.B) {
	defer os.Remove("./testfile")
	ids := make([]uint64, 100)

	l := NewLogger("./testfile")

	b.ReportAllocs()

	for j := 0; j < b.N; j++ {
		for i := 0; i < 100; i++ {
			l.AddTombstone(ids[i])
		}
	}
}

func BenchmarkClearLinks(b *testing.B) {
	defer os.Remove("./testfile")
	ids := make([]uint64, 100)

	l := NewLogger("./testfile")

	b.ReportAllocs()

	for j := 0; j < b.N; j++ {
		for i := 0; i < 100; i++ {
			l.ClearLinks(ids[i])
		}
	}
}

func BenchmarkDeleteNode(b *testing.B) {
	ids := make([]uint64, 100)

	l := NewLogger("./testfile")

	b.ReportAllocs()

	for j := 0; j < b.N; j++ {
		for i := 0; i < 100; i++ {
			l.DeleteNode(ids[i])
		}
	}
}

func BenchmarkReset(b *testing.B) {
	defer os.Remove("./testfile")
	l := NewLogger("./testfile")

	b.ReportAllocs()

	for j := 0; j < b.N; j++ {
		for i := 0; i < 100; i++ {
			l.Reset()
		}
	}
}
