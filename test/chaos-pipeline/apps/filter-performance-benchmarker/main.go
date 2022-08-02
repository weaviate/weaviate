package main

import (
	"log"
	"os"
	"sort"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
)

type BenchmarkResult struct {
	Name         string
	Count        int
	Best         time.Duration
	Worst        time.Duration
	Mean         time.Duration
	Percentile50 time.Duration
	Percentile99 time.Duration
	Percentile90 time.Duration
}

type Measurements []time.Duration

func (m *Measurements) Add(curr time.Duration) {
	*m = append(*m, curr)
}

func (m Measurements) BenchmarkResult(name string) BenchmarkResult {
	sort.Slice(m, func(a, b int) bool { return m[a] < m[b] })

	out := BenchmarkResult{
		Name:         name,
		Best:         m[0],
		Worst:        m[len(m)-1],
		Mean:         mean(m),
		Percentile50: m[int(float64(len(m))*0.5)],
		Percentile90: m[int(float64(len(m))*0.9)],
		Percentile99: m[int(float64(len(m))*0.99)],
		Count:        len(m),
	}

	return out
}

func mean(m Measurements) time.Duration {
	sum := time.Duration(0)

	for _, elem := range m {
		sum += elem
	}

	return sum / time.Duration(len(m))
}

func (b BenchmarkResult) TableRow() table.Row {
	return table.Row{
		b.Name, b.Count, b.Best, b.Mean, b.Worst, b.Percentile50, b.Percentile90, b.Percentile99,
	}
}

func main() {
	results, err := makeRuns()
	if err != nil {
		log.Fatal(err)
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{
		"Name", "Count", "Best", "Mean", "Worst",
		"50th Percentile", "90th Percentile", "99th Percentile",
	})
	for _, result := range results {
		t.AppendRow(result.TableRow())
	}
	t.SetStyle(table.StyleColoredBlackOnBlueWhite)
	t.SetColumnConfigs([]table.ColumnConfig{
		{
			Number:            4,
			TransformerHeader: makeBold,
			TransformerFooter: makeBold,
			Transformer:       makeBold,
		},
	})

	t.Render()
}

func makeBold(val interface{}) string {
	return text.Bold.Sprint(val)
}
