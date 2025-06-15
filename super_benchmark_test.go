package supercharged

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

func BenchmarkDetectAnomalies(b *testing.B) {
	builder := array.NewFloat64Builder(nil)
	for i := range 100000 {
		builder.Append(float64(i))
	}
	col := builder.NewFloat64Array()
	defer col.Release()

	for b.Loop() {
		res, err := DetectAnomalies(context.Background(), col, 3.0)
		if err != nil {
			b.Fatal(err)
		}
		res.Mask.Release()
		// leave Zscore for GC
	}
}
