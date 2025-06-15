package supercharged

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkDetectAnomalies(b *testing.B) {
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()

	// Pre-allocate capacity
	builder.Reserve(100000)
	for i := 0; i < 100000; i++ {
		builder.Append(float64(i))
	}
	col := builder.NewFloat64Array()
	defer col.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := DetectAnomalies(context.Background(), col, 3.0)
		if err != nil {
			b.Fatal(err)
		}
		res.Release() // This releases both Mask and Zscore
	}
}
