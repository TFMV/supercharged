package supercharged

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BenchmarkDetectAnomalies benchmarks the anomaly detection on synthetic data
func BenchmarkDetectAnomalies(b *testing.B) {
	sizes := []int{1_000, 10_000, 100_000, 1_000_000}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			// Create test data
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(b, 0)

			values := make([]float64, size)
			for i := range values {
				values[i] = float64(i%100) + 0.1*float64(i%5)
			}

			data := array.NewFloat64Data(array.NewData(
				arrow.PrimitiveTypes.Float64,
				size,
				[]*memory.Buffer{nil, memory.NewBufferBytes(arrow.Float64Traits.CastToBytes(values))},
				nil,
				0,
				0,
			))
			defer data.Release()

			ctx := context.Background()
			b.ResetTimer()

			for b.Loop() {
				result, err := DetectAnomalies(ctx, data, 2.5)
				if err != nil {
					b.Fatalf("error: %v", err)
				}
				result.Release()
			}
		})
	}
}
