package supercharged

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestDetectAnomalies(t *testing.T) {
	vals := array.NewFloat64Builder(nil)
	defer vals.Release()
	for _, v := range []float64{1, 2, 3, 100, 2} {
		vals.Append(v)
	}
	col := vals.NewFloat64Array()
	defer col.Release()

	res, err := DetectAnomalies(context.Background(), col, 2.0)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Mask.Release()
	defer res.Zscore.Release()

	// Expect the '100' to be flagged
	if !res.Mask.Value(3) {
		t.Errorf("expected index 3 to be anomalous")
	}
}
