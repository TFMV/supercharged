package supercharged

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// Result holds mask and z-scores for anomalies.
type Result struct {
	Mask   *array.Boolean
	Zscore *array.Float64
}

// Release frees memory associated with the Result.
func (r *Result) Release() {
	if r.Mask != nil {
		r.Mask.Release()
	}
	if r.Zscore != nil {
		r.Zscore.Release()
	}
}

// computeMeanAndVariance calculates mean and variance for a Float64 array
func computeMeanAndVariance(col *array.Float64) (mean, variance float64) {
	var sum, sumsq float64
	var count int
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		v := col.Value(i)
		sum += v
		sumsq += v * v
		count++
	}
	if count == 0 {
		return 0, 0
	}
	mean = sum / float64(count)
	// Population variance: sum of squared differences from mean
	variance = 0
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		diff := col.Value(i) - mean
		variance += diff * diff
	}
	variance /= float64(count)
	return
}

// DetectAnomalies computes z-scores and a boolean mask using Arrow compute functions.
func DetectAnomalies(ctx context.Context, col arrow.Array, threshold float64) (*Result, error) {
	// Ensure we have a Float64 array
	floatCol, ok := col.(*array.Float64)
	if !ok {
		return nil, fmt.Errorf("input must be Float64 array, got %T", col)
	}

	// 1. Compute mean and variance manually
	mean, variance := computeMeanAndVariance(floatCol)

	// 2. Create scalars for broadcasting
	meanScalar := scalar.NewFloat64Scalar(mean)
	varianceScalar := scalar.NewFloat64Scalar(variance)

	// 3. Compute standard deviation using Arrow compute
	stdDevResult, err := compute.CallFunction(ctx, "sqrt", nil, compute.NewDatum(varianceScalar))
	if err != nil {
		return nil, fmt.Errorf("sqrt computation: %w", err)
	}
	defer stdDevResult.Release()

	stdDevDatum := stdDevResult.(*compute.ScalarDatum)
	stdDev := stdDevDatum.Value.(*scalar.Float64).Value
	stdDevScalar := scalar.NewFloat64Scalar(stdDev)

	// 4. Subtract mean from each value
	diffResult, err := compute.CallFunction(ctx, "subtract", nil, compute.NewDatum(col), compute.NewDatum(meanScalar))
	if err != nil {
		return nil, fmt.Errorf("subtract computation: %w", err)
	}
	defer diffResult.Release()

	// 5. Divide by standard deviation to get z-scores
	zscoreResult, err := compute.CallFunction(ctx, "divide", nil, diffResult, compute.NewDatum(stdDevScalar))
	if err != nil {
		return nil, fmt.Errorf("divide computation: %w", err)
	}

	// 6. Take absolute value of z-scores
	absResult, err := compute.CallFunction(ctx, "abs", nil, zscoreResult)
	if err != nil {
		return nil, fmt.Errorf("abs computation: %w", err)
	}
	defer absResult.Release()

	// Get z-scores array
	zscoreDatum := zscoreResult.(*compute.ArrayDatum)
	zscore := array.MakeFromData(zscoreDatum.Value).(*array.Float64)

	// 7. Compare with threshold using Arrow compute
	thresholdScalar := scalar.NewFloat64Scalar(threshold)
	compResult, err := compute.CallFunction(ctx, "greater_equal", nil, absResult, compute.NewDatum(thresholdScalar))
	if err != nil {
		return nil, fmt.Errorf("threshold comparison: %w", err)
	}
	defer compResult.Release()

	// Get the boolean mask from the comparison result
	maskDatum := compResult.(*compute.ArrayDatum)
	mask := array.MakeFromData(maskDatum.Value).(*array.Boolean)

	return &Result{
		Mask:   mask,
		Zscore: zscore,
	}, nil
}
