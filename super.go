// Package supercharged provides a supercharged version of the anomaly detection algorithm.
package supercharged

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/substrait-io/substrait-go/v3/expr"
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

// DetectAnomalies computes z-scores and a boolean mask using exprs.Exec.
func DetectAnomalies(ctx context.Context, col *array.Float64, threshold float64) (*Result, error) {
	if col == nil {
		return nil, fmt.Errorf("input array is nil")
	}

	// Build a schema and a single-column record batch
	schema := arrow.NewSchema(
		[]arrow.Field{{Name: "col", Type: arrow.PrimitiveTypes.Float64, Nullable: true}},
		nil,
	)
	batch := array.NewRecord(
		schema,
		[]arrow.Array{col},
		int64(col.Len()),
	)

	// Create expression builder with default extension set
	extSet := exprs.NewDefaultExtensionSet()
	builder := exprs.NewExprBuilder(extSet)
	if err := builder.SetInputSchema(schema); err != nil {
		return nil, fmt.Errorf("set schema: %w", err)
	}

	// Build expression: z = abs((col - mean(col)) / stddev_pop(col))
	colRef := builder.FieldRef("col")
	meanCall, err := builder.CallScalar("mean", nil, colRef)
	if err != nil {
		return nil, fmt.Errorf("mean call: %w", err)
	}

	stdCall, err := builder.CallScalar("stddev_pop", nil, colRef)
	if err != nil {
		return nil, fmt.Errorf("stddev call: %w", err)
	}

	subCall, err := builder.CallScalar("subtract", nil, colRef, meanCall)
	if err != nil {
		return nil, fmt.Errorf("subtract call: %w", err)
	}

	divCall, err := builder.CallScalar("divide", nil, subCall, stdCall)
	if err != nil {
		return nil, fmt.Errorf("divide call: %w", err)
	}

	absCall, err := builder.CallScalar("abs", nil, divCall)
	if err != nil {
		return nil, fmt.Errorf("abs call: %w", err)
	}

	// Build z-score expression
	zExpr, err := absCall.BuildExpr()
	if err != nil {
		return nil, fmt.Errorf("build zscore expr: %w", err)
	}

	// Execute z-score expression
	zDatum, err := exprs.ExecuteScalarExpression(ctx, schema, zExpr, compute.NewDatumWithoutOwning(batch))
	if err != nil {
		return nil, fmt.Errorf("exec zscore: %w", err)
	}
	defer zDatum.Release()

	// Build and execute mask expression: z > threshold
	threshLit := expr.NewPrimitiveLiteral(threshold, false)
	gtCall, err := builder.CallScalar("greater", nil, absCall, builder.Literal(threshLit))
	if err != nil {
		return nil, fmt.Errorf("greater call: %w", err)
	}

	gtExpr, err := gtCall.BuildExpr()
	if err != nil {
		return nil, fmt.Errorf("build mask expr: %w", err)
	}

	mDatum, err := exprs.ExecuteScalarExpression(ctx, schema, gtExpr, compute.NewDatumWithoutOwning(batch))
	if err != nil {
		return nil, fmt.Errorf("exec mask: %w", err)
	}
	defer mDatum.Release()

	// Convert to Arrow arrays
	zArr := zDatum.(*compute.ArrayDatum).Value.(arrow.Array)
	mArr := mDatum.(*compute.ArrayDatum).Value.(arrow.Array)

	// Retain for return
	zArr.Retain()
	mArr.Retain()

	// Type assert to the correct types
	zFloat64 := zArr.(*array.Float64)
	mBool := mArr.(*array.Boolean)

	return &Result{Mask: mBool, Zscore: zFloat64}, nil
}
