// Package csvreader provides a streaming CSV reader for Arrow.
package csvreader

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CSVReader streams Arrow Records from a CSV.
type CSVReader struct {
	allocator memory.Allocator
	schema    *arrow.Schema
	reader    *csv.Reader
}

// NewCSVReader creates a streaming CSVReader with provided schema.
func NewCSVReader(r io.Reader, schema *arrow.Schema, opts ...csv.Option) *CSVReader {
	allocator := memory.NewGoAllocator()
	defaultOpts := []csv.Option{
		csv.WithAllocator(allocator),
		csv.WithHeader(true),
		csv.WithNullReader(true, "NULL", "null", "", "N/A", "n/a"),
		csv.WithChunk(1024),
	}
	allOpts := append(defaultOpts, opts...)
	reader := csv.NewReader(r, schema, allOpts...)
	return &CSVReader{allocator: allocator, schema: schema, reader: reader}
}

// Chan returns a channel of records; caller must Release each.
func (cr *CSVReader) Chan(ctx context.Context) (<-chan arrow.Record, <-chan error) {
	recs := make(chan arrow.Record)
	errs := make(chan error, 1)
	go func() {
		defer close(recs)
		for cr.reader.Next() {
			rec := cr.reader.Record()
			rec.Retain()
			select {
			case recs <- rec:
			case <-ctx.Done():
				rec.Release()
				return
			}
		}
		if err := cr.reader.Err(); err != nil {
			errs <- fmt.Errorf("csv read error: %w", err)
		}
		close(errs)
	}()
	return recs, errs
}

// ReadSingleColumn concatenates all chunks for a named column.
func (cr *CSVReader) ReadSingleColumn(r io.Reader, columnName string, opts ...csv.Option) (arrow.Array, error) {
	// rewind reader externally before calling
	reader := NewCSVReader(r, cr.schema, opts...)
	ctx := context.Background()
	recs, errs := reader.Chan(ctx)
	var chunks []arrow.Array
	for rec := range recs {
		idx := rec.Schema().FieldIndices(columnName)
		if len(idx) == 0 {
			rec.Release()
			return nil, fmt.Errorf("column %s not found", columnName)
		}
		col := rec.Column(idx[0])
		col.Retain()
		chunks = append(chunks, col)
		rec.Release()
	}
	if err := <-errs; err != nil {
		for _, c := range chunks {
			c.Release()
		}
		return nil, err
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no data for column %s", columnName)
	}
	// concatenate
	concat, err := array.Concatenate(chunks, memory.DefaultAllocator)
	for _, c := range chunks {
		c.Release()
	}
	if err != nil {
		return nil, err
	}
	return concat, nil
}

// InferSchemaFromCSV attempts to infer the schema from the first few rows of CSV
func InferSchemaFromCSV(r io.Reader, opts ...csv.Option) (*arrow.Schema, error) {
	// Use Arrow's built-in schema inference
	defaultOpts := []csv.Option{
		csv.WithAllocator(memory.NewGoAllocator()),
		csv.WithHeader(true),
		csv.WithNullReader(true, "NULL", "null", "", "N/A", "n/a"),
	}

	allOpts := append(defaultOpts, opts...)

	// Create an inferring reader
	inferringReader := csv.NewInferringReader(r, allOpts...)
	defer inferringReader.Release()

	// Read one record to trigger schema inference
	if !inferringReader.Next() {
		if err := inferringReader.Err(); err != nil {
			return nil, fmt.Errorf("error inferring schema: %w", err)
		}
		return nil, fmt.Errorf("no data found for schema inference")
	}

	return inferringReader.Schema(), nil
}
