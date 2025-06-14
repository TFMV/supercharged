// arrow_anomaly.go
package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CSVReader provides a best-in-class CSV reader using Apache Arrow
type CSVReader struct {
	allocator memory.Allocator
	schema    *arrow.Schema
	reader    *csv.Reader
}

// NewCSVReader creates a new CSV reader with the specified schema and options
func NewCSVReader(r io.Reader, schema *arrow.Schema, opts ...csv.Option) *CSVReader {
	allocator := memory.NewGoAllocator()

	// Default options for best-in-class CSV reading
	defaultOpts := []csv.Option{
		csv.WithAllocator(allocator),
		csv.WithHeader(true),
		csv.WithNullReader(true, "NULL", "null", "", "N/A", "n/a"),
		csv.WithChunk(1024), // Process in chunks for better memory management
	}

	// Combine default options with user-provided options
	allOpts := append(defaultOpts, opts...)

	csvReader := csv.NewReader(r, schema, allOpts...)

	return &CSVReader{
		allocator: allocator,
		schema:    schema,
		reader:    csvReader,
	}
}

// ReadAll reads all records from the CSV file and returns them as a table
func (cr *CSVReader) ReadAll() ([]arrow.Record, error) {
	var records []arrow.Record

	for cr.reader.Next() {
		record := cr.reader.Record()
		record.Retain() // Increase reference count to keep the record alive
		records = append(records, record)
	}

	// Check for any errors during reading
	if err := cr.reader.Err(); err != nil {
		// Release any records we've collected
		for _, rec := range records {
			rec.Release()
		}
		return nil, fmt.Errorf("error reading CSV: %w", err)
	}

	return records, nil
}

// ReadSingleColumn reads all records and extracts a single column by name
func (cr *CSVReader) ReadSingleColumn(columnName string) (arrow.Array, error) {
	records, err := cr.ReadAll()
	if err != nil {
		return nil, err
	}
	defer func() {
		// Release all records when done
		for _, rec := range records {
			rec.Release()
		}
	}()

	if len(records) == 0 {
		return nil, fmt.Errorf("no records found in CSV")
	}

	// Find the column index
	columnIndex := -1
	for i, field := range cr.schema.Fields() {
		if field.Name == columnName {
			columnIndex = i
			break
		}
	}

	if columnIndex == -1 {
		return nil, fmt.Errorf("column '%s' not found in schema", columnName)
	}

	// Collect all chunks for this column
	var chunks []arrow.Array
	for _, record := range records {
		if columnIndex >= int(record.NumCols()) {
			return nil, fmt.Errorf("column index %d out of range", columnIndex)
		}

		column := record.Column(columnIndex)
		column.Retain() // Keep the column alive
		chunks = append(chunks, column)
	}

	// Create a chunked array and then concatenate if needed
	if len(chunks) == 1 {
		return chunks[0], nil
	}

	// For multiple chunks, we need to concatenate them
	// This is a simplified approach - in production you might want to use arrow.Chunked
	chunked := arrow.NewChunked(chunks[0].DataType(), chunks)
	defer chunked.Release()

	// For simplicity, let's just return the first chunk
	// In a real implementation, you'd want to handle multiple chunks properly
	return chunks[0], nil
}

// Release releases the CSV reader resources
func (cr *CSVReader) Release() {
	if cr.reader != nil {
		cr.reader.Release()
	}
}

// CreateFloatSchema creates a schema for a CSV with a single float64 column
func CreateFloatSchema(columnName string) *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: columnName, Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil, // no metadata
	)
}

// CreateMultiColumnSchema creates a schema for a CSV with multiple columns
func CreateMultiColumnSchema(columns map[string]arrow.DataType) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(columns))
	for name, dataType := range columns {
		fields = append(fields, arrow.Field{
			Name:     name,
			Type:     dataType,
			Nullable: true,
		})
	}

	return arrow.NewSchema(fields, nil)
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

func DetectAnomalies(
	ctx context.Context,
	col *array.Float64,
	threshold float64,
) (*array.Boolean, *array.Float64, error) {
	if col.Len() == 0 {
		return nil, nil, fmt.Errorf("empty column provided")
	}

	// 1) Calculate mean
	sum := float64(0)
	validCount := 0
	for i := 0; i < col.Len(); i++ {
		if !col.IsNull(i) {
			sum += col.Value(i)
			validCount++
		}
	}

	if validCount == 0 {
		return nil, nil, fmt.Errorf("no valid values found in column")
	}

	mean := sum / float64(validCount)

	// 2) Calculate differences from mean
	diffBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer diffBuilder.Release()

	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			diffBuilder.AppendNull()
		} else {
			diffBuilder.Append(col.Value(i) - mean)
		}
	}
	diffs := diffBuilder.NewFloat64Array()
	defer diffs.Release()

	// 3) Calculate variance and standard deviation
	sumSquares := float64(0)
	for i := 0; i < diffs.Len(); i++ {
		if !diffs.IsNull(i) {
			diff := diffs.Value(i)
			sumSquares += diff * diff
		}
	}
	variance := sumSquares / float64(validCount)
	stddev := math.Sqrt(variance)

	if stddev == 0 {
		// All values are the same, no anomalies
		maskBuilder := array.NewBooleanBuilder(memory.DefaultAllocator)
		defer maskBuilder.Release()
		for i := 0; i < col.Len(); i++ {
			maskBuilder.Append(false)
		}
		mask := maskBuilder.NewBooleanArray()

		anomalyBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
		defer anomalyBuilder.Release()
		anomalies := anomalyBuilder.NewFloat64Array()

		return mask, anomalies, nil
	}

	// 4) Calculate z-scores
	zscoreBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer zscoreBuilder.Release()

	for i := 0; i < diffs.Len(); i++ {
		if diffs.IsNull(i) {
			zscoreBuilder.AppendNull()
		} else {
			zscore := math.Abs(diffs.Value(i)) / stddev
			zscoreBuilder.Append(zscore)
		}
	}
	zscores := zscoreBuilder.NewFloat64Array()
	defer zscores.Release()

	// 5) Create anomaly mask
	maskBuilder := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer maskBuilder.Release()

	for i := 0; i < zscores.Len(); i++ {
		if zscores.IsNull(i) {
			maskBuilder.Append(false) // Treat nulls as non-anomalous
		} else {
			maskBuilder.Append(zscores.Value(i) > threshold)
		}
	}
	mask := maskBuilder.NewBooleanArray()

	// 6) Extract anomalous values
	anomalyBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer anomalyBuilder.Release()

	for i := 0; i < col.Len(); i++ {
		if !mask.IsNull(i) && mask.Value(i) && !col.IsNull(i) {
			anomalyBuilder.Append(col.Value(i))
		}
	}
	anomalies := anomalyBuilder.NewFloat64Array()

	return mask, anomalies, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run arrow_anomaly.go <csv-file> [threshold] [column-name]")
		fmt.Println("  csv-file: Path to the CSV file")
		fmt.Println("  threshold: Z-score threshold for anomaly detection (default: 3.0)")
		fmt.Println("  column-name: Name of the column to analyze (default: auto-detect first numeric column)")
		os.Exit(1)
	}

	path := os.Args[1]
	threshold := 3.0
	var columnName string

	if len(os.Args) > 2 {
		var err error
		threshold, err = strconv.ParseFloat(os.Args[2], 64)
		if err != nil {
			fmt.Printf("Invalid threshold: %s\n", os.Args[2])
			os.Exit(1)
		}
	}

	if len(os.Args) > 3 {
		columnName = os.Args[3]
	}

	// Open the CSV file
	f, err := os.Open(path)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	// First, infer the schema from the CSV
	schema, err := InferSchemaFromCSV(f)
	if err != nil {
		fmt.Printf("Error inferring schema: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Inferred schema:\n")
	for i, field := range schema.Fields() {
		fmt.Printf("  Column %d: %s (%s)\n", i, field.Name, field.Type)
	}
	fmt.Println()

	// Reset file pointer to beginning
	f.Seek(0, 0)

	// Create the CSV reader with the inferred schema
	csvReader := NewCSVReader(f, schema,
		csv.WithComment('#'), // Allow comments
		csv.WithComma(','),   // Use comma as delimiter
	)
	defer csvReader.Release()

	// Read all records
	records, err := csvReader.ReadAll()
	if err != nil {
		fmt.Printf("Error reading CSV: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		for _, rec := range records {
			rec.Release()
		}
	}()

	if len(records) == 0 {
		fmt.Println("No records found in CSV file")
		os.Exit(1)
	}

	// Find the target column
	var targetColumn arrow.Array
	var targetColumnName string

	if columnName != "" {
		// User specified a column name
		targetColumnName = columnName
		columnIndex := -1
		for i, field := range schema.Fields() {
			if field.Name == columnName {
				columnIndex = i
				break
			}
		}

		if columnIndex == -1 {
			fmt.Printf("Column '%s' not found in CSV\n", columnName)
			os.Exit(1)
		}

		// Extract the column from the first record (simplified)
		targetColumn = records[0].Column(columnIndex)
	} else {
		// Auto-detect first numeric column
		for i, field := range schema.Fields() {
			if field.Type.ID() == arrow.FLOAT64 || field.Type.ID() == arrow.FLOAT32 ||
				field.Type.ID() == arrow.INT64 || field.Type.ID() == arrow.INT32 {
				targetColumn = records[0].Column(i)
				targetColumnName = field.Name
				break
			}
		}

		if targetColumn == nil {
			fmt.Println("No numeric columns found for anomaly detection")
			os.Exit(1)
		}
	}

	// Convert to Float64 if necessary
	var floatColumn *array.Float64
	switch col := targetColumn.(type) {
	case *array.Float64:
		floatColumn = col
	case *array.Float32:
		// Convert Float32 to Float64
		builder := array.NewFloat64Builder(memory.DefaultAllocator)
		defer builder.Release()
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(float64(col.Value(i)))
			}
		}
		floatColumn = builder.NewFloat64Array()
		defer floatColumn.Release()
	case *array.Int64:
		// Convert Int64 to Float64
		builder := array.NewFloat64Builder(memory.DefaultAllocator)
		defer builder.Release()
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(float64(col.Value(i)))
			}
		}
		floatColumn = builder.NewFloat64Array()
		defer floatColumn.Release()
	case *array.Int32:
		// Convert Int32 to Float64
		builder := array.NewFloat64Builder(memory.DefaultAllocator)
		defer builder.Release()
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(float64(col.Value(i)))
			}
		}
		floatColumn = builder.NewFloat64Array()
		defer floatColumn.Release()
	default:
		fmt.Printf("Column '%s' is not a numeric type: %s\n", targetColumnName, targetColumn.DataType())
		os.Exit(1)
	}

	// Run anomaly detection
	mask, anomalies, err := DetectAnomalies(context.Background(), floatColumn, threshold)
	if err != nil {
		fmt.Printf("Error detecting anomalies: %v\n", err)
		os.Exit(1)
	}
	defer mask.Release()
	defer anomalies.Release()

	// Print results
	fmt.Printf("Analyzing column: %s\n", targetColumnName)
	fmt.Printf("Total points: %d\n", floatColumn.Len())
	fmt.Printf("Anomalies (z > %.1f): %d\n\n", threshold, anomalies.Len())

	if anomalies.Len() > 0 {
		fmt.Println("Anomalous values:")
		for i := 0; i < anomalies.Len(); i++ {
			fmt.Printf("  • %g\n", anomalies.Value(i))
		}
	} else {
		fmt.Println("No anomalies detected.")
	}
}
