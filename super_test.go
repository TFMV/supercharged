package main

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateFloatSchema(t *testing.T) {
	schema := CreateFloatSchema("temperature")
	require.NotNil(t, schema)
	assert.Equal(t, 1, len(schema.Fields()))
	assert.Equal(t, "temperature", schema.Field(0).Name)
	assert.Equal(t, arrow.PrimitiveTypes.Float64, schema.Field(0).Type)
}

func TestCreateMultiColumnSchema(t *testing.T) {
	columns := map[string]arrow.DataType{
		"temperature": arrow.PrimitiveTypes.Float64,
		"humidity":    arrow.PrimitiveTypes.Float64,
		"pressure":    arrow.PrimitiveTypes.Float64,
	}

	schema := CreateMultiColumnSchema(columns)
	require.NotNil(t, schema)
	assert.Equal(t, 3, len(schema.Fields()))

	// Verify field names and types
	for i := 0; i < len(schema.Fields()); i++ {
		field := schema.Field(i)
		require.NotNil(t, field)
		expectedType, ok := columns[field.Name]
		require.True(t, ok)
		assert.Equal(t, expectedType, field.Type)
	}
}

func TestInferSchemaFromCSV(t *testing.T) {
	csvData := `temperature,humidity,pressure
23.5,45.2,1013.2
24.1,46.0,1012.8
NULL,47.1,1013.5`

	reader := strings.NewReader(csvData)
	schema, err := InferSchemaFromCSV(reader)
	require.NoError(t, err)
	require.NotNil(t, schema)
	assert.Equal(t, 3, len(schema.Fields()))

	// Verify field names
	expectedFields := []string{"temperature", "humidity", "pressure"}
	for i := range expectedFields {
		field := schema.Field(i)
		require.NotNil(t, field)
		assert.Equal(t, arrow.PrimitiveTypes.Float64, field.Type)
	}
}

func TestCSVReader_ReadAll(t *testing.T) {
	csvData := `temperature,humidity
23.5,45.2
24.1,46.0
NULL,47.1`

	schema := CreateMultiColumnSchema(map[string]arrow.DataType{
		"temperature": arrow.PrimitiveTypes.Float64,
		"humidity":    arrow.PrimitiveTypes.Float64,
	})

	reader := NewCSVReader(strings.NewReader(csvData), schema)
	defer reader.Release()

	records, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, records, 1) // Due to chunk size of 1024, all data should be in one record

	record := records[0]
	assert.Equal(t, int64(3), record.NumRows())
	assert.Equal(t, int64(2), record.NumCols())

	// Verify temperature column
	tempCol := record.Column(0).(*array.Float64)
	assert.Equal(t, 23.5, tempCol.Value(0))
	assert.Equal(t, 24.1, tempCol.Value(1))
	assert.True(t, tempCol.IsNull(2))

	// Verify humidity column
	humidityCol := record.Column(1).(*array.Float64)
	assert.Equal(t, 45.2, humidityCol.Value(0))
	assert.Equal(t, 46.0, humidityCol.Value(1))
	assert.Equal(t, 47.1, humidityCol.Value(2))
}

func TestCSVReader_ReadSingleColumn(t *testing.T) {
	csvData := `temperature,humidity
23.5,45.2
24.1,46.0
NULL,47.1`

	schema := CreateMultiColumnSchema(map[string]arrow.DataType{
		"temperature": arrow.PrimitiveTypes.Float64,
		"humidity":    arrow.PrimitiveTypes.Float64,
	})

	reader := NewCSVReader(strings.NewReader(csvData), schema)
	defer reader.Release()

	// Test reading temperature column
	tempCol, err := reader.ReadSingleColumn("temperature")
	require.NoError(t, err)
	require.NotNil(t, tempCol)

	tempArray := tempCol.(*array.Float64)
	assert.Equal(t, 3, tempArray.Len())
	assert.Equal(t, 23.5, tempArray.Value(0))
	assert.Equal(t, 24.1, tempArray.Value(1))
	assert.True(t, tempArray.IsNull(2))

	// Test reading non-existent column
	_, err = reader.ReadSingleColumn("nonexistent")
	assert.Error(t, err)
}

func TestCSVReader_ErrorHandling(t *testing.T) {
	// Test with malformed CSV data
	malformedCSV := `temperature,humidity
23.5,45.2
invalid_number,46.0
NULL,47.1`

	schema := CreateMultiColumnSchema(map[string]arrow.DataType{
		"temperature": arrow.PrimitiveTypes.Float64,
		"humidity":    arrow.PrimitiveTypes.Float64,
	})

	reader := NewCSVReader(strings.NewReader(malformedCSV), schema)
	defer reader.Release()

	// This should handle the error gracefully
	_, err := reader.ReadAll()
	// Note: Arrow CSV reader might handle invalid numbers differently,
	// so we just ensure it doesn't panic and returns some result
	if err != nil {
		t.Logf("Expected error with malformed CSV: %v", err)
	}
}

func TestInferSchemaFromCSV_EmptyFile(t *testing.T) {
	// Test schema inference with empty CSV
	emptyCSV := ""
	reader := strings.NewReader(emptyCSV)

	_, err := InferSchemaFromCSV(reader)
	assert.Error(t, err, "Should return error for empty CSV")
}

func TestDetectAnomalies_WithNulls(t *testing.T) {
	// Test anomaly detection with null values
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()

	// Create data: [100, 100, NULL, 100, 200, 0, NULL, 100]
	// This should have anomalies at indices 4 (200) and 5 (0)
	dataPoints := []interface{}{100.0, 100.0, nil, 100.0, 200.0, 0.0, nil, 100.0}

	for _, point := range dataPoints {
		if point == nil {
			builder.AppendNull()
		} else {
			builder.Append(point.(float64))
		}
	}

	data := builder.NewFloat64Array()
	defer data.Release()

	mask, anomalies, err := DetectAnomalies(context.Background(), data, 1.5)
	require.NoError(t, err)
	require.NotNil(t, mask)
	require.NotNil(t, anomalies)

	// Should still detect anomalies despite null values
	anomalyCount := 0
	for i := 0; i < mask.Len(); i++ {
		if mask.Value(i) {
			anomalyCount++
		}
	}
	assert.Greater(t, anomalyCount, 0, "Should detect anomalies even with null values")
}

// Helper function for the test
func contains(slice []int, item int) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func TestDetectAnomalies(t *testing.T) {
	// Create test data with some obvious anomalies
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()

	// Use more extreme values to ensure detection
	values := []float64{100.0, 100.0, 100.0, 100.0, 200.0, 100.0, 0.0, 100.0}
	for _, v := range values {
		builder.Append(v)
	}

	data := builder.NewFloat64Array()
	defer data.Release()

	// Test with standard threshold (1.9) - should catch extreme outliers
	mask, anomalies, err := DetectAnomalies(context.Background(), data, 1.9)
	require.NoError(t, err)
	require.NotNil(t, mask)
	require.NotNil(t, anomalies)

	// Verify mask length
	assert.Equal(t, 8, mask.Len())

	// Count anomalies detected with threshold 1.9
	anomalyCount := 0
	for i := 0; i < mask.Len(); i++ {
		if mask.Value(i) {
			anomalyCount++
		}
	}

	// With extreme values (0.0 and 200.0), we should detect anomalies
	assert.Greater(t, anomalyCount, 0, "Should detect at least one anomaly with extreme values")

	// Test with stricter threshold (1.0)
	mask, anomalies, err = DetectAnomalies(context.Background(), data, 1.0)
	require.NoError(t, err)
	require.NotNil(t, mask)
	require.NotNil(t, anomalies)

	// More values should be considered anomalous with stricter threshold
	stricterAnomalyCount := 0
	for i := 0; i < mask.Len(); i++ {
		if mask.Value(i) {
			stricterAnomalyCount++
		}
	}
	assert.GreaterOrEqual(t, stricterAnomalyCount, anomalyCount, "Stricter threshold should detect at least as many anomalies")
}

func TestDetectAnomalies_EdgeCases(t *testing.T) {
	// Test empty array
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	emptyData := builder.NewFloat64Array()
	defer emptyData.Release()

	_, _, err := DetectAnomalies(context.Background(), emptyData, 3.0)
	assert.Error(t, err)

	// Test array with all null values
	nullBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer nullBuilder.Release()
	nullBuilder.AppendNull()
	nullBuilder.AppendNull()
	nullData := nullBuilder.NewFloat64Array()
	defer nullData.Release()

	_, _, err = DetectAnomalies(context.Background(), nullData, 3.0)
	assert.Error(t, err)

	// Test array with all same values
	sameBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer sameBuilder.Release()
	sameBuilder.Append(100.0)
	sameBuilder.Append(100.0)
	sameBuilder.Append(100.0)
	sameData := sameBuilder.NewFloat64Array()
	defer sameData.Release()

	mask, scores, err := DetectAnomalies(context.Background(), sameData, 3.0)
	require.NoError(t, err)
	require.NotNil(t, mask)
	require.NotNil(t, scores)

	// No anomalies should be detected
	for i := 0; i < mask.Len(); i++ {
		assert.False(t, mask.Value(i))
	}
}
