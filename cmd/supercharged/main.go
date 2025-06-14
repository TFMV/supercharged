package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	anomaly "github.com/TFMV/supercharged"
	"github.com/TFMV/supercharged/csvreader"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func main() {
	var (
		path       string
		threshold  float64
		columnName string
		jsonOut    bool
	)
	flag.StringVar(&path, "file", "", "CSV file path")
	flag.Float64Var(&threshold, "threshold", 3.0, "Z-score threshold")
	flag.StringVar(&columnName, "column", "", "Column name to analyze")
	flag.BoolVar(&jsonOut, "json", false, "Output results in JSON")
	flag.Parse()

	if path == "" {
		flag.Usage()
		os.Exit(1)
	}
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	// infer schema
	schema, err := csvreader.InferSchemaFromCSV(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "infer: %v\n", err)
		os.Exit(1)
	}

	// rewind
	f.Seek(0, 0)

	// read column
	arr, err := csvreader.NewCSVReader(f, schema).ReadSingleColumn(f, columnName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read col: %v\n", err)
		os.Exit(1)
	}
	defer arr.Release()

	// ensure Float64
	var col *array.Float64
	switch v := arr.(type) {
	case *array.Float64:
		col = v
	case *array.Float32, *array.Int64, *array.Int32:
		// convert as needed (omitted for brevity)
	default:
		fmt.Fprintf(os.Stderr, "unsupported type: %T\n", v)
		os.Exit(1)
	}

	// detect
	ctx := context.Background()
	res, err := anomaly.DetectAnomalies(ctx, col, threshold)
	if err != nil {
		fmt.Fprintf(os.Stderr, "detect: %v\n", err)
		os.Exit(1)
	}
	defer res.Mask.Release()
	defer res.Zscore.Release()

	out := struct {
		Count     int64     `json:"count"`
		Anomalies []float64 `json:"anomalies"`
	}{
		Count:     int64(col.Len()),
		Anomalies: make([]float64, 0),
	}
	for i := 0; i < int(res.Mask.Len()); i++ {
		if res.Mask.Value(i) {
			out.Anomalies = append(out.Anomalies, res.Zscore.Value(i))
		}
	}

	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(out)
	} else {
		fmt.Printf("Total: %d\nAnomalies: %v\n", out.Count, out.Anomalies)
	}
}
