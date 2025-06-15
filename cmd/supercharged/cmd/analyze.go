package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	anomaly "github.com/TFMV/supercharged"
	"github.com/TFMV/supercharged/csvreader"
	"github.com/apache/arrow-go/v18/arrow/array"
)

var analyzeCmd = &cobra.Command{
	Use:   "analyze",
	Short: "Run anomaly detection on a CSV column",
	RunE: func(cmd *cobra.Command, args []string) error {
		path := viper.GetString("file")
		if path == "" {
			return fmt.Errorf("--file is required")
		}
		column := viper.GetString("column")
		if column == "" {
			return fmt.Errorf("--column is required")
		}

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open: %w", err)
		}
		defer f.Close()

		schema, err := csvreader.InferSchemaFromCSV(f)
		if err != nil {
			return fmt.Errorf("infer: %w", err)
		}
		if _, err := f.Seek(0, 0); err != nil {
			return fmt.Errorf("seek: %w", err)
		}

		arr, err := csvreader.NewCSVReader(f, schema).ReadSingleColumn(f, column)
		if err != nil {
			return fmt.Errorf("read column: %w", err)
		}
		defer arr.Release()

		colArr, ok := arr.(*array.Float64)
		if !ok {
			return fmt.Errorf("unsupported array type: %T", arr)
		}

		res, err := anomaly.DetectAnomalies(context.Background(), colArr, viper.GetFloat64("threshold"))
		if err != nil {
			return fmt.Errorf("detect anomalies: %w", err)
		}
		defer res.Mask.Release()
		defer res.Zscore.Release()

		type Out struct {
			Count     int64     `json:"count"`
			Anomalies []float64 `json:"anomalies"`
		}
		out := Out{Count: int64(colArr.Len())}
		for i := 0; i < int(res.Mask.Len()); i++ {
			if res.Mask.Value(i) {
				out.Anomalies = append(out.Anomalies, res.Zscore.Value(i))
			}
		}

		if viper.GetBool("json") {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(out)
		}

		fmt.Printf("Total: %d\nAnomalies: %v\n", out.Count, out.Anomalies)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(analyzeCmd)
}
