# Supercharged Anomaly Detection

A high-performance anomaly detection tool using Apache Arrow for CSV data analysis.

## Features

- Fast CSV reading with Apache Arrow
- Streaming data processing with memory efficiency
- Z-score based anomaly detection
- JSON output support
- Support for various numeric data types

## Installation

```bash
go install github.com/TFMV/supercharged/cmd/supercharged@latest
```

## Usage

```bash
supercharged -file data.csv -column "value" -threshold 3.0 -json
```

### Options

- `-file`: Path to the CSV file (required)
- `-column`: Name of the column to analyze
- `-threshold`: Z-score threshold for anomaly detection (default: 3.0)
- `-json`: Output results in JSON format

## Development

### Prerequisites

- Go 1.21 or later
- Apache Arrow Go v18.0.0

### Building

```bash
go build ./cmd/supercharged
```

### Testing

```bash
go test ./...
```

### Benchmarking

```bash
go test -bench=. ./anomaly
```

## License

MIT License
