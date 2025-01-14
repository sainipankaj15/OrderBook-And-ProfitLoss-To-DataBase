package profitLossGraph

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"
)

func ReadProfitLossFile(filename string) ([]ProfitLossEntry, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Read the header
	if _, err := reader.Read(); err != nil {
		return nil, err
	}

	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	entries := make([]ProfitLossEntry, 0, len(records))
	for _, record := range records {
		timestamp, err := time.Parse(time.RFC3339, record[0])
		if err != nil {
			return nil, err
		}

		value, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			return nil, err
		}

		entries = append(entries, ProfitLossEntry{
			Timestamp: timestamp,
			Value:     value,
		})
	}

	return entries, nil
}

// GetFileNameForDate generates the filename for a specific date
func GetFileNameForDate(date time.Time) string {
	return fmt.Sprintf("profitLoss_%02d-%02d-%d.csv",
		date.Day(), date.Month(), date.Year())
}
