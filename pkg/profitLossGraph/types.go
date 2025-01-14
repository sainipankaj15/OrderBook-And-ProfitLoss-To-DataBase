package profitLossGraph

import "time"

type ProfitLossEntry struct {
	Timestamp time.Time
	Value     float64
}

type DailyProfitLoss struct {
	Date    time.Time
	Entries []ProfitLossEntry
}
