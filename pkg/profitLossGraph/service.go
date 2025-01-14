package profitLossGraph

import (
	"context"
	"fmt"
	"time"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{
		repo: repo,
	}
}

// ProcessDailyProfitLoss reads the profit/loss file for a given date and stores it in the database
func (s *Service) ProcessDailyProfitLoss(ctx context.Context, date time.Time) error {
	filename := GetFileNameForDate(date)

	entries, err := ReadProfitLossFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read profit loss file: %w", err)
	}

	if len(entries) == 0 {
		return fmt.Errorf("no entries found in file %s", filename)
	}

	if err := s.repo.SaveProfitLossEntries(ctx, entries); err != nil {
		return fmt.Errorf("failed to save profit loss entries: %w", err)
	}

	return nil
}
