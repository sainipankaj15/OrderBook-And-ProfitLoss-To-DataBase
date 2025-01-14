package profitLossGraph

import (
	"context"
	"fmt"
	"profitLossAndTradeInfoToDB/constants"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type Repository struct {
	collection *mongo.Collection
}

func NewRepository(db *mongo.Database) (*Repository, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	return &Repository{
		collection: db.Collection(constants.PROFITLOSS_SCHEMA),
	}, nil
}

func (r *Repository) SaveProfitLossEntries(ctx context.Context, entries []ProfitLossEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Convert entries to interface{} for bulk write
	documents := make([]interface{}, len(entries))
	for i, entry := range entries {
		documents[i] = entry
	}

	// Perform bulk insert
	_, err := r.collection.InsertMany(ctx, documents)
	if err != nil {
		return fmt.Errorf("failed to insert entries: %w", err)
	}

	return nil
}

// GetProfitLossByDateRange retrieves profit/loss entries within a date range
func (r *Repository) GetProfitLossByDateRange(ctx context.Context, startDate, endDate time.Time) ([]ProfitLossEntry, error) {
	filter := bson.M{
		"timestamp": bson.M{
			"$gte": startDate,
			"$lte": endDate,
		},
	}

	cursor, err := r.collection.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to query profit loss: %w", err)
	}
	defer cursor.Close(ctx)

	var entries []ProfitLossEntry
	if err := cursor.All(ctx, &entries); err != nil {
		return nil, fmt.Errorf("failed to decode entries: %w", err)
	}

	return entries, nil
}
