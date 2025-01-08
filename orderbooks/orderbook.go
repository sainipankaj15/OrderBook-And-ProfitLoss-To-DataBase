package orderbook

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	constants "profitLossAndTradeInfoToDB/constants"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Order represents a single order entry
type Order struct {
	Timestamp       time.Time `bson:"timestamp" json:"timestamp"`
	TransactionType string    `bson:"transaction_type" json:"transaction_type"`
	Symbol          string    `bson:"symbol" json:"symbol"`
	Product         string    `bson:"product" json:"product"`
	Quantity        int32     `bson:"quantity" json:"quantity"`
	AveragePrice    float64   `bson:"average_price" json:"average_price"`
	OrderStatus     string    `bson:"order_status" json:"order_status"`
	Timestamp3      int64     `bson:"timestamp3" json:"timestamp3"` // Unix timestamp field from the data

	// Metadata fields for time series
	MetaData struct {
		StrikePrice int    `bson:"strike_price" json:"strike_price"`
		OptionType  string `bson:"option_type" json:"option_type"`
	} `bson:"metadata" json:"metadata"`
}

// DailySummary represents the daily trading summary
type DailySummary struct {
	Date              time.Time `bson:"date" json:"date"`
	TotalTrades       int32     `bson:"total_trades" json:"total_trades"`
	TotalBuyQuantity  int32     `bson:"total_buy_quantity" json:"total_buy_quantity"`
	TotalSellQuantity int32     `bson:"total_sell_quantity" json:"total_sell_quantity"`
	UniqueSymbols     int32     `bson:"unique_symbols" json:"unique_symbols"`
	LastUpdated       time.Time `bson:"last_updated" json:"last_updated"`
}

// OrderBook handles MongoDB operations
type OrderBook struct {
	client            *mongo.Client
	ordersCollection  *mongo.Collection
	summaryCollection *mongo.Collection
}

// NewOrderBook creates a new OrderBook instance
func NewOrderBook(ctx context.Context, mongoURI string) (*OrderBook, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	// Ping the database
	if err = client.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	db := client.Database(constants.DB_NAME)

	// Create time series collection for orders
	timeSeriesOpts := options.CreateCollection().SetTimeSeriesOptions(
		options.TimeSeries().
			SetTimeField("timestamp").
			SetMetaField("metadata").
			SetGranularity("minutes"),
	)

	if err := db.CreateCollection(ctx, "orders", timeSeriesOpts); err != nil {
		// Ignore error if collection already exists
		if !mongo.IsDuplicateKeyError(err) {
			return nil, fmt.Errorf("failed to create time series collection: %v", err)
		}
	}

	return &OrderBook{
		client:            client,
		ordersCollection:  db.Collection(constants.ORDERBOOK_SCHEMA),
		summaryCollection: db.Collection(constants.DAILY_SUMMARY_SCHEMA),
	}, nil
}

// extractMetadata extracts strike price and option type from symbol
func extractMetadata(symbol string) (int, string) {
	// Extract strike price - assuming it's the last numbers in the symbol
	strikePrice := 0
	for i := len(symbol) - 1; i >= 0; i-- {
		if symbol[i] >= '0' && symbol[i] <= '9' {
			continue
		}
		if i+1 < len(symbol) {
			strikePrice, _ = strconv.Atoi(symbol[i+1:])
		}
		break
	}

	// Determine option type
	optionType := "C"
	if symbol[len(symbol)-5] == 'P' {
		optionType = "P"
	}

	return strikePrice, optionType
}

// LoadCSVFile loads orders from a CSV file
func (ob *OrderBook) LoadCSVFile(ctx context.Context, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Skip header
	if _, err := reader.Read(); err != nil {
		return fmt.Errorf("failed to read header: %v", err)
	}

	var orders []interface{}
	tradeDate := time.Time{}

	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		fmt.Println("All record", record[0], record[1], record[2], record[3], record[4], record[5], record[6])
		timestamp, err := time.Parse("2006-01-02T15:04:05Z", record[0])
		if err != nil {
			return fmt.Errorf("failed to parse timestamp: %v", err)
		}

		quantity, _ := strconv.Atoi(record[4])
		price, _ := strconv.ParseFloat(record[5], 64)

		strikePrice, optionType := extractMetadata(record[2])

		order := Order{
			Timestamp:       timestamp,
			TransactionType: record[1],
			Symbol:          record[2],
			Product:         record[3],
			Quantity:        int32(quantity),
			AveragePrice:    price,
			OrderStatus:     record[6],
		}
		order.MetaData.StrikePrice = strikePrice
		order.MetaData.OptionType = optionType

		orders = append(orders, order)
		tradeDate = timestamp
	}

	// Insert orders in bulk
	if len(orders) > 0 {
		_, err = ob.ordersCollection.InsertMany(ctx, orders)
		if err != nil {
			return fmt.Errorf("failed to insert orders: %v", err)
		}

		// Update daily summary
		if err := ob.updateDailySummary(ctx, tradeDate); err != nil {
			return fmt.Errorf("failed to update daily summary: %v", err)
		}
	}

	return nil
}

// updateDailySummary updates the daily summary
func (ob *OrderBook) updateDailySummary(ctx context.Context, date time.Time) error {
	startOfDay := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())
	endOfDay := startOfDay.Add(24 * time.Hour)

	pipeline := []bson.M{
		{
			"$match": bson.M{
				"timestamp": bson.M{
					"$gte": startOfDay,
					"$lt":  endOfDay,
				},
			},
		},
		{
			"$group": bson.M{
				"_id":          nil,
				"total_trades": bson.M{"$sum": 1},
				"total_buy_quantity": bson.M{
					"$sum": bson.M{
						"$cond": []interface{}{
							bson.M{"$eq": []interface{}{"$transaction_type", "B"}},
							"$quantity",
							0,
						},
					},
				},
				"total_sell_quantity": bson.M{
					"$sum": bson.M{
						"$cond": []interface{}{
							bson.M{"$eq": []interface{}{"$transaction_type", "S"}},
							"$quantity",
							0,
						},
					},
				},
				"unique_symbols": bson.M{"$addToSet": "$symbol"},
			},
		},
	}

	cursor, err := ob.ordersCollection.Aggregate(ctx, pipeline)
	if err != nil {
		return fmt.Errorf("failed to aggregate daily summary: %v", err)
	}

	var results []bson.M
	if err = cursor.All(ctx, &results); err != nil {
		return fmt.Errorf("failed to get aggregation results: %v", err)
	}

	if len(results) > 0 {
		summary := DailySummary{
			Date:              startOfDay,
			TotalTrades:       results[0]["total_trades"].(int32),
			TotalBuyQuantity:  results[0]["total_buy_quantity"].(int32),
			TotalSellQuantity: results[0]["total_sell_quantity"].(int32),
			// UniqueSymbols:     len(results[0]["unique_symbols"].(bson.A)),
			LastUpdated: time.Now(),
		}

		_, err = ob.summaryCollection.UpdateOne(
			ctx,
			bson.M{"date": startOfDay},
			bson.M{"$set": summary},
			options.Update().SetUpsert(true),
		)
		if err != nil {
			return fmt.Errorf("failed to update daily summary document: %v", err)
		}
	}

	return nil
}

// GetDailySummary retrieves the summary for a specific date
func (ob *OrderBook) GetDailySummary(ctx context.Context, date time.Time) (*DailySummary, error) {
	startOfDay := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())

	var summary DailySummary
	err := ob.summaryCollection.FindOne(ctx, bson.M{"date": startOfDay}).Decode(&summary)
	if err != nil {
		return nil, fmt.Errorf("failed to get daily summary: %v", err)
	}

	return &summary, nil
}

// Close closes the MongoDB connection
func (ob *OrderBook) Close(ctx context.Context) error {
	return ob.client.Disconnect(ctx)
}
