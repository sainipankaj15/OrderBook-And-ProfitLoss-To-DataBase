package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"time"

	"profitLossAndTradeInfoToDB/constants"
	orderbook "profitLossAndTradeInfoToDB/orderbooks"
	"profitLossAndTradeInfoToDB/pkg/profitLossGraph"

	"github.com/joho/godotenv"
	"go.uber.org/zap"
)

// Config holds application configuration
type Config struct {
	MongoURI    string
	CSVDir      string
	ProcessDate string
}

func main() {
	// Setup configuration
	config := parseFlags()

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt)
	go func() {
		<-shutdown
		log.Println("Shutting down gracefully...")
		cancel()
	}()

	// Initialize OrderBook
	ob, err := orderbook.NewOrderBook(ctx, config.MongoURI)
	if err != nil {
		log.Fatalf("Failed to initialize OrderBook: %v", err)
	}
	defer func() {
		if err := ob.Close(ctx); err != nil {
			log.Printf("Error closing MongoDB connection: %v", err)
		}
	}()

	// Get MongoDB database instance from OrderBook
	// Note: You'll need to expose the DB from OrderBook or create a new connection
	mongoClient := ob.GetMongoClient()            // You'll need to add this method to OrderBook
	db := mongoClient.Database(constants.DB_NAME) // Use the same database as OrderBook

	// Initialize ProfitLoss repository and service
	plRepo, err := profitLossGraph.NewRepository(db)
	if err != nil {
		log.Fatalf("Failed to initialize ProfitLoss repository: %v", err)
	}

	prl, err := plRepo.GetProfitLossByDateRange(ctx, time.Now().AddDate(0, 0, -1), time.Now())
	if err != nil {
		log.Fatalf("Failed to get profit loss: %v", err)
	}

	fmt.Println(prl)

	plService := profitLossGraph.NewService(plRepo)

	// Process files based on date
	if err := processFiles(ctx, ob, plService, config); err != nil {
		log.Fatalf("Failed to process files: %v", err)
	}

	// Get and display summary
	// if err := displaySummary(ctx, ob, config); err != nil {
	// 	log.Fatalf("Failed to display summary: %v", err)
	// }
}

func parseFlags() Config {
	config := Config{}

	flag.StringVar(&config.MongoURI, "mongo-uri", os.Getenv("MONGODB_CONNECTION_URL"),
		"MongoDB connection string")
	flag.StringVar(&config.CSVDir, "csv-dir", ".",
		"Directory containing CSV files")
	flag.StringVar(&config.ProcessDate, "date", time.Now().Format("2006-01-02"),
		"Date to process (YYYY-MM-DD)")

	flag.Parse()

	return config
}

func processFiles(ctx context.Context, ob *orderbook.OrderBook, plService *profitLossGraph.Service, config Config) error {
	// Parse the process date
	processDate, err := time.Parse("2006-01-02", config.ProcessDate)
	if err != nil {
		return fmt.Errorf("invalid date format: %v", err)
	}

	// Process orderbook files
	if err := processOrderBookFiles(ctx, ob, config, processDate); err != nil {
		fmt.Println("failed to process orderbook files: ", err)
	}

	// Process profit/loss file
	if err := plService.ProcessDailyProfitLoss(ctx, processDate); err != nil {
		fmt.Println("failed to process profit/loss file: ", err)
	}

	return nil
}

func processOrderBookFiles(ctx context.Context, ob *orderbook.OrderBook, config Config, processDate time.Time) error {
	// Find CSV files for the specified date
	pattern := fmt.Sprintf("orderbook_*%s*.csv", processDate.Format("02-01-2006"))
	matches, err := filepath.Glob(filepath.Join(config.CSVDir, pattern))
	if err != nil {
		return fmt.Errorf("failed to find CSV files: %v", err)
	}

	if len(matches) == 0 {
		return fmt.Errorf("no CSV files found for date %s", config.ProcessDate)
	}

	// Process each file
	var wg sync.WaitGroup
	errorChan := make(chan error, len(matches))

	for _, file := range matches {
		// Skip profit/loss files
		if filepath.Base(file)[:10] == "profitLoss" {
			continue
		}

		wg.Add(1)
		go func(filename string) {
			defer wg.Done()

			log.Printf("Processing orderbook file: %s", filename)
			if err := ob.LoadCSVFile(ctx, filename); err != nil {
				errorChan <- fmt.Errorf("failed to process %s: %v", filename, err)
				return
			}
			log.Printf("Completed processing: %s", filename)
		}(file)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errorChan)

	// Check for any errors
	for err := range errorChan {
		if err != nil {
			return err
		}
	}

	return nil
}

func displaySummary(ctx context.Context, ob *orderbook.OrderBook, config Config) error {
	processDate, err := time.Parse("2006-01-02", config.ProcessDate)
	if err != nil {
		return fmt.Errorf("invalid date format: %v", err)
	}

	summary, err := ob.GetDailySummary(ctx, processDate)
	if err != nil {
		return fmt.Errorf("failed to get daily summary: %v", err)
	}

	// Display summary in a formatted table
	fmt.Println("\nDaily Summary Report")
	fmt.Println("===================")
	fmt.Printf("Date: %s\n", summary.Date.Format("02-Jan-2006"))
	fmt.Printf("Total Trades: %d\n", summary.TotalTrades)
	fmt.Printf("Total Buy Quantity: %d\n", summary.TotalBuyQuantity)
	fmt.Printf("Total Sell Quantity: %d\n", summary.TotalSellQuantity)
	fmt.Printf("Unique Symbols: %d\n", summary.UniqueSymbols)
	fmt.Printf("Last Updated: %s\n", summary.LastUpdated.Format("15:04:05"))

	return nil
}

func init() {
	// Load .env file
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error loading .env file", zap.Error(err))
		return
	}
}
