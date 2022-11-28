package main

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go-hana/internal/hana"
	"go-hana/internal/mongodb"
	"go-hana/internal/schedulers"
	"go.uber.org/zap"
	"log"
	"net/http"
	"os"
)

var (
	successProcessedOffersTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "success_processed_offers_total",
		Help: "The total number of successfully processed offers",
	})

	failedProcessedOffersTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "failed_processed_offers_total",
		Help: "The total number of failed processed offers",
	})

	successProcessedProductsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "success_processed_products_total",
		Help: "The total number of successfully processed products",
	})

	failedProcessedProductsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "failed_processed_products_total",
		Help: "The total number of failed processed products",
	})

	successProcessedShopsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "success_processed_shops_total",
		Help: "The total number of successfully processed shops",
	})

	failedProcessedShopsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "failed_processed_shops_total",
		Help: "The total number of failed processed shops",
	})

	successProcessedShopReviewsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "success_processed_shop_reviews_total",
		Help: "The total number of successfully processed shop reviews",
	})

	failedProcessedShopReviewsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "failed_processed_shop_reviews_total",
		Help: "The total number of failed processed shop reviews",
	})
)

func main() {
	lg, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("can't initialize zap logger: %v", err)
	}
	defer lg.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mongoDB, err := mongodb.NewMongoDB(ctx, mongodb.Config{
		URI: os.Getenv("MONGO_URI"),
	})
	if err != nil {
		lg.Fatal("error while connecting to MongoDB", zap.Error(err))
		return
	}
	lg.Info("connected to MongoDB")

	hanaUri := fmt.Sprintf("hdb://%s:%s@%s:%s?TLSServerName=%s&TLSRootCAFile=DigiCertGlobalRootCA.crt.pem",
		os.Getenv("HANA_USER"),
		os.Getenv("HANA_PASSWORD"),
		os.Getenv("HANA_HOST"),
		os.Getenv("HANA_PORT"),
		os.Getenv("HANA_HOST"),
	)

	hanaDB, err := hana.NewHanaDB(hanaUri)
	if err != nil {
		lg.Fatal("error while connecting to HANA", zap.Error(err))
		return
	}
	lg.Info("connected to HANA")

	// run if needed
	//if err = hana.DropTables(hanaDB); err != nil {
	//	lg.Fatal("error while dropping tables", zap.Error(err))
	//	return
	//}
	//lg.Info("dropped tables")
	//if err = hana.CreateTables(hanaDB); err != nil {
	//	lg.Fatal("error while creating tables", zap.Error(err))
	//	return
	//}
	//lg.Info("created tables")

	// metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err = http.ListenAndServe(":9090", nil); err != nil {
			lg.Fatal("error while starting metrics server", zap.Error(err))
			return
		}
	}()

	// ETL from MongoDB to HANA
	go func() {
		if err = schedulers.NewOfferScheduler(ctx, mongoDB, hanaDB); err != nil {
			lg.Fatal("error while starting offer scheduler", zap.Error(err))
			return
		}
	}()
	go func() {
		if err = schedulers.NewProductScheduler(ctx, mongoDB, hanaDB); err != nil {
			lg.Fatal("error while starting product scheduler", zap.Error(err))
			return
		}
	}()
	go func() {
		if err = schedulers.NewShopScheduler(ctx, mongoDB, hanaDB); err != nil {
			lg.Fatal("error while starting shop scheduler", zap.Error(err))
			return
		}
	}()
	go func() {
		if err = schedulers.NewShopReviewScheduler(ctx, mongoDB, hanaDB); err != nil {
			lg.Fatal("error while starting shop review scheduler", zap.Error(err))
			return
		}
	}()

	<-ctx.Done()
	if err = mongoDB.Disconnect(ctx); err != nil {
		lg.Fatal("error while disconnecting from MongoDB", zap.Error(err))
	}
	if err = hanaDB.Close(); err != nil {
		lg.Fatal("error while disconnecting from HANA", zap.Error(err))
	}
	lg.Info("main finished")
}
