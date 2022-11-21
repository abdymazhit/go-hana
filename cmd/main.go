package main

import (
	"context"
	"fmt"
	"go-hana/internal/hana"
	"go-hana/internal/mongodb"
	"go-hana/internal/schedulers"
	"log"
	"os"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mongoDB, err := mongodb.NewMongoDB(ctx, mongodb.Config{
		URI: os.Getenv("MONGO_URI"),
	})
	if err != nil {
		log.Fatalf("failed to connect to MongoDB: %v", err)
		return
	}
	log.Println("Successfully connected to Mongo database")

	hanaUri := fmt.Sprintf("hdb://%s:%s@%s:%s?TLSServerName=%s&TLSRootCAFile=DigiCertGlobalRootCA.crt.pem",
		os.Getenv("HANA_USER"),
		os.Getenv("HANA_PASSWORD"),
		os.Getenv("HANA_HOST"),
		os.Getenv("HANA_PORT"),
		os.Getenv("HANA_HOST"),
	)

	hanaDB, err := hana.NewHanaDB(hanaUri)
	if err != nil {
		log.Fatalf("failed to connect to HANA database: %v", err)
		return
	}
	log.Println("Successfully connected to HANA database")

	// run if needed
	//if err := hana.DropTables(hanaDB); err != nil {
	//	log.Fatalf("failed to drop tables: %v", err)
	//	return
	//}
	//log.Println("Successfully dropped tables")
	//if err := hana.CreateTables(hanaDB); err != nil {
	//	log.Fatalf("failed to create tables: %v", err)
	//	return
	//}
	//log.Println("Successfully created tables")

	// ETL from MongoDB to HANA
	go func() {
		if err := schedulers.NewOfferScheduler(ctx, mongoDB, hanaDB); err != nil {
			log.Fatalf("error while running offer scheduler: %v", err)
			return
		}
	}()
	go func() {
		if err := schedulers.NewProductScheduler(ctx, mongoDB, hanaDB); err != nil {
			log.Fatalf("error while running product scheduler: %v", err)
			return
		}
	}()
	go func() {
		if err := schedulers.NewShopScheduler(ctx, mongoDB, hanaDB); err != nil {
			log.Fatalf("error while running shop scheduler: %v", err)
			return
		}
	}()
	go func() {
		if err := schedulers.NewShopReviewScheduler(ctx, mongoDB, hanaDB); err != nil {
			log.Fatalf("error while running shop review scheduler: %v", err)
			return
		}
	}()

	<-ctx.Done()
	if err = mongoDB.Disconnect(ctx); err != nil {
		log.Printf("Error disconnecting from MongoDB: %v\n", err)
	}
	if err = hanaDB.Close(); err != nil {
		log.Printf("Error disconnecting from HANA: %v\n", err)
	}
	log.Println("main: context is done")
}
