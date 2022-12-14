package schedulers

import (
	"context"
	"database/sql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go-hana/internal/hana"
	"go-hana/internal/mongodb"
	"log"
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
)

func NewOfferScheduler(ctx context.Context, mongoDB *mongodb.DB, hanaDB *hana.DB) error {
	log.Printf("starting offer scheduler")

	// 1. Get all offers count from MongoDB
	// 1. Get all offers by 1000 (with skip and limit)
	// 2. Insert into HANA
	// 3. Repeat until all offers are inserted
	// 4. When all offers are inserted, restart the scheduler
	var errChannel = make(chan error)
	var doneChannel = make(chan bool)

	go func() {
		count, err := mongoDB.GetCount(ctx, mongodb.MAIN_DATABASE, mongodb.OFFERS_COLLECTION)
		if err != nil {
			errChannel <- err
			return
		}

		for i := int64(0); i < count; i += 1000 {
			offers, err := mongoDB.GetAll(ctx, mongodb.MAIN_DATABASE, mongodb.OFFERS_COLLECTION, i, 1000)
			if err != nil {
				log.Printf("error while getting offers from MongoDB: %v\n", err)
				failedProcessedOffersTotal.Add(1000)
				continue
			}

			for _, offer := range offers {
				// start transaction
				tx, err := hanaDB.Begin()
				if err != nil {
					log.Printf("error while starting transaction: %v\n", err)
					failedProcessedOffersTotal.Add(1)
					continue
				}

				// get offer fields
				id := offer["_id"]
				productId := offer["masterSku"]
				category := offer["masterCategory"]
				shopId := offer["merchantId"]
				availabilityDate := offer["availabilityDate"]
				delivery := offer["delivery"]
				deliveryDuration := offer["deliveryDuration"]
				kaspiDelivery := offer["kaspiDelivery"]
				kdDestinationCity := offer["kdDestinationCity"]
				kdPickupDate := offer["kdPickupDate"]
				locatedInPoint := offer["locatedInPoint"]
				shopRating := offer["merchantRating"]
				shopReviewsQuantity := offer["merchantReviewsQuantity"]
				preorder := offer["preorder"]
				price := offer["price"]

				// find by id, if exists, update, else insert
				row := tx.QueryRow("SELECT ID FROM OFFERS WHERE ID = ?", id)
				var o interface{}
				if err = row.Scan(&o); err != nil {
					if err != sql.ErrNoRows {
						log.Printf("error while scanning offer id: %v\n", err)
						failedProcessedOffersTotal.Add(1)
						continue
					}

					// insert
					_, err = tx.Exec("INSERT INTO OFFERS (ID, PRODUCT_ID, CATEGORY, SHOP_ID, AVAILABILITY_DATE, DELIVERY, "+
						"DELIVERY_DURATION, KASPI_DELIVERY, KD_DESTINATION_CITY, KD_PICKUP_DATE, LOCATED_IN_POINT, SHOP_RATING, "+
						"SHOP_REVIEWS_QUANTITY, PREORDER, PRICE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
						id, productId, category, shopId, availabilityDate, delivery, deliveryDuration, kaspiDelivery,
						kdDestinationCity, kdPickupDate, locatedInPoint, shopRating, shopReviewsQuantity, preorder, price)
					if err != nil {
						log.Printf("error while inserting offer: %v\n", err)
						failedProcessedOffersTotal.Add(1)
						continue
					}
				} else {
					// update
					_, err = tx.Exec("UPDATE OFFERS SET PRODUCT_ID = ?, CATEGORY = ?, SHOP_ID = ?, AVAILABILITY_DATE = ?, "+
						"DELIVERY = ?, DELIVERY_DURATION = ?, KASPI_DELIVERY = ?, KD_DESTINATION_CITY = ?, KD_PICKUP_DATE = ?, "+
						"LOCATED_IN_POINT = ?, SHOP_RATING = ?, SHOP_REVIEWS_QUANTITY = ?, PREORDER = ?, PRICE = ? WHERE ID = ?",
						productId, category, shopId, availabilityDate, delivery, deliveryDuration, kaspiDelivery, kdDestinationCity,
						kdPickupDate, locatedInPoint, shopRating, shopReviewsQuantity, preorder, price, id)
					if err != nil {
						log.Printf("error while updating offer: %v\n", err)
						failedProcessedOffersTotal.Add(1)
						continue
					}
				}

				// commit transaction
				if err = tx.Commit(); err != nil {
					log.Printf("error while committing transaction: %v\n", err)
					failedProcessedOffersTotal.Add(1)
					continue
				}

				successProcessedOffersTotal.Add(1)
			}
		}

		doneChannel <- true
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChannel:
		log.Printf("error in offer scheduler: %v\n", err)
		return NewOfferScheduler(ctx, mongoDB, hanaDB)
	case <-doneChannel:
		log.Printf("offer scheduler is done")
		return NewOfferScheduler(ctx, mongoDB, hanaDB)
	}
}
