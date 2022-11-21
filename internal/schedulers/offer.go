package schedulers

import (
	"context"
	"database/sql"
	"go-hana/internal/hana"
	"go-hana/internal/mongodb"
	"log"
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
				errChannel <- err
				return
			}

			for _, offer := range offers {
				// start transaction
				tx, err := hanaDB.Begin()
				if err != nil {
					errChannel <- err
					return
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
				if err := row.Scan(&o); err != nil {
					if err != sql.ErrNoRows {
						errChannel <- err
						return
					}

					// insert
					_, err = tx.Exec("INSERT INTO OFFERS (ID, PRODUCT_ID, CATEGORY, SHOP_ID, AVAILABILITY_DATE, DELIVERY, "+
						"DELIVERY_DURATION, KASPI_DELIVERY, KD_DESTINATION_CITY, KD_PICKUP_DATE, LOCATED_IN_POINT, SHOP_RATING, "+
						"SHOP_REVIEWS_QUANTITY, PREORDER, PRICE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
						id, productId, category, shopId, availabilityDate, delivery, deliveryDuration, kaspiDelivery,
						kdDestinationCity, kdPickupDate, locatedInPoint, shopRating, shopReviewsQuantity, preorder, price)
					if err != nil {
						errChannel <- err
						return
					}
				} else {
					// update
					_, err = tx.Exec("UPDATE OFFERS SET PRODUCT_ID = ?, CATEGORY = ?, SHOP_ID = ?, AVAILABILITY_DATE = ?, "+
						"DELIVERY = ?, DELIVERY_DURATION = ?, KASPI_DELIVERY = ?, KD_DESTINATION_CITY = ?, KD_PICKUP_DATE = ?, "+
						"LOCATED_IN_POINT = ?, SHOP_RATING = ?, SHOP_REVIEWS_QUANTITY = ?, PREORDER = ?, PRICE = ? WHERE ID = ?",
						productId, category, shopId, availabilityDate, delivery, deliveryDuration, kaspiDelivery, kdDestinationCity,
						kdPickupDate, locatedInPoint, shopRating, shopReviewsQuantity, preorder, price, id)
					if err != nil {
						errChannel <- err
						return
					}
				}

				// commit transaction
				if err = tx.Commit(); err != nil {
					errChannel <- err
					return
				}
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
