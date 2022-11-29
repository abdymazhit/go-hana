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
	successProcessedShopReviewsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "success_processed_shop_reviews_total",
		Help: "The total number of successfully processed shop reviews",
	})

	failedProcessedShopReviewsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "failed_processed_shop_reviews_total",
		Help: "The total number of failed processed shop reviews",
	})
)

func NewShopReviewScheduler(ctx context.Context, mongoDB *mongodb.DB, hanaDB *hana.DB) error {
	log.Printf("starting shop review scheduler")

	// 1. Get all shop reviews count from MongoDB
	// 1. Get all shop reviews by 1000 (with skip and limit)
	// 2. Insert into HANA
	// 3. Repeat until all shop reviews are inserted
	// 4. When all shop reviews are inserted, restart the scheduler
	var errChannel = make(chan error)
	var doneChannel = make(chan bool)

	go func() {
		count, err := mongoDB.GetCount(ctx, mongodb.MAIN_DATABASE, mongodb.SHOP_REVIEWS_COLLECTION)
		if err != nil {
			errChannel <- err
			return
		}

		for i := int64(0); i < count; i += 1000 {
			shopReviews, err := mongoDB.GetAll(ctx, mongodb.MAIN_DATABASE, mongodb.SHOP_REVIEWS_COLLECTION, i, 1000)
			if err != nil {
				log.Printf("error getting shop reviews: %v\n", err)
				failedProcessedShopReviewsTotal.Add(1000)
				continue
			}

			for _, shopReview := range shopReviews {
				// start transaction
				tx, err := hanaDB.Begin()
				if err != nil {
					log.Printf("error while starting transaction: %v\n", err)
					failedProcessedShopReviewsTotal.Add(1)
					continue
				}

				// get shop review fields
				id := shopReview["_id"]
				shopId := shopReview["merchant_id"]
				rating := shopReview["rating"]
				author := shopReview["author"]
				comment := shopReview["comment"]
				date := shopReview["date"]

				commentMap, ok := comment.(map[string]interface{})
				if !ok {
					log.Printf("error while converting comment to map: %v\n", err)
					failedProcessedShopReviewsTotal.Add(1)
					continue
				}
				text, ok := commentMap["text"]
				if !ok {
					log.Printf("error while getting comment text: %v\n", err)
					failedProcessedShopReviewsTotal.Add(1)
					continue
				}

				// find by id, is not exists then insert, else update
				row := tx.QueryRow("SELECT ID FROM SHOP_REVIEWS WHERE ID = ?", id)
				var s interface{}
				if err = row.Scan(&s); err != nil {
					if err != sql.ErrNoRows {
						log.Printf("error while scanning: %v\n", err)
						failedProcessedShopReviewsTotal.Add(1)
						continue
					}

					// insert
					if _, err = tx.Exec("INSERT INTO SHOP_REVIEWS (ID, SHOP_ID, RATING, AUTHOR, COMMENT, DATE) VALUES (?, ?, ?, ?, ?, ?)",
						id, shopId, rating, author, text, date); err != nil {
						log.Printf("error while inserting shop review: %v\n", err)
						failedProcessedShopReviewsTotal.Add(1)
						continue
					}
				} else {
					// update
					if _, err = tx.Exec("UPDATE SHOP_REVIEWS SET SHOP_ID = ?, RATING = ?, AUTHOR = ?, COMMENT = ?, DATE = ? WHERE ID = ?",
						shopId, rating, author, text, date, id); err != nil {
						log.Printf("error while updating shop review: %v\n", err)
						failedProcessedShopReviewsTotal.Add(1)
						continue
					}
				}

				// commit transaction
				if err = tx.Commit(); err != nil {
					log.Printf("error while commiting transaction: %v\n", err)
					failedProcessedShopReviewsTotal.Add(1)
					continue
				}

				successProcessedShopReviewsTotal.Add(1)
			}
		}

		doneChannel <- true
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChannel:
		log.Printf("error in shop review scheduler: %v\n", err)
		return NewShopReviewScheduler(ctx, mongoDB, hanaDB)
	case <-doneChannel:
		log.Printf("shop review scheduler is done")
		return NewShopReviewScheduler(ctx, mongoDB, hanaDB)
	}
}
