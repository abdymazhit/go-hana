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
	successProcessedShopsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "success_processed_shops_total",
		Help: "The total number of successfully processed shops",
	})

	failedProcessedShopsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "failed_processed_shops_total",
		Help: "The total number of failed processed shops",
	})
)

func NewShopScheduler(ctx context.Context, mongoDB *mongodb.DB, hanaDB *hana.DB) error {
	log.Printf("starting shop scheduler")

	// 1. Get all shops count from MongoDB
	// 1. Get all shops by 1000 (with skip and limit)
	// 2. Insert into HANA
	// 3. Repeat until all shops are inserted
	// 4. When all shops are inserted, restart the scheduler
	var errChannel = make(chan error)
	var doneChannel = make(chan bool)

	go func() {
		count, err := mongoDB.GetCount(ctx, mongodb.MAIN_DATABASE, mongodb.SHOPS_COLLECTION)
		if err != nil {
			errChannel <- err
			return
		}

		for i := int64(0); i < count; i += 1000 {
			shops, err := mongoDB.GetAll(ctx, mongodb.MAIN_DATABASE, mongodb.SHOPS_COLLECTION, i, 1000)
			if err != nil {
				log.Printf("error getting shops: %v\n", err)
				failedProcessedShopsTotal.Add(1000)
				continue
			}

			for _, shop := range shops {
				// start transaction
				tx, err := hanaDB.Begin()
				if err != nil {
					log.Printf("error while starting transaction: %v\n", err)
					failedProcessedShopsTotal.Add(1)
					continue
				}

				// get shop fields
				id := shop["_id"]
				name := shop["name"]

				// find by id, is not exists then insert, else update
				row := tx.QueryRow("SELECT ID FROM SHOPS WHERE ID = ?", id)
				var s interface{}
				if err = row.Scan(&s); err != nil {
					if err != sql.ErrNoRows {
						log.Printf("error while scanning: %v\n", err)
						failedProcessedShopsTotal.Add(1)
						continue
					}

					// insert
					if _, err = tx.Exec("INSERT INTO SHOPS (ID, NAME) VALUES (?, ?)", id, name); err != nil {
						log.Printf("error while inserting: %v\n", err)
						failedProcessedShopsTotal.Add(1)
						continue
					}
				} else {
					// update
					if _, err = tx.Exec("UPDATE SHOPS SET NAME = ? WHERE ID = ?", name, id); err != nil {
						log.Printf("error while updating: %v\n", err)
						failedProcessedShopsTotal.Add(1)
						continue
					}
				}

				// commit transaction
				if err = tx.Commit(); err != nil {
					log.Printf("error while committing transaction: %v\n", err)
					failedProcessedShopsTotal.Add(1)
					continue
				}

				successProcessedShopsTotal.Add(1)
			}
		}

		doneChannel <- true
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChannel:
		log.Printf("error in shop scheduler: %v\n", err)
		return NewShopScheduler(ctx, mongoDB, hanaDB)
	case <-doneChannel:
		log.Printf("shop scheduler is done")
		return NewShopScheduler(ctx, mongoDB, hanaDB)
	}
}
