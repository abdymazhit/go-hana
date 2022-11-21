package schedulers

import (
	"context"
	"database/sql"
	"go-hana/internal/hana"
	"go-hana/internal/mongodb"
	"log"
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
				errChannel <- err
				return
			}

			for _, shop := range shops {
				// start transaction
				tx, err := hanaDB.Begin()
				if err != nil {
					errChannel <- err
					return
				}

				// get shop fields
				id := shop["_id"]
				name := shop["name"]

				// find by id, is not exists then insert, else update
				row := tx.QueryRow("SELECT ID FROM SHOPS WHERE ID = ?", id)
				var s interface{}
				if err := row.Scan(&s); err != nil {
					if err != sql.ErrNoRows {
						errChannel <- err
						return
					}

					// insert
					_, err = tx.Exec("INSERT INTO SHOPS (ID, NAME) VALUES (?, ?)", id, name)
					if err != nil {
						errChannel <- err
						return
					}
				} else {
					// update
					_, err = tx.Exec("UPDATE SHOPS SET NAME = ? WHERE ID = ?", name, id)
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
		log.Printf("error in shop scheduler: %v\n", err)
		return NewShopScheduler(ctx, mongoDB, hanaDB)
	case <-doneChannel:
		log.Printf("shop scheduler is done")
		return NewShopScheduler(ctx, mongoDB, hanaDB)
	}
}
