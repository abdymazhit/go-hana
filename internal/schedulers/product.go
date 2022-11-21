package schedulers

import (
	"context"
	"database/sql"
	"go-hana/internal/hana"
	"go-hana/internal/mongodb"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"log"
	"strconv"
)

func NewProductScheduler(ctx context.Context, mongoDB *mongodb.DB, hanaDB *hana.DB) error {
	log.Printf("starting product scheduler")

	// 1. Get all products count from MongoDB
	// 1. Get all products by 1000 (with skip and limit)
	// 2. Insert into HANA
	// 3. Repeat until all products are inserted
	// 4. When all products are inserted, restart the scheduler
	var errChannel = make(chan error)
	var doneChannel = make(chan bool)

	go func() {
		count, err := mongoDB.GetCount(ctx, mongodb.MAIN_DATABASE, mongodb.PRODUCTS_COLLECTION)
		if err != nil {
			errChannel <- err
			return
		}

		for i := int64(0); i < count; i += 1000 {
			products, err := mongoDB.GetAll(ctx, mongodb.MAIN_DATABASE, mongodb.PRODUCTS_COLLECTION, i, 1000)
			if err != nil {
				log.Printf("error getting products: %v\n", err)
				continue
			}

			for _, product := range products {
				// start transaction
				tx, err := hanaDB.Begin()
				if err != nil {
					log.Printf("error while starting transaction: %v\n", err)
					continue
				}

				// get product fields
				id := product["_id"]
				adjustedRating := product["adjustedRating"]
				brand := product["brand"]
				category := product["category"]
				categoryCodes := product["categoryCodes"]
				catId := product["categoryId"]
				createdTime := product["createdTime"]
				creditMonthlyPrice := product["creditMonthlyPrice"]
				currency := product["currency"]
				deliveryDuration := product["deliveryDuration"]
				discount := product["discount"]
				hasVariants := product["hasVariants"]
				loanAvailable := product["loanAvailable"]
				monthlyInstallment := product["monthlyInstallment"]
				promo := product["promo"]
				rating := product["rating"]
				reviewsLink := product["reviewsLink"]
				reviewsQuantity := product["reviewsQuantity"]
				link := product["shopLink"]
				title := product["title"]
				unitPrice := product["unitPrice"]
				unitSalePrice := product["unitSalePrice"]
				weight := product["weight"]

				categId, ok := catId.(string)
				if !ok {
					log.Printf("error while converting categoryId to string: %v\n", err)
					continue
				}
				categoryId, err := strconv.ParseInt(categId, 10, 64)
				if err != nil {
					log.Printf("error while converting categoryId to int: %v\n", err)
					continue
				}

				// find brand id in HANA, if not found, insert into HANA
				var brandId int64
				if err = tx.QueryRow("SELECT ID FROM BRANDS WHERE NAME = ?", brand).Scan(&brandId); err != nil {
					if err != sql.ErrNoRows {
						log.Printf("error while getting brand id: %v\n", err)
						continue
					}

					if _, err = tx.Exec("INSERT INTO BRANDS (NAME) VALUES (?)", brand); err != nil {
						log.Printf("error while inserting brand: %v\n", err)
						continue
					}
					if err = tx.QueryRow("SELECT ID FROM BRANDS WHERE NAME = ?", brand).Scan(&brandId); err != nil {
						log.Printf("error while getting brand id: %v\n", err)
						continue
					}
				}

				// find categories id in HANA, if not found, insert into HANA
				categories, ok := category.(primitive.A)
				if !ok {
					log.Printf("error while converting category to array: %v\n", err)
					continue
				}
				for _, c := range categories {
					categoryName, ok := c.(string)
					if !ok {
						log.Printf("error while converting category to string: %v\n", err)
						continue
					}

					var cId int64
					if err = tx.QueryRow("SELECT ID FROM CATEGORIES WHERE NAME = ?", categoryName).Scan(&cId); err != nil {
						if err != sql.ErrNoRows {
							log.Printf("error while getting category id: %v\n", err)
							continue
						}

						if _, err = tx.Exec("INSERT INTO CATEGORIES (NAME) VALUES (?)", categoryName); err != nil {
							log.Printf("error while inserting category: %v\n", err)
							continue
						}
						if err = tx.QueryRow("SELECT ID FROM CATEGORIES WHERE NAME = ?", categoryName).Scan(&cId); err != nil {
							log.Printf("error while getting category id: %v\n", err)
							continue
						}
					}

					if _, err = tx.Exec("INSERT INTO PRODUCT_CATEGORIES (PRODUCT_ID, CATEGORY_ID) VALUES (?, ?)", id, cId); err != nil {
						//@TODO: don't log
						//errChannel <- err
						//return
					}
				}

				// find category codes id in HANA, if not found, insert into HANA
				catCodes, ok := categoryCodes.(primitive.A)
				if !ok {
					log.Printf("error while converting categoryCodes to array: %v\n", err)
					continue
				}
				for _, catCode := range catCodes {
					categoryCode, ok := catCode.(string)
					if !ok {
						log.Printf("error while converting categoryCodes to string: %v\n", err)
						continue
					}

					var categoryCodeId int64
					if err = tx.QueryRow("SELECT ID FROM CATEGORY_CODES WHERE CODE = ?", categoryCode).Scan(&categoryCodeId); err != nil {
						if err != sql.ErrNoRows {
							log.Printf("error while getting category code id: %v\n", err)
							continue
						}

						if _, err = tx.Exec("INSERT INTO CATEGORY_CODES (CODE) VALUES (?)", categoryCode); err != nil {
							log.Printf("error while inserting category code: %v\n", err)
							continue
						}
						if err = tx.QueryRow("SELECT ID FROM CATEGORY_CODES WHERE CODE = ?", categoryCode).Scan(&categoryCodeId); err != nil {
							log.Printf("error while getting category code id: %v\n", err)
							continue
						}
					}

					if _, err = tx.Exec("INSERT INTO PRODUCT_CATEGORY_CODES (PRODUCT_ID, CATEGORY_CODE_ID) VALUES (?, ?)", id, categoryCodeId); err != nil {
						//@TODO: don't log
						//errChannel <- err
						//return
					}
				}

				// insert into products
				_, err = tx.Exec("INSERT INTO PRODUCTS (ID, ADJUSTED_RATING, BRAND_ID, CATEGORY_ID, CREATED_TIME, "+
					"CREDIT_MONTHLY_PRICE, CURRENCY, DELIVERY_DURATION, DISCOUNT, HAS_VARIANTS, LOAN_AVAILABLE, RATING, "+
					"REVIEWS_LINK, REVIEWS_QUANTITY, LINK, TITLE, UNIT_PRICE, UNIT_SALE_PRICE, WEIGHT) VALUES "+
					"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
					id, adjustedRating, brandId, categoryId, createdTime, creditMonthlyPrice, currency, deliveryDuration,
					discount, hasVariants, loanAvailable, rating, reviewsLink, reviewsQuantity, link, title, unitPrice,
					unitSalePrice, weight)
				if err != nil {
					//errChannel <- err
					//return
				}

				// insert into product monthly installments
				if monthlyInstallment != nil {
					monthlyInstallmentMap, ok := monthlyInstallment.(map[string]interface{})
					if !ok {
						log.Printf("error while converting monthlyInstallment to map: %v\n", err)
						continue
					}

					installmentId, ok := monthlyInstallmentMap["id"].(float64)
					if !ok {
						log.Printf("error while converting monthlyInstallment id to float: %v\n", err)
						continue
					}
					installment, ok := monthlyInstallmentMap["installment"].(bool)
					if !ok {
						log.Printf("error while converting monthlyInstallment installment to bool: %v\n", err)
						continue
					}
					formattedPerMonth, ok := monthlyInstallmentMap["formattedPerMonth"].(string)
					if !ok {
						log.Printf("error while converting monthlyInstallment formattedPerMonth to string: %v\n", err)
						continue
					}

					if _, err = tx.Exec("INSERT INTO PRODUCT_MONTHLY_INSTALLMENTS (PRODUCT_ID, "+
						"INSTALLMENT_ID, INSTALLMENT, INSTALLMENT_PER_MONTH) VALUES (?, ?, ?, ?)", id,
						int64(installmentId), installment, formattedPerMonth); err != nil {
						log.Printf("error while inserting product monthly installment: %v\n", err)
						continue
					}
				}

				// insert into product promo
				if promo != nil {
					promos, ok := promo.(primitive.A)
					if !ok {
						log.Printf("error while converting promo to array: %v\n", err)
						continue
					}

					for _, p := range promos {
						promoMap, ok := p.(map[string]interface{})
						if !ok {
							log.Printf("error while converting promo to map: %v\n", err)
							continue
						}

						priority, ok := promoMap["priority"].(float64)
						if !ok {
							log.Printf("error while converting promo priority to float: %v\n", err)
							continue
						}
						code, ok := promoMap["code"].(string)
						if !ok {
							log.Printf("error while converting promo code to string: %v\n", err)
							continue
						}
						var text *string
						if promoMap["text"] != nil {
							t, ok := promoMap["text"].(string)
							if !ok {
								log.Printf("error while converting promo text to string: %v\n", err)
								continue
							}
							text = &t
						}
						promoType, ok := promoMap["type"].(string)
						if !ok {
							log.Printf("error while converting promo type to string: %v\n", err)
							continue
						}

						if _, err = tx.Exec("INSERT INTO PRODUCT_PROMOS (PRODUCT_ID, CODE, COMMENT, TYPE, PRIORITY) "+
							"VALUES (?, ?, ?, ?, ?)", id, code, text, promoType, int64(priority)); err != nil {
							log.Printf("error inserting product promo: %v\n", err)
							continue
						}
					}
				}

				// commit transaction
				if err = tx.Commit(); err != nil {
					log.Printf("error commiting transaction: %v\n", err)
					continue
				}
			}
		}

		doneChannel <- true
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChannel:
		log.Printf("error in product scheduler: %v\n", err)
		return NewProductScheduler(ctx, mongoDB, hanaDB)
	case <-doneChannel:
		log.Printf("product scheduler is done")
		return NewProductScheduler(ctx, mongoDB, hanaDB)
	}
}
