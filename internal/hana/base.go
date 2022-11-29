package hana

import (
	"database/sql"
	"fmt"
	_ "github.com/SAP/go-hdb/driver"
)

const (
	driverName = "hdb"
)

type DB struct {
	*sql.DB
}

func NewHanaDB(uri string) (*DB, error) {
	db, err := sql.Open(driverName, uri)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

func CreateTables(db *DB) error {
	if err := CreateProductsTable(db); err != nil {
		return fmt.Errorf("failed to create products table: %v", err)
	}
	if err := CreateProductCategoriesTable(db); err != nil {
		return fmt.Errorf("failed to create product categories table: %v", err)
	}
	if err := CreateProductCategoryCodesTable(db); err != nil {
		return fmt.Errorf("failed to create product category codes table: %v", err)
	}
	if err := CreateProductMonthlyInstallmentsTable(db); err != nil {
		return fmt.Errorf("failed to create product monthly installments table: %v", err)
	}
	if err := CreateProductPromosTable(db); err != nil {
		return fmt.Errorf("failed to create product promos table: %v", err)
	}
	if err := CreateOffersTable(db); err != nil {
		return fmt.Errorf("failed to create offers table: %v", err)
	}
	if err := CreateShopsTable(db); err != nil {
		return fmt.Errorf("failed to create shops table: %v", err)
	}
	if err := CreateShopReviewsTable(db); err != nil {
		return fmt.Errorf("failed to create shop reviews table: %v", err)
	}
	if err := CreateBrandsTable(db); err != nil {
		return fmt.Errorf("failed to create brands table: %v", err)
	}
	if err := CreateCategoriesTable(db); err != nil {
		return fmt.Errorf("failed to create categories table: %v", err)
	}
	if err := CreateCategoryCodesTable(db); err != nil {
		return fmt.Errorf("failed to create category codes table: %v", err)
	}
	return nil
}

func DropTables(db *DB) error {
	if _, err := db.Exec("DROP TABLE PRODUCT_PROMOS"); err != nil {
		return fmt.Errorf("failed to drop product promos table: %v", err)
	}
	if _, err := db.Exec("DROP TABLE PRODUCT_MONTHLY_INSTALLMENTS"); err != nil {
		return fmt.Errorf("failed to drop product monthly installments table: %v", err)
	}
	if _, err := db.Exec("DROP TABLE PRODUCT_CATEGORY_CODES"); err != nil {
		return fmt.Errorf("failed to drop product category codes table: %v", err)
	}
	if _, err := db.Exec("DROP TABLE PRODUCT_CATEGORIES"); err != nil {
		return fmt.Errorf("failed to drop product categories table: %v", err)
	}
	if _, err := db.Exec("DROP TABLE PRODUCTS"); err != nil {
		return fmt.Errorf("failed to drop products table: %v", err)
	}
	if _, err := db.Exec("DROP TABLE OFFERS"); err != nil {
		return fmt.Errorf("failed to drop offers table: %v", err)
	}
	if _, err := db.Exec("DROP TABLE SHOP_REVIEWS"); err != nil {
		return fmt.Errorf("failed to drop shop reviews table: %v", err)
	}
	if _, err := db.Exec("DROP TABLE SHOPS"); err != nil {
		return fmt.Errorf("failed to drop shops table: %v", err)
	}
	if _, err := db.Exec("DROP TABLE BRANDS"); err != nil {
		return fmt.Errorf("failed to drop brands table: %v", err)
	}
	if _, err := db.Exec("DROP TABLE CATEGORY_CODES"); err != nil {
		return fmt.Errorf("failed to drop category codes table: %v", err)
	}
	if _, err := db.Exec("DROP TABLE CATEGORIES"); err != nil {
		return fmt.Errorf("failed to drop categories table: %v", err)
	}
	return nil
}

func CreateProductsTable(db *DB) error {
	_, err := db.Exec("CREATE TABLE PRODUCTS (" +
		"ID VARCHAR(255) NOT NULL PRIMARY KEY, " +
		"ADJUSTED_RATING DOUBLE, " +
		"BRAND_ID INTEGER, " +
		"CATEGORY_ID INTEGER, " +
		"CREATED_TIME VARCHAR(255), " +
		"CREDIT_MONTHLY_PRICE DOUBLE, " +
		"CURRENCY VARCHAR(255), " +
		"DELIVERY_DURATION VARCHAR(255), " +
		"DISCOUNT DOUBLE, " +
		"HAS_VARIANTS BOOLEAN, " +
		"LOAN_AVAILABLE BOOLEAN, " +
		"RATING DOUBLE, " +
		"REVIEWS_LINK VARCHAR(255), " +
		"REVIEWS_QUANTITY INTEGER, " +
		"LINK VARCHAR(255), " +
		"TITLE VARCHAR(255), " +
		"UNIT_PRICE DOUBLE, " +
		"UNIT_SALE_PRICE DOUBLE, " +
		"WEIGHT DOUBLE" +
		")")
	return err
}

func CreateProductCategoriesTable(db *DB) error {
	_, err := db.Exec("CREATE TABLE PRODUCT_CATEGORIES (" +
		"PRODUCT_ID INTEGER NOT NULL, " +
		"CATEGORY_ID INTEGER NOT NULL, " +
		"PRIMARY KEY (PRODUCT_ID, CATEGORY_ID)" +
		")")
	return err
}

func CreateProductCategoryCodesTable(db *DB) error {
	_, err := db.Exec("CREATE TABLE PRODUCT_CATEGORY_CODES (" +
		"PRODUCT_ID INTEGER NOT NULL, " +
		"CATEGORY_CODE_ID INTEGER NOT NULL, " +
		"PRIMARY KEY (PRODUCT_ID, CATEGORY_CODE_ID)" +
		")")
	return err
}

func CreateProductMonthlyInstallmentsTable(db *DB) error {
	_, err := db.Exec("CREATE TABLE PRODUCT_MONTHLY_INSTALLMENTS (" +
		"PRODUCT_ID INTEGER NOT NULL, " +
		"INSTALLMENT_ID INTEGER NOT NULL, " +
		"INSTALLMENT BOOLEAN, " +
		"INSTALLMENT_PER_MONTH VARCHAR(255), " +
		"PRIMARY KEY (PRODUCT_ID, INSTALLMENT_ID)" +
		")")
	return err
}

func CreateProductPromosTable(db *DB) error {
	_, err := db.Exec("CREATE TABLE PRODUCT_PROMOS (" +
		"PRODUCT_ID INTEGER NOT NULL, " +
		"CODE VARCHAR(255), " +
		"COMMENT VARCHAR(255), " +
		"TYPE VARCHAR(255), " +
		"PRIORITY INTEGER" +
		")")
	return err
}

func CreateOffersTable(db *DB) error {
	_, err := db.Exec("CREATE TABLE OFFERS (" +
		"ID VARCHAR(255) NOT NULL PRIMARY KEY, " +
		"PRODUCT_ID INTEGER, " +
		"CATEGORY VARCHAR(255), " +
		"SHOP_ID VARCHAR(255), " +
		"AVAILABILITY_DATE VARCHAR(255), " +
		"DELIVERY VARCHAR(255), " +
		"DELIVERY_DURATION VARCHAR(255), " +
		"KASPI_DELIVERY BOOLEAN, " +
		"KD_DESTINATION_CITY VARCHAR(255), " +
		"KD_PICKUP_DATE VARCHAR(255), " +
		"LOCATED_IN_POINT VARCHAR(255), " +
		"SHOP_RATING DOUBLE, " +
		"SHOP_REVIEWS_QUANTITY INTEGER, " +
		"PREORDER BOOLEAN, " +
		"PRICE DOUBLE" +
		")")
	return err
}

func CreateShopsTable(db *DB) error {
	_, err := db.Exec("CREATE TABLE SHOPS (" +
		"ID VARCHAR(255) NOT NULL PRIMARY KEY, " +
		"NAME VARCHAR(255)" +
		")")
	return err
}

func CreateShopReviewsTable(db *DB) error {
	_, err := db.Exec("CREATE TABLE SHOP_REVIEWS (" +
		"ID VARCHAR(255) NOT NULL PRIMARY KEY, " +
		"SHOP_ID VARCHAR(255) NOT NULL, " +
		"RATING DOUBLE, " +
		"AUTHOR VARCHAR(255), " +
		"COMMENT VARCHAR2(2000), " +
		"DATE VARCHAR(255)" +
		")")
	return err
}

func CreateBrandsTable(db *DB) error {
	_, err := db.Exec("CREATE TABLE BRANDS (" +
		"ID INTEGER NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY, " +
		"NAME VARCHAR(255)" +
		")")
	return err
}

func CreateCategoriesTable(db *DB) error {
	_, err := db.Exec("CREATE TABLE CATEGORIES (" +
		"ID INTEGER NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY, " +
		"NAME VARCHAR(255)" +
		")")
	return err
}

func CreateCategoryCodesTable(db *DB) error {
	_, err := db.Exec("CREATE TABLE CATEGORY_CODES (" +
		"ID INTEGER NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY, " +
		"CODE VARCHAR(255)" +
		")")
	return err
}
