package mongodb

import (
	"context"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	// main database
	MAIN_DATABASE           = "main"
	PRODUCTS_COLLECTION     = "products"
	OFFERS_COLLECTION       = "offers"
	SHOPS_COLLECTION        = "shops"
	SHOP_REVIEWS_COLLECTION = "shop_reviews"
)

var (
	ErrDatabaseNotFound   = errors.New("database not found")
	ErrCollectionNotFound = errors.New("collection not found")
)

type Config struct {
	URI string
}

type DB struct {
	*mongo.Client
}

func NewMongoDB(ctx context.Context, cfg Config) (*DB, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.URI))
	if err != nil {
		return nil, err
	}
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}
	return &DB{client}, nil
}

func (c DB) GetAll(ctx context.Context, databaseName, collectionName string, skip, limit int64) ([]map[string]interface{}, error) {
	if databaseName != MAIN_DATABASE {
		return nil, ErrDatabaseNotFound
	}
	if collectionName != PRODUCTS_COLLECTION && collectionName != OFFERS_COLLECTION &&
		collectionName != SHOPS_COLLECTION && collectionName != SHOP_REVIEWS_COLLECTION {
		return nil, ErrCollectionNotFound
	}

	findOptions := options.FindOptions{}
	if skip > 0 {
		findOptions.SetSkip(skip)
	}
	if limit > 0 {
		findOptions.SetLimit(limit)
	}

	cur, err := c.Database(databaseName).Collection(collectionName).Find(ctx, bson.M{}, &findOptions)
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	if err = cur.All(ctx, &results); err != nil {
		return nil, err
	}
	return results, nil
}

func (c DB) GetCount(ctx context.Context, databaseName, collectionName string) (int64, error) {
	if databaseName != MAIN_DATABASE {
		return 0, ErrDatabaseNotFound
	}
	if collectionName != PRODUCTS_COLLECTION && collectionName != OFFERS_COLLECTION &&
		collectionName != SHOPS_COLLECTION && collectionName != SHOP_REVIEWS_COLLECTION {
		return 0, ErrCollectionNotFound
	}
	return c.Database(databaseName).Collection(collectionName).CountDocuments(ctx, bson.M{})
}
