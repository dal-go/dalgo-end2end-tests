package end2end

import (
	"context"
	"fmt"
	"github.com/dal-go/dalgo-end2end-tests/models"
	"github.com/dal-go/dalgo/dal"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func testQueryOperations(ctx context.Context, t *testing.T, db dal.Database) {
	if err := setupDataForQueryTests(ctx, db); err != nil {
		t.Fatalf("unexpected error while setting up test data: %v", err)
	}
	t.Run(`SELECT ID FROM Cities`, func(t *testing.T) {
		query := dal.From(models.CitiesCollection).SelectKeysOnly(reflect.String)
		t.Run("no_limit", func(t *testing.T) {
			query2 := query
			anyIds, err := db.SelectAllIDs(ctx, query2)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			expectedIDs := make([]any, len(models.Cities))
			for i, city := range models.Cities {
				expectedIDs[i] = cityID(city)
			}
			assert.Equal(t, len(models.Cities), len(anyIds), "expected ids: %+v, got: %+v", expectedIDs, anyIds)
		})
		t.Run("limit=3", func(t *testing.T) {
			query2 := query
			query2.Limit = 3
			anyIds, err := db.SelectAllIDs(ctx, query2)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			expectedIDs := make([]any, len(models.Cities))
			for i, city := range models.Cities {
				expectedIDs[i] = cityID(city)
			}
			assert.Equal(t, 3, len(anyIds))
		})
	})
	t.Run(`SELECT * FROM Cities`, func(t *testing.T) {
		query := dal.From(models.CitiesCollection).SelectInto(func() dal.Record {
			return dal.NewRecordWithIncompleteKey(models.CitiesCollection, reflect.String, &models.City{})
		})
		t.Run("no_limit", func(t *testing.T) {
			query2 := query
			records, err := db.SelectAll(ctx, query2)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			expectedIDs := make([]any, len(models.Cities))
			for i, city := range models.Cities {
				expectedIDs[i] = cityID(city)
			}
			assert.Equal(t, len(models.Cities), len(records))
		})
		t.Run("no_limit", func(t *testing.T) {
			query2 := query
			query2.Limit = 3
			records, err := db.SelectAll(ctx, query2)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			expectedIDs := make([]any, len(models.Cities))
			for i, city := range models.Cities {
				expectedIDs[i] = cityID(city)
			}
			assert.Equal(t, len(models.Cities), len(records))
		})
	})
	return
}

func cityID(city models.City) string {
	return fmt.Sprintf("%s/%s", city.State, city.Name)
}

func setupDataForQueryTests(ctx context.Context, db dal.Database) error {
	return db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		records := make([]dal.Record, len(models.Cities))
		for i, city := range models.Cities {
			records[i] = dal.NewRecordWithData(
				dal.NewKeyWithID(models.CitiesCollection, cityID(city)),
				&city,
			)
		}
		return tx.SetMulti(ctx, records)
	})
}
