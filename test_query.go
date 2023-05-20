package end2end

import (
	"context"
	"fmt"
	"github.com/dal-go/dalgo-end2end-tests/models"
	"github.com/dal-go/dalgo/dal"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sort"
	"testing"
	"time"
)

func selectAllCities(ctx context.Context, db dal.Database) (records []dal.Record, err error) {
	q := dal.From(models.CitiesCollection).SelectInto(func() dal.Record {
		return dal.NewRecordWithIncompleteKey(models.CitiesCollection, reflect.String, &models.City{})
	})
	return db.QueryAllRecords(ctx, q)
}

func testQueryOperations(ctx context.Context, t *testing.T, db dal.Database, eventuallyConsistent bool) {
	defer func() { // Cleanup after test
		if err := deleteAllCities(ctx, db); err != nil {
			t.Fatalf("unexpected error while deleting test data: %v", err)
		}
	}()
	if err := setupDataForQueryTests(ctx, db); err != nil {
		t.Fatalf("unexpected error while setting up test data: %v", err)
	}

	if eventuallyConsistent { // This is to work around eventual consistency
		time.Sleep(1 * time.Second)
		if _, err := selectAllCities(ctx, db); err != nil {
			t.Fatalf("unexpected error while loading all cities: %v", err)
		}
	}

	var newCityRecord = func() dal.Record {
		return dal.NewRecordWithIncompleteKey(models.CitiesCollection, reflect.String, &models.City{})
	}
	t.Run(`SELECT ID FROM Cities`, func(t *testing.T) {
		qb := dal.From(models.CitiesCollection)
		t.Run("no_limit", func(t *testing.T) {
			q := qb.SelectKeysOnly(reflect.String)
			if q == nil {
				t.Fatalf("query is nil")
			}
			reader, err := db.QueryReader(ctx, q)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if reader == nil {
				t.Fatalf("reader is nil")
			}
			var ids []string
			if ids, err = dal.SelectAllIDs[string](reader, q.Limit()); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			expectedIDs := models.SortedCityIDs
			assert.Equal(t, expectedIDs, ids)
		})
		t.Run("limit=3", func(t *testing.T) {
			q := qb.Limit(3).SelectKeysOnly(reflect.String)
			reader, err := db.QueryReader(ctx, q)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var ids []string
			if ids, err = dal.SelectAllIDs[string](reader, q.Limit()); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assert.Equal(t, q.Limit(), len(ids))
			expectedIDs := models.SortedCityIDs[:q.Limit()]
			sort.Strings(ids)
			assert.Equal(t, expectedIDs, ids)
		})
	})
	t.Run(`SELECT * FROM Cities`, func(t *testing.T) {
		qb := dal.From(models.CitiesCollection)
		t.Run("no_limit", func(t *testing.T) {
			query2 := qb.SelectInto(newCityRecord)
			records, err := db.QueryAllRecords(ctx, query2)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assert.Equal(t, len(models.Cities), len(records))
		})
		t.Run("limit=3", func(t *testing.T) {
			q := qb.Limit(3).SelectInto(newCityRecord)
			records, err := db.QueryAllRecords(ctx, q)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assert.Equal(t, q.Limit(), len(records))
		})
	})
	t.Run("SELECT ID FROM Cities ORDER BY Population", func(t *testing.T) {
		qb := dal.From(models.CitiesCollection)
		t.Run("ascending", func(t *testing.T) {
			q := qb.
				OrderBy(dal.AscendingField("Population")).
				Limit(3).
				SelectKeysOnly(reflect.String)
			reader, err := db.QueryReader(ctx, q)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var ids []string
			if ids, err = dal.SelectAllIDs[string](reader, q.Limit()); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			expectedIDs := []string{
				dal.EscapeID("Istanbul_Istanbul"),
				dal.EscapeID("Sindh_Karachi"),
				dal.EscapeID("Dhaka_Dhaka"),
			}
			assert.Equal(t, expectedIDs, ids)
		})
		t.Run("descending", func(t *testing.T) {
			q := qb.
				OrderBy(dal.DescendingField("Population")).
				Limit(3).
				SelectKeysOnly(reflect.String)
			reader, err := db.QueryReader(ctx, q)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var ids []string
			if ids, err = dal.SelectAllIDs[string](reader, q.Limit()); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			expectedIDs := []string{
				dal.EscapeID("Tokyo_Tokyo"),
				dal.EscapeID("Delhi_Delhi"),
				dal.EscapeID("Shanghai_Shanghai"),
			}
			assert.Equal(t, expectedIDs, ids)
		})
	})
	t.Run("SELECT ID FROM Cities WHERE Country = 'IN'", func(t *testing.T) {
		qb := dal.From(models.CitiesCollection)
		t.Run("no_limit", func(t *testing.T) {
			q := qb.WhereField("Country", dal.Equal, "IN").SelectKeysOnly(reflect.String)
			reader, err := db.QueryReader(ctx, q)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var ids []string
			if ids, err = dal.SelectAllIDs[string](reader, q.Limit()); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			sort.Strings(ids)
			expectedIDs := []string{
				dal.EscapeID("Delhi_Delhi"),
				dal.EscapeID("Maharashtra_Mumbai"),
			}
			assert.Equal(t, expectedIDs, ids)
		})
	})
}

func deleteAllCities(ctx context.Context, db dal.Database) (err error) {
	err = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		q := dal.From(models.CitiesCollection).Limit(1000).SelectKeysOnly(reflect.String)
		var reader dal.Reader
		var err error
		if reader, err = db.QueryReader(ctx, q); err != nil {
			return fmt.Errorf("failed to query all cities: %w", err)
		}
		var ids []string
		if ids, err = dal.SelectAllIDs[string](reader, q.Limit()); err != nil {
			return fmt.Errorf("failed to query all cities: %w", err)
		}
		keys := make([]*dal.Key, len(ids))
		for i, id := range ids {
			keys[i] = dal.NewKeyWithID(models.CitiesCollection, id)
		}
		if len(ids) == 0 {
			return nil
		}
		return tx.DeleteMulti(ctx, keys)
	})
	if err != nil {
		return fmt.Errorf("failed to delete all cities: %w", err)
	}
	return nil
}

func setupDataForQueryTests(ctx context.Context, db dal.Database) (err error) {
	if err := deleteAllCities(ctx, db); err != nil {
		return err
	}
	return db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		records := make([]dal.Record, len(models.Cities))
		for i := range models.Cities { // Do not use value `for _, city` variable as all record will have same pointer to last city
			records[i] = dal.NewRecordWithData(
				dal.NewKeyWithID(models.CitiesCollection, models.CityID(models.Cities[i])),
				&models.Cities[i],
			)
		}
		return tx.SetMulti(ctx, records)
	})
}
