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

func selectAllCities(ctx context.Context, db dal.DB) (records []dal.Record, err error) {
	q := dal.From(dal.NewRootCollectionRef(models.CitiesCollection, "")).SelectInto(func() dal.Record {
		return dal.NewRecordWithIncompleteKey(models.CitiesCollection, reflect.String, &models.City{})
	})
	err = db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
		records, err = tx.QueryAllRecords(ctx, q)
		return err
	}, dal.TxWithName("selectAllCities"))
	return
}

func queryOperationsTest(ctx context.Context, t *testing.T, db dal.DB, eventuallyConsistent bool) {
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
		qb := dal.From(dal.NewRootCollectionRef(models.CitiesCollection, ""))
		t.Run("no_limit", func(t *testing.T) {
			q := qb.SelectKeysOnly(reflect.String)
			if q == nil {
				t.Fatalf("query is nil")
			}
			err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
				reader, err := tx.QueryReader(ctx, q)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				//defer func() {
				//	_ = reader.Close()
				//}()
				if reader == nil {
					t.Fatalf("reader is nil")
				}
				var ids []string
				if ids, err = dal.SelectAllIDs[string](reader, dal.WithLimit(q.Limit())); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				expectedIDs := models.SortedCityIDs
				assert.Equal(t, expectedIDs, ids)
				return nil
			}, dal.TxWithName("SELECT ID FROM Cities; limit=0"))
			assert.Nil(t, err)
		})
		t.Run("limit=3", func(t *testing.T) {
			q := qb.Limit(3).SelectKeysOnly(reflect.String)
			err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
				reader, err := tx.QueryReader(ctx, q)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				//defer func() {
				//	_ = reader.Close() - reader is closed by dal.SelectAllIDs
				//}()
				var ids []string
				if ids, err = dal.SelectAllIDs[string](reader, dal.WithLimit(q.Limit())); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				assert.Equal(t, q.Limit(), len(ids))
				expectedIDs := models.SortedCityIDs[:q.Limit()]
				sort.Strings(ids)
				assert.Equal(t, expectedIDs, ids)
				return nil
			}, dal.TxWithName("SELECT ID FROM Cities; limit=3"))
			assert.Nil(t, err)
		})
	})
	t.Run(`SELECT * FROM Cities`, func(t *testing.T) {
		qb := dal.From(dal.NewRootCollectionRef(models.CitiesCollection, ""))
		t.Run("no_limit", func(t *testing.T) {
			query2 := qb.SelectInto(newCityRecord)
			err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
				records, err := tx.QueryAllRecords(ctx, query2)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				assert.Equal(t, len(models.Cities), len(records))
				return nil
			}, dal.TxWithName("SELECT * FROM Cities: no_limit"))
			assert.Nil(t, err)
		})
		t.Run("limit=3", func(t *testing.T) {
			q := qb.Limit(3).SelectInto(newCityRecord)
			err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
				records, err := tx.QueryAllRecords(ctx, q)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				assert.Equal(t, q.Limit(), len(records))
				return nil
			}, dal.TxWithName("SELECT * FROM Cities: limit=3"))
			assert.Nil(t, err)
		})
	})
	t.Run("SELECT ID FROM Cities ORDER BY Population", func(t *testing.T) {
		qb := dal.From(dal.NewRootCollectionRef(models.CitiesCollection, ""))
		t.Run("ascending", func(t *testing.T) {
			q := qb.
				OrderBy(dal.AscendingField("Population")).
				Limit(3).
				SelectKeysOnly(reflect.String)
			err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
				reader, err := tx.QueryReader(ctx, q)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				//defer func() {
				//	_ = reader.Close()
				//}()
				var ids []string
				if ids, err = dal.SelectAllIDs[string](reader, dal.WithLimit(q.Limit())); err != nil {
					return err
				}
				expectedIDs := []string{
					dal.EscapeID("Istanbul_Istanbul"),
					dal.EscapeID("Sindh_Karachi"),
					dal.EscapeID("Dhaka_Dhaka"),
				}
				assert.Equal(t, expectedIDs, ids)
				return nil
			}, dal.TxWithName("SELECT ID FROM Cities ORDER BY Population; limit=3"))
			assert.Nil(t, err)
		})
		t.Run("descending", func(t *testing.T) {
			q := qb.
				OrderBy(dal.DescendingField("Population")).
				Limit(3).
				SelectKeysOnly(reflect.String)
			err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
				reader, err := tx.QueryReader(ctx, q)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				//defer func() {
				//	_ = reader.Close()
				//}()
				var ids []string
				if ids, err = dal.SelectAllIDs[string](reader, dal.WithLimit(q.Limit())); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				expectedIDs := []string{
					dal.EscapeID("Tokyo_Tokyo"),
					dal.EscapeID("Delhi_Delhi"),
					dal.EscapeID("Shanghai_Shanghai"),
				}
				assert.Equal(t, expectedIDs, ids)
				return nil
			}, dal.TxWithName("SELECT ID FROM Cities ORDER BY Population DESCENDING; limit=3"))
			assert.Nil(t, err)

		})
	})
	t.Run("SELECT_ID_FROM_Cities_WHERE_Country_=_'IN'", func(t *testing.T) {
		qb := dal.From(dal.NewRootCollectionRef(models.CitiesCollection, ""))
		t.Run("no_limit", func(t *testing.T) {
			q := qb.WhereField("Country", dal.Equal, "IN").SelectKeysOnly(reflect.String)
			err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
				reader, err := tx.QueryReader(ctx, q)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				//defer func() {
				//	_ = reader.Close()
				//}()
				var ids []string
				if ids, err = dal.SelectAllIDs[string](reader, dal.WithLimit(q.Limit())); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				sort.Strings(ids)
				expectedIDs := []string{
					dal.EscapeID("Delhi_Delhi"),
					dal.EscapeID("Maharashtra_Mumbai"),
				}
				assert.Equal(t, expectedIDs, ids)
				return nil
			}, dal.TxWithName("SELECT ID FROM Cities WHERE Country = 'IN'"))
			assert.Nil(t, err)

		})
	})
}

func deleteAllCities(ctx context.Context, db dal.DB) (err error) {
	err = db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		q := dal.From(dal.NewRootCollectionRef(models.CitiesCollection, "")).Limit(1000).SelectKeysOnly(reflect.String)
		var reader dal.Reader
		if reader, err = tx.QueryReader(ctx, q); err != nil {
			return fmt.Errorf("failed to query all cities: %w", err)
		}
		//defer func() {
		//	_ = reader.Close()
		//}()
		var ids []string
		if ids, err = dal.SelectAllIDs[string](reader, dal.WithLimit(q.Limit())); err != nil {
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
	}, dal.TxWithName("deleteAllCities"))
	if err != nil {
		return fmt.Errorf("failed to delete all cities: %w", err)
	}
	return nil
}

func setupDataForQueryTests(ctx context.Context, db dal.DB) (err error) {
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
	}, dal.TxWithName("setupDataForQueryTests"))
}
