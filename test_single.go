package end2end

import (
	"context"
	"github.com/dal-go/dalgo/dal"
	"testing"
)

func testSingleOperations(ctx context.Context, t *testing.T, db dal.Database) {
	t.Run("single", func(t *testing.T) {
		const id = "r0"
		key := dal.NewKeyWithID(E2ETestKind1, id)
		t.Run("delete1", func(t *testing.T) {
			testSingleDelete(t, db, key)
		})
		t.Run("get", func(t *testing.T) {
			testSingleGet(t, db, key)
		})
		t.Run("create", func(t *testing.T) {
			t.Run("with_predefined_id", func(t *testing.T) {
				testSingleCreateWithPredefinedID(t, db, key)
			})
		})
		t.Run("delete2", func(t *testing.T) {
			testSingleDelete(t, db, key)
		})
	})
}

func testSingleDelete(t *testing.T, db dal.Database, key *dal.Key) {
	ctx := context.Background()
	err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Delete(ctx, key)
	})
	if err != nil {
		t.Errorf("Failed to delete: %v", err)
	}

}

func testSingleGet(t *testing.T, db dal.Database, key *dal.Key) {
	data := TestData{
		StringProp:  "str1",
		IntegerProp: 1,
	}
	ctx := context.Background()
	record := dal.NewRecordWithData(key, &data)
	if err := db.Get(ctx, record); err != nil {
		if !dal.IsNotFound(err) {
			t.Errorf("unexpected error: %v", err)
		}
	}

}

func testSingleCreateWithPredefinedID(t *testing.T, db dal.Database, key *dal.Key) {
	data := TestData{
		StringProp:  "str1",
		IntegerProp: 1,
	}
	record := dal.NewRecordWithData(key, &data)
	ctx := context.Background()
	err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Insert(ctx, record)
	})
	if err != nil {
		t.Errorf("got unexpected error: %v", err)
	}
}
