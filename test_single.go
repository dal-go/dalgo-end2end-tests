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
		var keepGoing bool = true
		if keepGoing {
			keepGoing = t.Run("delete1", func(t *testing.T) {
				testSingleDelete(t, db, key)
			})
		}
		if keepGoing {
			keepGoing = t.Run("get1", func(t *testing.T) {
				testSingleGet(t, db, key, false)
			})
		}
		if keepGoing {
			keepGoing = t.Run("create", func(t *testing.T) {
				t.Run("with_predefined_id", func(t *testing.T) {
					testSingleCreateWithPredefinedID(t, db, key)
				})
			})
		}
		if keepGoing {
			keepGoing = t.Run("get2", func(t *testing.T) {
				testSingleGet(t, db, key, true)
			})
		}
		if keepGoing {
			keepGoing = t.Run("delete2", func(t *testing.T) {
				testSingleDelete(t, db, key)
			})
		}
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

func testSingleGet(t *testing.T, db dal.Database, key *dal.Key, mustExists bool) {
	var data = new(TestData)
	record := dal.NewRecordWithData(key, data)
	ctx := context.Background()
	err := db.Get(ctx, record)
	if err != nil {
		if dal.IsNotFound(err) {
			if mustExists {
				t.Errorf("record expected to exist but received error: %v", err)
			}
		} else {
			t.Errorf("unexpected error: %v", err)
		}
	} else {
		if data.StringProp == "" {
			t.Error("field 'StringProp' is unexpectedely empty")
		}
		if data.IntegerProp == 0 {
			t.Error("field 'IntegerProp' is unexpectedely 0")
		}
		if !mustExists {
			t.Error("record unexpectedely found")
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
