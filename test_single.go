package end2end

import (
	"context"
	"github.com/dal-go/dalgo/dal"
	"testing"
)

func testSingleOperations(ctx context.Context, t *testing.T, db dal.DB) {
	t.Run("single", func(t *testing.T) {
		const id = "r0"
		key := dal.NewKeyWithID(E2ETestKind1, id)
		var keepGoing bool
		keepGoing = t.Run("delete1", func(t *testing.T) {
			testSingleDelete(t, db, key)
		})
		if keepGoing {
			keepGoing = t.Run("get1", func(t *testing.T) {
				testSingleGet(ctx, t, db, key, false)
			})
		}
		if keepGoing {
			keepGoing = t.Run("exists1", func(t *testing.T) {
				testSingleExists(ctx, t, db, key, false)
			})
		}
		if keepGoing {
			keepGoing = t.Run("create", func(t *testing.T) {
				t.Run("with_predefined_id", func(t *testing.T) {
					testSingleCreateWithPredefinedID(ctx, t, db, key)
				})
			})
		}
		if keepGoing {
			keepGoing = t.Run("exists2", func(t *testing.T) {
				testSingleExists(ctx, t, db, key, true)
			})
		}
		if keepGoing {
			keepGoing = t.Run("get2", func(t *testing.T) {
				testSingleGet(ctx, t, db, key, true)
			})
		}
		if keepGoing {
			/*keepGoing*/ _ = t.Run("delete2", func(t *testing.T) {
				testSingleDelete(t, db, key)
			})
		}
		if keepGoing {
			keepGoing = t.Run("exists3", func(t *testing.T) {
				testSingleExists(ctx, t, db, key, false)
			})
		}
	})
}

func testSingleDelete(t *testing.T, db dal.DB, key *dal.Key) {
	ctx := context.Background()
	err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Delete(ctx, key)
	})
	if err != nil {
		t.Errorf("Failed to delete: %v", err)
	}

}

func testSingleExists(ctx context.Context, t *testing.T, db dal.DB, key *dal.Key, expectedToExist bool) {
	exists, err := db.Exists(ctx, key)
	if err != nil {
		if dal.IsNotFound(err) {
			t.Errorf("db.Exists(ctx, key) should return no error in case of NotFound error, got: %v", err)
		} else {
			t.Errorf("unexpected error: %v", err)
		}
		return
	}
	if expectedToExist && !exists {
		t.Error("record expected to exist but received exists=false")
		return
	}
	if !expectedToExist && exists {
		t.Error("record expected NOT to exist but received exists=true")
		return
	}
}

func testSingleGet(ctx context.Context, t *testing.T, db dal.DB, key *dal.Key, mustExists bool) {
	var data = new(TestData)
	record := dal.NewRecordWithData(key, data)
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

func testSingleCreateWithPredefinedID(ctx context.Context, t *testing.T, db dal.DB, key *dal.Key) {
	data := TestData{
		StringProp:  "str1",
		IntegerProp: 1,
	}
	record := dal.NewRecordWithData(key, &data)
	err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Insert(ctx, record)
	})
	if err != nil {
		t.Errorf("got unexpected error: %v", err)
	}
}
