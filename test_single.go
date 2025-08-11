package end2end

import (
	"context"
	"github.com/dal-go/dalgo/dal"
	"testing"
)

func singleOperationsTest(ctx context.Context, t *testing.T, db dal.DB) {
	t.Run("single", func(t *testing.T) {
		const id = "r0"
		key := dal.NewKeyWithID(E2ETestKind1, id)
		if !t.Run("delete1", func(t *testing.T) {
			singleDeleteTest(t, db, key)
		}) {
			return
		}
		if !t.Run("get1", func(t *testing.T) {
			singleGetTest(ctx, t, db, key, false)
		}) {
			return
		}
		if !t.Run("exists1", func(t *testing.T) {
			singleExistsTest(ctx, t, db, key, false)
		}) {
			return
		}
		if !t.Run("create", func(t *testing.T) {
			if !t.Run("with_predefined_id", func(t *testing.T) {
				singleCreateWithPredefinedIDTest(ctx, t, db, key)
			}) {
				t.Error("failed in sub-test")
			}
		}) {
			return
		}
		if !t.Run("exists2", func(t *testing.T) {
			singleExistsTest(ctx, t, db, key, true)
		}) {
			return
		}
		if !t.Run("get2", func(t *testing.T) {
			singleGetTest(ctx, t, db, key, true)
		}) {
			return
		}
		if !t.Run("delete2", func(t *testing.T) {
			singleDeleteTest(t, db, key)
		}) {
			return
		}
		if !t.Run("exists3", func(t *testing.T) {
			singleExistsTest(ctx, t, db, key, false)
		}) {
			return
		}
	})
}

func singleDeleteTest(t *testing.T, db dal.DB, key *dal.Key) {
	ctx := context.Background()
	err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Delete(ctx, key)
	}, dal.TxWithName("singleDeleteTest"))
	if err != nil {
		t.Errorf("Failed to delete: %v", err)
	}

}

func singleExistsTest(ctx context.Context, t *testing.T, db dal.DB, key *dal.Key, expectedToExist bool) {
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

func singleGetTest(ctx context.Context, t *testing.T, db dal.DB, key *dal.Key, mustExists bool) {
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

func singleCreateWithPredefinedIDTest(ctx context.Context, t *testing.T, db dal.DB, key *dal.Key) {
	data := TestData{
		StringProp:  "str1",
		IntegerProp: 1,
	}
	record := dal.NewRecordWithData(key, &data)
	err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Insert(ctx, record)
	}, dal.TxWithName("singleCreateWithPredefinedIDTest"))
	if err != nil {
		t.Errorf("got unexpected error: %v", err)
	}
}
