package end2end

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/update"
)

func deleteAllRecords(ctx context.Context, t *testing.T, db dal.DB, keys []*dal.Key) {
	err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.DeleteMulti(ctx, keys)
	}, dal.TxWithName("deleteAllRecords"))
	if err != nil {
		t.Fatalf("failed at DeleteMulti(ctx, keys) for %v records: %v", len(keys), err)
	}
}

func multiOperationsTest(ctx context.Context, t *testing.T, db dal.DB) {

	var k1r1Key = dal.NewKeyWithID(E2ETestKind1, "k1r1")
	var k1r2Key = dal.NewKeyWithID(E2ETestKind1, "k1r2")
	var k2r1Key = dal.NewKeyWithID(E2ETestKind2, "k2r1")

	var allKeys = []*dal.Key{
		k1r1Key,
		k1r2Key,
		k2r1Key,
	}

	t.Run("1st_initial_delete", func(t *testing.T) {
		deleteAllRecords(ctx, t, db, allKeys)
	})
	t.Run("2nd_initial_delete", func(t *testing.T) {
		deleteAllRecords(ctx, t, db, allKeys)
	})
	t.Run("get_3_non_existing_records", func(t *testing.T) {
		get3NonExistingRecords(t, db)
	})
	t.Run("SetMulti", func(t *testing.T) {
		setMulti(t, db, k1r1Key, k1r2Key, k2r1Key)
	})
	t.Run("GetMulti", func(t *testing.T) {
		t.Run("3_existing_records", func(t *testing.T) {
			getMulti3existingRecords(t, allKeys, db)
		})
		t.Run("2_existing_2_missing_records", func(t *testing.T) {
			getMulti2existing2missingRecords(t, db, k1r1Key, k1r2Key)
		})
	})
	t.Run("update_2_records", func(t *testing.T) {
		update2records(t, db, k1r1Key, k1r2Key, k2r1Key)
	})
	t.Run("cleanup_delete", func(t *testing.T) {
		cleanupDelete(t, db, allKeys)
	})
}

func getMulti2existing2missingRecords(t *testing.T, db dal.DB, k1r1Key, k1r2Key *dal.Key) {
	keys := []*dal.Key{
		k1r1Key,
		k1r2Key,
		dal.NewKeyWithID(E2ETestKind1, "k1r9"),
		dal.NewKeyWithID(E2ETestKind2, "k2r9"),
	}
	data := make([]TestData, len(keys))
	records := make([]dal.Record, len(keys))
	for i, key := range keys {
		records[i] = dal.NewRecordWithData(key, &data[i])
	}
	ctx := context.Background()
	if err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
		return tx.GetMulti(ctx, records)
	}, dal.TxWithName("getMulti2existing2missingRecords")); err != nil {
		t.Fatalf("failed to set multiple records at once: %v", err)
	}
	hasErrors := recordsMustExist(t, records[:2]) > 0
	hasErrors = !recordsMustNotExist(t, records[2:]) || hasErrors
	if hasErrors {
		return
	}
	checkPropValue := func(i int, expected string) error {
		if data[i].StringProp != expected {
			t.Errorf("expected %v got %v, err: %v", expected, data[i].StringProp, records[i].Error())
		}
		return nil
	}
	if err := checkPropValue(0, "k1r1str"); err != nil {
		t.Error(err)
	}
	if err := checkPropValue(1, "k1r2str"); err != nil {
		t.Error(err)
	}
	for i := 2; i < 4; i++ {
		if records[i].Exists() {
			t.Errorf("record unexpectedly showing as existing, key: %v", records[i].Key())
		}
	}
}

func get3NonExistingRecords(t *testing.T, db dal.DB) {
	records := make([]dal.Record, 3)
	for i := 0; i < 3; i++ {
		records[i] = dal.NewRecordWithData(
			dal.NewKeyWithID("NonExistingKind", fmt.Sprintf("non_existing_id_%v", i)),
			&TestData{},
		)
	}
	ctx := context.Background()

	if err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
		return tx.GetMulti(ctx, records)
	}, dal.TxWithName("get3NonExistingRecords")); err != nil {
		t.Fatalf("failed to get multiple records at once: %v", err)
	}
	recordsMustNotExist(t, records)

}
func setMulti(t *testing.T, db dal.DB, k1r1Key, k1r2Key, k2r1Key *dal.Key) {
	newRecord := func(key *dal.Key) dal.Record {
		return dal.NewRecordWithData(key, &TestData{
			StringProp: fmt.Sprintf("%vstr", key.ID),
		})
	}
	records := []dal.Record{
		newRecord(k1r1Key),
		newRecord(k1r2Key),
		newRecord(k2r1Key),
	}
	ctx := context.Background()
	err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.SetMulti(ctx, records)
	}, dal.TxWithName("setMulti"))
	if err != nil {
		t.Fatalf("failed to set multiple records at once: %v", err)
	}
}

func cleanupDelete(t *testing.T, db dal.DB, allKeys []*dal.Key) {
	ctx := context.Background()
	deleteAllRecords(ctx, t, db, allKeys)
	data := make([]struct{}, len(allKeys))
	records := make([]dal.Record, len(allKeys))
	for i := range records {
		records[i] = dal.NewRecordWithData(allKeys[i], &data[i])
	}
	if err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
		return tx.GetMulti(ctx, records)
	}, dal.TxWithName("verify_cleanupDelete")); err != nil {
		t.Fatalf("failed to get multiple records at once: %v", err)
	}
	recordsMustNotExist(t, records)
}

func update2records(t *testing.T, db dal.DB, k1r1Key, k1r2Key, k2r1Key *dal.Key) {
	const newValue = "UpdateD"
	updates := []update.Update{
		update.ByFieldName("StringProp", newValue),
	}
	newRecords := func() []dal.Record {
		data := make([]*TestData, 3)
		for i := range data {
			data[i] = new(TestData)
		}
		return []dal.Record{
			dal.NewRecordWithData(k1r1Key, data[0]),
			dal.NewRecordWithData(k1r2Key, data[1]),
			dal.NewRecordWithData(k2r1Key, data[2]),
		}
	}
	ctx := context.Background()
	err := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.UpdateMulti(ctx, []*dal.Key{k1r1Key, k1r2Key}, updates)
	}, dal.TxWithName("update2records"))
	if err != nil {
		if errors.Is(err, dal.ErrNotSupported) {
			t.Log(err)
			return
		}
		t.Fatalf("failed to update 2 records at once: %v", err)
	}
	records := newRecords()
	if err = db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
		return tx.GetMulti(ctx, records)
	}, dal.TxWithName("getMultiNewRecords")); err != nil {
		t.Fatalf("failed to get 3 records at once: %v", err)
	}
	if recordsMustExist(t, records) > 0 {
		return
	}
	asserRecord := func(i int, expected string) {
		if actual := records[i].Data().(*TestData).StringProp; actual != expected {
			t.Errorf("record #%d expected to have StringProp as '%v' but got '%v', key: %v", i+1, expected, actual, records[i].Key())
		}
	}
	asserRecord(0, newValue)
	asserRecord(1, newValue)
	asserRecord(2, "k2r1str")
}

func recordsMustExist(t *testing.T, records []dal.Record) (missingCount int) {
	for _, record := range records {
		if err := record.Error(); err != nil {
			if !errors.Is(err, dal.ErrNoError) {
				t.Errorf("not able to check record for existence as it has unexpected error: %v", err)
				missingCount++
			}
		}
		if !record.Exists() {
			t.Errorf("record was expected to exist, key: %v", record.Key())
			missingCount++
		}
	}
	return missingCount
}

func recordsMustNotExist(t *testing.T, records []dal.Record) (hasError bool) {
	t.Helper()
	for i, record := range records {
		if err := record.Error(); err != nil {
			hasError = true
			t.Errorf("expected to get `%v` error for  record with key=[%v] but got unexpected error: %v", dal.ErrRecordNotFound, record.Key(), err)
		} else if record.Exists() {
			hasError = true
			t.Errorf("for record #%v of %v Exists() returned true, but expected false; key: %v",
				i+1, len(records), record.Key())
		}
	}
	return hasError
}

func getMulti3existingRecords(t *testing.T, allKeys []*dal.Key, db dal.DB) {
	var data []TestData
	records := make([]dal.Record, len(allKeys))
	assetProps := func(t *testing.T) {
		//if recordsMustExist(t, records) > 0 {
		//	return
		//}
		assertStringProp := func(i int, record dal.Record) {
			id := record.Key().ID.(string)
			if expected, actual := id+"str", data[i].StringProp; actual != expected {
				t.Errorf("field StringProp was expected to have value '%v' got '%v'", expected, actual)
			}
		}
		for i, record := range records {
			assertStringProp(i, record)
		}
	}
	t.Run("using_records_with_data", func(t *testing.T) {
		data = make([]TestData, len(allKeys))
		for i := range records {
			records[i] = dal.NewRecordWithData(allKeys[i], &data[i])
		}
		ctx := context.Background()
		if err := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
			return tx.GetMulti(ctx, records)
		}, dal.TxWithName("using_records_with_data")); err != nil {
			t.Fatalf("failed to get multiple records at once: %v", err)
		}
		if recordsMustExist(t, records) > 0 {
			return
		}
		assetProps(t)
	})
	//t.Run("using_DataTo", func(t *testing.T) {
	//	for i := range records {
	//		records[i] = dal.NewRecord(allKeys[i])
	//	}
	//	if err := db.GetMulti(ctx, records); err != nil {
	//		t.Fatalf("failed to get multiple records at once: %v", err)
	//	}
	//	recordsMustExist(t, records)
	//	data = make([]TestData, len(allKeys))
	//	for i, record := range records {
	//		if err := record.DataTo(&data[i]); err != nil {
	//			t.Fatalf("failed to record #%v", i+1)
	//		}
	//	}
	//	assetProps(t)
	//})
}
