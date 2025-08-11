package end2end

import (
	"context"
	"github.com/dal-go/dalgo-end2end-tests/models"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/mocks4dalgo/mock_dal"
	"go.uber.org/mock/gomock"
	"slices"
	"testing"
)

func TestEndToEnd_panics(t *testing.T) {
	t.Run("panics_on_nil_t", func(t *testing.T) {
		defer func() {
			if err := recover(); err == nil {
				t.Fatal("should panic on nil t parameters")
			}
		}()
		ctrl := gomock.NewController(t)
		db := mock_dal.NewMockDB(ctrl)
		TestDalgoDB(nil, db, nil, true)
	})
	t.Run("panics_on_nil_db", func(t *testing.T) {
		defer func() {
			if err := recover(); err == nil {
				t.Fatal("should panic on nil db parameters")
			}
		}()
		TestDalgoDB(t, nil, nil, true)
	})
}

func TestEndToEnd(t *testing.T) {
	dbCtrl := gomock.NewController(t)
	defer dbCtrl.Finish()

	var controllers []*gomock.Controller

	db := mock_dal.NewMockDB(dbCtrl)

	keyOnlyRecord := func(collection, id string) dal.Record {
		return dal.NewRecord(dal.NewKeyWithID(collection, dal.EscapeID(id)))
	}

	var getNumber int
	db.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, r dal.Record) error {
		getNumber++
		switch getNumber {
		case 1:
			r.SetError(dal.ErrRecordNotFound)
			return dal.ErrRecordNotFound
		case 2:
			r.SetError(nil)
			data := r.Data().(*TestData)
			data.StringProp = "str1"
			data.IntegerProp = 1
			r.SetError(nil)
		}
		return nil
	}).Times(2)

	var existsCallNumber int
	db.EXPECT().Exists(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, key *dal.Key) (bool, error) {
		existsCallNumber++
		switch existsCallNumber {
		case 1:
			return false, nil
		case 2:
			return true, nil
		case 3:
			return false, nil
		default:
			panic("unexpected call number")
		}
	}).Times(3)

	readCityIDs := func(cityIDs []string) func(ctx context.Context, query dal.Query) (dal.Reader, error) {
		return func(ctx context.Context, query dal.Query) (dal.Reader, error) {
			ctrl := gomock.NewController(t)
			controllers = append(controllers, ctrl)
			reader := mock_dal.NewMockReader(ctrl)
			i := 0
			sortedCityIDs := make([]string, len(cityIDs))
			copy(sortedCityIDs, cityIDs)
			if orderBy := query.OrderBy(); len(orderBy) == 1 {
				if orderBy[0].Descending() {
					slices.Reverse(sortedCityIDs)
				}
			}
			limit := query.Limit()
			if citiesCount := len(sortedCityIDs); limit == 0 || limit > citiesCount {
				limit = citiesCount
			}
			reader.EXPECT().Next().DoAndReturn(func() (r dal.Record, err error) {
				if i >= limit {
					return nil, dal.ErrNoMoreRecords
				}
				r = keyOnlyRecord(models.CitiesCollection, sortedCityIDs[i])
				i++
				return
			}).AnyTimes()
			reader.EXPECT().Close().Times(1)
			return reader, nil
		}
	}

	// Expectation for calls WITHOUT transaction options
	db.EXPECT().RunReadwriteTransaction(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, f dal.RWTxWorker, options ...dal.TransactionOption) error {
		ctrl := gomock.NewController(t)
		controllers = append(controllers, ctrl)
		tx := mock_dal.NewMockReadwriteTransaction(ctrl)

		txOptions := dal.NewTransactionOptions(options...)
		//tx.EXPECT().Options().Return(txOptions)

		txName := txOptions.Name()
		t.Log("RW tx:", txName)
		switch txName {
		case "SELECT * FROM Cities: no_limit":
			tx.EXPECT().QueryAllRecords(ctx, gomock.Any()).DoAndReturn(func(ctx context.Context, query dal.Query) (records []dal.Record, err error) {
				records = make([]dal.Record, len(models.Cities))
				for i, city := range models.Cities {
					key := dal.NewKeyWithID("c1", city)
					records[i] = dal.NewRecordWithData(key, &city)
				}
				return
			})
		case "SELECT * FROM Cities: limit=3":
			tx.EXPECT().QueryAllRecords(ctx, gomock.Any()).DoAndReturn(func(ctx context.Context, query dal.Query) (records []dal.Record, err error) {
				records = make([]dal.Record, 3)
				for i, cityID := range models.SortedCityIDs[:3] {
					key := dal.NewKeyWithID("c1", cityID)
					for _, city := range models.Cities {
						if models.CityID(city) == cityID {
							records[i] = dal.NewRecordWithData(key, &city)
							break
						}
					}
				}
				return
			})
		case "singleDeleteTest":
			tx.EXPECT().Delete(ctx, gomock.Any()).Return(nil).Times(1)
		case "deleteAllRecords":
			tx.EXPECT().DeleteMulti(ctx, gomock.Any()).Return(nil).Times(1)
		case "deleteAllCities":
			tx.EXPECT().DeleteMulti(ctx, gomock.Any()).Return(nil).Times(1)
			tx.EXPECT().QueryReader(gomock.Any(), gomock.Any()).DoAndReturn(readCityIDs(models.SortedCityIDs))
		case "singleCreateWithPredefinedIDTest":
			tx.EXPECT().Insert(ctx, gomock.Any()).Return(nil).Times(1)
		case "setMulti":
			tx.EXPECT().SetMulti(ctx, gomock.Any()).Return(nil).Times(1)
		case "update2records":
			tx.EXPECT().UpdateMulti(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
		case "setupDataForQueryTests":
			tx.EXPECT().SetMulti(ctx, gomock.Any()).Return(nil).Times(1)
		case "":
			panic("no RW tx name")
		default:
			panic("unexpected RW tx name: " + txName)
		}
		return f(ctx, tx)
	}).Times(13)

	db.EXPECT().RunReadonlyTransaction(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, f dal.ROTxWorker, options ...dal.TransactionOption) error {
		ctrl := gomock.NewController(t)
		controllers = append(controllers, ctrl)
		tx := mock_dal.NewMockReadTransaction(ctrl)

		txOptions := dal.NewTransactionOptions(options...)
		//tx.EXPECT().Options().Return(txOptions)

		txName := txOptions.Name()
		t.Log("RO tx:", txName)
		switch txName {
		case "SELECT ID FROM Cities; limit=0":
			tx.EXPECT().QueryReader(gomock.Any(), gomock.Any()).DoAndReturn(readCityIDs(models.SortedCityIDs))
		case "SELECT ID FROM Cities ORDER BY Population; limit=3":
			tx.EXPECT().QueryReader(gomock.Any(), gomock.Any()).DoAndReturn(readCityIDs(models.CityIDsSortedByPopulation))
		case "SELECT ID FROM Cities ORDER BY Population DESCENDING; limit=3":
			tx.EXPECT().QueryReader(gomock.Any(), gomock.Any()).DoAndReturn(readCityIDs(models.CityIDsSortedByPopulation))
		case "SELECT ID FROM Cities WHERE Country = 'IN'":
			tx.EXPECT().QueryReader(gomock.Any(), gomock.Any()).DoAndReturn(readCityIDs([]string{"Delhi_Delhi", "Maharashtra_Mumbai"}))
		case "verify_cleanupDelete":
			tx.EXPECT().GetMulti(ctx, gomock.Any()).DoAndReturn(func(ctx context.Context, records []dal.Record) error {
				for _, record := range records {
					record.SetError(dal.ErrRecordNotFound)
				}
				return nil
			}).Times(1)
			//tx.EXPECT().QueryReader(gomock.Any(), gomock.Any()).DoAndReturn(readCityIDs(models.SortedCityIDs))
		case "get3NonExistingRecords":
			tx.EXPECT().GetMulti(ctx, gomock.Any()).DoAndReturn(func(ctx context.Context, records []dal.Record) error {
				for _, record := range records {
					record.SetError(dal.ErrRecordNotFound)
				}
				return nil
			}).Times(1)
		case "using_records_with_data":
			tx.EXPECT().GetMulti(ctx, gomock.Any()).DoAndReturn(func(ctx context.Context, records []dal.Record) error {
				for _, record := range records {
					record.SetError(nil)
					data := record.Data().(*TestData)
					data.StringProp = record.Key().ID.(string) + "str"
				}
				return nil
			}).Times(1)
		case "getMulti2existing2missingRecords":
			tx.EXPECT().GetMulti(ctx, gomock.Any()).DoAndReturn(func(ctx context.Context, records []dal.Record) error {
				r1 := records[0]
				r1.SetError(nil)

				r2 := records[1]
				r2.SetError(nil)

				r3 := records[2]
				r3.SetError(dal.ErrRecordNotFound)

				r4 := records[3]
				r4.SetError(dal.ErrRecordNotFound)

				return nil
			}).Times(1)
		case "getMultiNewRecords":
			tx.EXPECT().GetMulti(ctx, gomock.Any()).DoAndReturn(func(ctx context.Context, records []dal.Record) error {
				//t.Log("len(records):", len(records))
				r1 := records[0]
				r1.SetError(nil)
				d1 := r1.Data().(*TestData)
				d1.StringProp = "UpdateD"

				r2 := records[1]
				r2.SetError(nil)
				d2 := r2.Data().(*TestData)
				d2.StringProp = "UpdateD"

				r3 := records[2]
				r3.SetError(nil)
				d3 := r3.Data().(*TestData)
				d3.StringProp = "k2r1str"

				return nil
			}).Times(1)
		case "selectAllCities":
			tx.EXPECT().QueryAllRecords(ctx, gomock.Any()).Return([]dal.Record{}, nil).Times(1)
		case "SELECT ID FROM Cities; limit=3":
			tx.EXPECT().QueryReader(gomock.Any(), gomock.Any()).DoAndReturn(readCityIDs(models.SortedCityIDs))
		case "":
			panic("no RO tx name")
		default:
			panic("unexpected RO tx name: " + txName)
		}
		return f(ctx, tx)
	}).AnyTimes()

	TestDalgoDB(t, db, nil, true)

	for _, ctrl := range controllers {
		ctrl.Finish()
	}
}
