package end2end

import (
	"context"
	"github.com/dal-go/dalgo/dal"
	"testing"
)

// TestDalgoDB tests a dalgo DB implementation
func TestDalgoDB(t *testing.T, db dal.DB, errQuerySupport error, eventuallyConsistent bool) {
	if t == nil {
		panic("t == nil")
	}
	if db == nil {
		panic("db == nil")
	}

	ctx := context.Background()

	t.Run("single", func(t *testing.T) {
		testSingleOperations(ctx, t, db)
	})
	t.Run("multi", func(t *testing.T) {
		testMultiOperations(ctx, t, db)
	})
	t.Run("query", func(t *testing.T) {
		if errQuerySupport == nil {
			testQueryOperations(ctx, t, db, eventuallyConsistent)
		} else {
			t.Skip("query not supported by dalgo driver or unerlying DB:", errQuerySupport)
		}
	})
}
