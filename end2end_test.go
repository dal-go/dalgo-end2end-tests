package end2end

import "testing"

func TestEndToEnd(t *testing.T) {
	t.Run("panics_on_nil_params", func(t *testing.T) {
		defer func() {
			if err := recover(); err == nil {
				t.Fatal("should panic on nil parameters")
			}
		}()
		TestDalgoDB(nil, nil, nil, true)
	})
}
