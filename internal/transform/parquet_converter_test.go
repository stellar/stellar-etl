package transform

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestToParquetCarriesClosedAtMs guards the one failure mode the schema trio
// exists to prevent.
//
// ToParquet builds a named-field struct literal, and Go does not require every
// field to be listed. A field added to schema.go and schema_parquet.go but
// missed in parquet_converter.go therefore compiles, runs, and writes a column
// that is zero on every row, with nothing to notice. The integration tests
// cannot catch it either: --write-parquet defaults to false and no test or DAG
// passes it, so the parquet path is never exercised end to end.
func TestToParquetCarriesClosedAtMs(t *testing.T) {
	const want = int64(1594584547123)

	// Every output type that has a parquet twin. The four that do not
	// (ClaimableBalanceOutput, LedgerTransactionOutput, RestoredKeyOutput,
	// TokenTransferOutput) have no ToParquet method and are absent by design.
	sources := []SchemaParquet{
		AccountOutput{},
		AccountSignerOutput{},
		AssetOutput{},
		ConfigSettingOutput{},
		ContractCodeOutput{},
		ContractDataOutput{},
		ContractEventOutput{},
		EffectOutput{},
		LedgerOutput{},
		OfferOutput{},
		OperationOutput{},
		PoolOutput{},
		TradeOutput{},
		TransactionOutput{},
		TrustlineOutput{},
		TtlOutput{},
	}

	for _, source := range sources {
		name := reflect.TypeOf(source).Name()
		t.Run(name, func(t *testing.T) {
			// set ClosedAtMs on a copy of the source struct
			v := reflect.New(reflect.TypeOf(source)).Elem()
			v.Set(reflect.ValueOf(source))
			field := v.FieldByName("ClosedAtMs")
			if !field.IsValid() {
				t.Fatalf("%s has no ClosedAtMs field", name)
			}
			field.SetInt(want)

			converted := v.Interface().(SchemaParquet).ToParquet()

			got := reflect.ValueOf(converted).FieldByName("ClosedAtMs")
			if !got.IsValid() {
				t.Fatalf("%sParquet has no ClosedAtMs field", name)
			}
			assert.Equal(t, want, got.Int(),
				fmt.Sprintf("%s.ToParquet() dropped ClosedAtMs: the mapping is "+
					"missing from parquet_converter.go", name))
		})
	}
}
