package transform

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestTransformExpiration(t *testing.T) {
	type transformTest struct {
		input      ingest.Change
		wantOutput ExpirationOutput
		wantErr    error
	}

	hardCodedInput := makeExpirationTestInput()
	hardCodedOutput := makeExpirationTestOutput()
	tests := []transformTest{
		{
			ingest.Change{
				Type: xdr.LedgerEntryTypeOffer,
				Pre:  nil,
				Post: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeOffer,
					},
				},
			},
			ExpirationOutput{}, fmt.Errorf("Could not extract contract code from ledger entry; actual type is LedgerEntryTypeOffer"),
		},
	}

	for i := range hardCodedInput {
		tests = append(tests, transformTest{
			input:      hardCodedInput[i],
			wantOutput: hardCodedOutput[i],
			wantErr:    nil,
		})
	}

	for _, test := range tests {
		actualOutput, actualError := TransformContractCode(test.input)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeExpirationTestInput() []ingest.Change {
	var hash [32]byte

	contractCodeLedgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 24229503,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeExpiration,
			Expiration: &xdr.ExpirationEntry{
				KeyHash:             hash,
				ExpirationLedgerSeq: 1,
			},
		},
	}

	return []ingest.Change{
		{
			Type: xdr.LedgerEntryTypeContractCode,
			Pre:  &xdr.LedgerEntry{},
			Post: &contractCodeLedgerEntry,
		},
	}
}

func makeExpirationTestOutput() []ExpirationOutput {
	return []ExpirationOutput{
		{
			KeyHash:             "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
			ExpirationLedgerSeq: 1,
			LastModifiedLedger:  24229503,
			LedgerEntryChange:   1,
			Deleted:             false,
		},
	}
}
