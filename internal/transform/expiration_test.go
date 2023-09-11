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
			ExpirationOutput{}, fmt.Errorf("Could not extract expiration from ledger entry; actual type is LedgerEntryTypeOffer"),
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
		actualOutput, actualError := TransformExpiration(test.input)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeExpirationTestInput() []ingest.Change {
	var hash xdr.Hash

	preExpirationLedgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 0,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeExpiration,
			Expiration: &xdr.ExpirationEntry{
				KeyHash:             hash,
				ExpirationLedgerSeq: 0,
			},
		},
	}

	expirationLedgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 1,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeExpiration,
			Expiration: &xdr.ExpirationEntry{
				KeyHash:             hash,
				ExpirationLedgerSeq: 123,
			},
		},
	}

	return []ingest.Change{
		{
			Type: xdr.LedgerEntryTypeExpiration,
			Pre:  &preExpirationLedgerEntry,
			Post: &expirationLedgerEntry,
		},
	}
}

func makeExpirationTestOutput() []ExpirationOutput {
	return []ExpirationOutput{
		{
			KeyHash:             "0000000000000000000000000000000000000000000000000000000000000000",
			ExpirationLedgerSeq: 123,
			LastModifiedLedger:  1,
			LedgerEntryChange:   1,
			Deleted:             false,
		},
	}
}
