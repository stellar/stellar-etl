package transform

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestTransformTtl(t *testing.T) {
	type transformTest struct {
		input      ingest.Change
		wantOutput TtlOutput
		wantErr    error
	}

	hardCodedInput := makeTtlTestInput()
	hardCodedOutput := makeTtlTestOutput()
	tests := []transformTest{
		{
			ingest.Change{
				ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
				Type:       xdr.LedgerEntryTypeOffer,
				Pre:        nil,
				Post: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeOffer,
					},
				},
			},
			TtlOutput{}, fmt.Errorf("could not extract ttl from ledger entry; actual type is LedgerEntryTypeOffer"),
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
		header := xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{
				ScpValue: xdr.StellarValue{
					CloseTime: 1000,
				},
				LedgerSeq: 10,
			},
		}
		actualOutput, actualError := TransformTtl(test.input, header)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeTtlTestInput() []ingest.Change {
	var hash xdr.Hash

	preTtlLedgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 0,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTtl,
			Ttl: &xdr.TtlEntry{
				KeyHash:            hash,
				LiveUntilLedgerSeq: 0,
			},
		},
	}

	TtlLedgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 1,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeTtl,
			Ttl: &xdr.TtlEntry{
				KeyHash:            hash,
				LiveUntilLedgerSeq: 123,
			},
		},
	}

	return []ingest.Change{
		{
			ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
			Type:       xdr.LedgerEntryTypeTtl,
			Pre:        &preTtlLedgerEntry,
			Post:       &TtlLedgerEntry,
		},
	}
}

func makeTtlTestOutput() []TtlOutput {
	return []TtlOutput{
		{
			KeyHash:            "0000000000000000000000000000000000000000000000000000000000000000",
			LiveUntilLedgerSeq: 123,
			LastModifiedLedger: 1,
			LedgerEntryChange:  1,
			Deleted:            false,
			LedgerSequence:     10,
			ClosedAt:           time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC),
		},
	}
}
