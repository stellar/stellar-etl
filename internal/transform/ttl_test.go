package transform

import (
	"fmt"
	"testing"
	"time"

	"github.com/guregu/null"
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
				Type: xdr.LedgerEntryTypeOffer,
				Pre:  nil,
				Post: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeOffer,
					},
				},
				Ledger: &xdr.LedgerCloseMeta{
					V: 1,
					V1: &xdr.LedgerCloseMetaV1{
						LedgerHeader: xdr.LedgerHeaderHistoryEntry{
							Header: xdr.LedgerHeader{
								ScpValue: xdr.StellarValue{
									CloseTime: 1000,
								},
								LedgerSeq: 10,
							},
						},
					},
				},
				Transaction: &ingest.LedgerTransaction{
					Index: 1,
				},
				OperationIndex: 1,
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
		actualOutput, actualError := TransformTtl(test.input)
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
			Type: xdr.LedgerEntryTypeTtl,
			Pre:  &preTtlLedgerEntry,
			Post: &TtlLedgerEntry,
			Ledger: &xdr.LedgerCloseMeta{
				V: 1,
				V1: &xdr.LedgerCloseMetaV1{
					LedgerHeader: xdr.LedgerHeaderHistoryEntry{
						Header: xdr.LedgerHeader{
							ScpValue: xdr.StellarValue{
								CloseTime: 1000,
							},
							LedgerSeq: 10,
						},
					},
				},
			},
			Transaction: &ingest.LedgerTransaction{
				Index: 1,
				Envelope: xdr.TransactionEnvelope{
					Type: 2,
					V1: &xdr.TransactionV1Envelope{
						Tx: xdr.Transaction{
							Operations: []xdr.Operation{
								{
									Body: xdr.OperationBody{
										Type: 1,
									},
								},
							},
						},
					},
				},
			},
			OperationIndex: 0,
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
			TransactionID:      null.NewInt(42949677056, true),
			OperationID:        null.NewInt(42949677057, true),
			OperationType:      null.NewInt(1, true),
		},
	}
}
