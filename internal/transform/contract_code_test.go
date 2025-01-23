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

func TestTransformContractCode(t *testing.T) {
	type transformTest struct {
		input      ingest.Change
		wantOutput ContractCodeOutput
		wantErr    error
	}

	hardCodedInput := makeContractCodeTestInput()
	hardCodedOutput := makeContractCodeTestOutput()
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
			ContractCodeOutput{}, fmt.Errorf("could not extract contract code from ledger entry; actual type is LedgerEntryTypeOffer"),
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

func makeContractCodeTestInput() []ingest.Change {
	var hash [32]byte

	contractCodeLedgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 24229503,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractCode,
			ContractCode: &xdr.ContractCodeEntry{
				Hash: hash,
				Ext: xdr.ContractCodeEntryExt{
					V: 1,
					V1: &xdr.ContractCodeEntryV1{
						CostInputs: xdr.ContractCodeCostInputs{
							NInstructions:     1,
							NFunctions:        2,
							NGlobals:          3,
							NTableEntries:     4,
							NTypes:            5,
							NDataSegments:     6,
							NElemSegments:     7,
							NImports:          8,
							NExports:          9,
							NDataSegmentBytes: 10,
						},
					},
				},
			},
		},
	}

	return []ingest.Change{
		{
			Type: xdr.LedgerEntryTypeContractCode,
			Pre:  &xdr.LedgerEntry{},
			Post: &contractCodeLedgerEntry,
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

func makeContractCodeTestOutput() []ContractCodeOutput {
	return []ContractCodeOutput{
		{
			ContractCodeHash:   "0000000000000000000000000000000000000000000000000000000000000000",
			ContractCodeExtV:   1,
			LastModifiedLedger: 24229503,
			LedgerEntryChange:  1,
			Deleted:            false,
			LedgerSequence:     10,
			ClosedAt:           time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC),
			LedgerKeyHash:      "dfed061dbe464e0ff320744fcd604ac08b39daa74fa24110936654cbcb915ccc",
			NInstructions:      1,
			NFunctions:         2,
			NGlobals:           3,
			NTableEntries:      4,
			NTypes:             5,
			NDataSegments:      6,
			NElemSegments:      7,
			NImports:           8,
			NExports:           9,
			NDataSegmentBytes:  10,
			TransactionID:      null.NewInt(42949677056, true),
			OperationID:        null.NewInt(42949677057, true),
			OperationType:      null.NewInt(1, true),
		},
	}
}
