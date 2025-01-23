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

func TestTransformConfigSetting(t *testing.T) {
	type transformTest struct {
		input      ingest.Change
		wantOutput ConfigSettingOutput
		wantErr    error
	}

	hardCodedInput := makeConfigSettingTestInput()
	hardCodedOutput := makeConfigSettingTestOutput()
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
			ConfigSettingOutput{}, fmt.Errorf("could not extract config setting from ledger entry; actual type is LedgerEntryTypeOffer"),
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
		actualOutput, actualError := TransformConfigSetting(test.input)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeConfigSettingTestInput() []ingest.Change {
	var contractMaxByte xdr.Uint32 = 0

	contractDataLedgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 24229503,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId:      xdr.ConfigSettingIdConfigSettingContractMaxSizeBytes,
				ContractMaxSizeBytes: &contractMaxByte,
			},
		},
	}

	return []ingest.Change{
		{
			Type: xdr.LedgerEntryTypeConfigSetting,
			Pre:  &xdr.LedgerEntry{},
			Post: &contractDataLedgerEntry,
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

func makeConfigSettingTestOutput() []ConfigSettingOutput {
	contractMapType := make([]map[string]string, 0)
	bucket := make([]uint64, 0)

	return []ConfigSettingOutput{
		{
			ConfigSettingId:                 0,
			ContractMaxSizeBytes:            0,
			LedgerMaxInstructions:           0,
			TxMaxInstructions:               0,
			FeeRatePerInstructionsIncrement: 0,
			TxMemoryLimit:                   0,
			LedgerMaxReadLedgerEntries:      0,
			LedgerMaxReadBytes:              0,
			LedgerMaxWriteLedgerEntries:     0,
			LedgerMaxWriteBytes:             0,
			TxMaxReadLedgerEntries:          0,
			TxMaxReadBytes:                  0,
			TxMaxWriteLedgerEntries:         0,
			TxMaxWriteBytes:                 0,
			FeeReadLedgerEntry:              0,
			FeeWriteLedgerEntry:             0,
			FeeRead1Kb:                      0,
			BucketListTargetSizeBytes:       0,
			WriteFee1KbBucketListLow:        0,
			WriteFee1KbBucketListHigh:       0,
			BucketListWriteFeeGrowthFactor:  0,
			FeeHistorical1Kb:                0,
			TxMaxContractEventsSizeBytes:    0,
			FeeContractEvents1Kb:            0,
			LedgerMaxTxsSizeBytes:           0,
			TxMaxSizeBytes:                  0,
			FeeTxSize1Kb:                    0,
			ContractCostParamsCpuInsns:      contractMapType,
			ContractCostParamsMemBytes:      contractMapType,
			ContractDataKeySizeBytes:        0,
			ContractDataEntrySizeBytes:      0,
			MaxEntryTtl:                     0,
			MinTemporaryTtl:                 0,
			MinPersistentTtl:                0,
			AutoBumpLedgers:                 0,
			PersistentRentRateDenominator:   0,
			TempRentRateDenominator:         0,
			MaxEntriesToArchive:             0,
			BucketListSizeWindowSampleSize:  0,
			EvictionScanSize:                0,
			StartingEvictionScanLevel:       0,
			LedgerMaxTxCount:                0,
			BucketListSizeWindow:            bucket,
			LastModifiedLedger:              24229503,
			LedgerEntryChange:               1,
			Deleted:                         false,
			LedgerSequence:                  10,
			ClosedAt:                        time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC),
			TransactionID:                   null.NewInt(42949677056, true),
			OperationID:                     null.NewInt(42949677057, true),
			OperationType:                   null.NewInt(1, true),
		},
	}
}
