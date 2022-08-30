package transform

import (
	"fmt"
	"testing"
	"time"

	"github.com/guregu/null"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestTransformTransaction(t *testing.T) {
	type inputStruct struct {
		transaction   ingest.LedgerTransaction
		historyHeader xdr.LedgerHeaderHistoryEntry
	}
	type transformTest struct {
		input      inputStruct
		wantOutput TransactionOutput
		wantErr    error
	}
	genericInput := inputStruct{genericLedgerTransaction, genericLedgerHeaderHistoryEntry}
	negativeSeqInput := genericInput
	negativeSeqEnvelope := genericBumpOperationEnvelopeForTransaction
	negativeSeqEnvelope.Tx.SeqNum = xdr.SequenceNumber(-1)
	negativeSeqInput.transaction.Envelope.V1 = &negativeSeqEnvelope

	badTimeboundInput := genericInput
	badTimeboundEnvelope := genericBumpOperationEnvelopeForTransaction
	badTimeboundEnvelope.Tx.Cond.Type = xdr.PreconditionTypePrecondTime
	badTimeboundEnvelope.Tx.Cond.TimeBounds = &xdr.TimeBounds{
		MinTime: 1594586912,
		MaxTime: 100,
	}
	badTimeboundInput.transaction.Envelope.V1 = &badTimeboundEnvelope

	badFeeChargedInput := genericInput
	badFeeChargedInput.transaction.Result.Result.FeeCharged = -1

	hardCodedTransaction, hardCodedLedgerHeader, err := makeTransactionTestInput()
	assert.NoError(t, err)
	hardCodedOutput, err := makeTransactionTestOutput()
	assert.NoError(t, err)

	tests := []transformTest{
		transformTest{
			negativeSeqInput,
			TransactionOutput{},
			fmt.Errorf("The account's sequence number (-1) is negative for ledger 0; transaction 1 (transaction id=4096)"),
		},
		{
			badFeeChargedInput,
			TransactionOutput{},
			fmt.Errorf("The fee charged (-1) is negative for ledger 0; transaction 1 (transaction id=4096)"),
		},
		{
			badTimeboundInput,
			TransactionOutput{},
			fmt.Errorf("The max time is earlier than the min time (100 < 1594586912) for ledger 0; transaction 1 (transaction id=4096)"),
		},
	}

	for i := range hardCodedTransaction {
		tests = append(tests, transformTest{
			input:      inputStruct{hardCodedTransaction[i], hardCodedLedgerHeader[i]},
			wantOutput: hardCodedOutput[i],
			wantErr:    nil,
		})
	}

	for _, test := range tests {
		actualOutput, actualError := TransformTransaction(test.input.transaction, test.input.historyHeader)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeTransactionTestOutput() (output []TransactionOutput, err error) {
	correctTime, err := time.Parse("2006-1-2 15:04:05 MST", "2020-07-09 05:28:42 UTC")
	output = []TransactionOutput{
		TransactionOutput{
			TxEnvelope:       "AAAAAgAAAACI4aa0pXFSj6qfJuIObLw/5zyugLRGYwxb7wFSr3B9eAABX5ABjydzAABBtwAAAAEAAAAAAAAAAAAAAABfBqt0AAAAAQAAABdITDVhQ2dvelFISVc3c1NjNVhkY2ZtUgAAAAABAAAAAQAAAAAcR0GXGO76pFs4y38vJVAanjnLg4emNun7zAx0pHcDGAAAAAIAAAAAAAAAAAAAAAAAAAAAAQIDAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
			TxResult:         "AAAAAAAAASz/////AAAAAQAAAAAAAAAAAAAAAAAAAAA=",
			TxMeta:           "AAAAAQAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAAA",
			TxFeeMeta:        "AAAAAA==",
			TransactionHash:  "a87fef5eeb260269c380f2de456aad72b59bb315aaac777860456e09dac0bafb",
			LedgerSequence:   30521816,
			ApplicationOrder: 1,
			TransactionID:    131090201534533632,
			Account:          testAccount1Address,
			AccountSequence:  112351890582290871,
			MaxFee:           90000,
			FeeCharged:       300,
			OperationCount:   1,
			CreatedAt:        correctTime,
			MemoType:         "MemoTypeMemoText",
			Memo:             "HL5aCgozQHIW7sSc5XdcfmR",
			TimeBounds:       "[0,1594272628)",
			Successful:       false,
		},
		TransactionOutput{
			TxEnvelope:           "AAAABQAAAABnzACGTDuJFoxqr+C8NHCe0CHFBXLi+YhhNCIILCIpcgAAAAAAABwgAAAAAgAAAACI4aa0pXFSj6qfJuIObLw/5zyugLRGYwxb7wFSr3B9eAAAAAACFPY2AAAAfQAAAAEAAAAAAAAAAAAAAABfBqt0AAAAAQAAABdITDVhQ2dvelFISVc3c1NjNVhkY2ZtUgAAAAABAAAAAQAAAAAcR0GXGO76pFs4y38vJVAanjnLg4emNun7zAx0pHcDGAAAAAIAAAAAAAAAAAAAAAAAAAAAAQIDAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==",
			TxResult:             "AAAAAAAAASwAAAABqH/vXusmAmnDgPLeRWqtcrWbsxWqrHd4YEVuCdrAuvsAAAAAAAAAZAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
			TxMeta:               "AAAAAQAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAAA",
			TxFeeMeta:            "AAAAAA==",
			TransactionHash:      "a87fef5eeb260269c380f2de456aad72b59bb315aaac777860456e09dac0bafb",
			LedgerSequence:       30521817,
			ApplicationOrder:     1,
			TransactionID:        131090205829500928,
			Account:              testAccount1Address,
			AccountSequence:      150015399398735997,
			MaxFee:               0,
			FeeCharged:           300,
			OperationCount:       1,
			CreatedAt:            correctTime,
			MemoType:             "MemoTypeMemoText",
			Memo:                 "HL5aCgozQHIW7sSc5XdcfmR",
			TimeBounds:           "[0,1594272628)",
			Successful:           true,
			InnerTransactionHash: "a87fef5eeb260269c380f2de456aad72b59bb315aaac777860456e09dac0bafb",
			FeeAccount:           testAccount3Address,
			NewMaxFee:            7200,
		},
		TransactionOutput{
			TxEnvelope:                  "AAAAAgAAAAAcR0GXGO76pFs4y38vJVAanjnLg4emNun7zAx0pHcDGAAAAGQBpLyvsiV6gwAAAAIAAAABAAAAAAAAAAAAAAAAXwardAAAAAEAAAAFAAAACgAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAMCAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAABdITDVhQ2dvelFISVc3c1NjNVhkY2ZtUgAAAAABAAAAAQAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAAIAAAAAAAAAAAAAAAAAAAAAAQIDAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
			TxResult:                    "AAAAAAAAAGT/////AAAAAQAAAAAAAAAAAAAAAAAAAAA=",
			TxMeta:                      "AAAAAQAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAAA",
			TxFeeMeta:                   "AAAAAA==",
			TransactionHash:             "a87fef5eeb260269c380f2de456aad72b59bb315aaac777860456e09dac0bafb",
			LedgerSequence:              30521818,
			ApplicationOrder:            1,
			TransactionID:               131090210124468224,
			Account:                     testAccount2Address,
			AccountSequence:             118426953012574851,
			MaxFee:                      100,
			FeeCharged:                  100,
			OperationCount:              1,
			CreatedAt:                   correctTime,
			MemoType:                    "MemoTypeMemoText",
			Memo:                        "HL5aCgozQHIW7sSc5XdcfmR",
			TimeBounds:                  "[0,1594272628)",
			Successful:                  false,
			LedgerBounds:                "[5,10)",
			ExtraSigners:                pq.StringArray{"GABQEAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB7QL"},
			MinAccountSequenceAge:       null.IntFrom(0),
			MinAccountSequenceLedgerGap: null.IntFrom(0),
		},
	}
	return
}
func makeTransactionTestInput() (transaction []ingest.LedgerTransaction, historyHeader []xdr.LedgerHeaderHistoryEntry, err error) {
	hardCodedMemoText := "HL5aCgozQHIW7sSc5XdcfmR"
	hardCodedTransactionHash := xdr.Hash([32]byte{0xa8, 0x7f, 0xef, 0x5e, 0xeb, 0x26, 0x2, 0x69, 0xc3, 0x80, 0xf2, 0xde, 0x45, 0x6a, 0xad, 0x72, 0xb5, 0x9b, 0xb3, 0x15, 0xaa, 0xac, 0x77, 0x78, 0x60, 0x45, 0x6e, 0x9, 0xda, 0xc0, 0xba, 0xfb})
	genericResultResults := &[]xdr.OperationResult{
		xdr.OperationResult{
			Tr: &xdr.OperationResultTr{
				Type: xdr.OperationTypeCreateAccount,
				CreateAccountResult: &xdr.CreateAccountResult{
					Code: 0,
				},
			},
		},
	}
	hardCodedMeta := xdr.TransactionMeta{
		V:  1,
		V1: genericTxMeta,
	}

	source := xdr.MuxedAccount{
		Type:    xdr.CryptoKeyTypeKeyTypeEd25519,
		Ed25519: &xdr.Uint256{3, 2, 1},
	}
	destination := xdr.MuxedAccount{
		Type:    xdr.CryptoKeyTypeKeyTypeEd25519,
		Ed25519: &xdr.Uint256{1, 2, 3},
	}
	signerKey := xdr.SignerKey{
		Type:    xdr.SignerKeyTypeSignerKeyTypeEd25519,
		Ed25519: source.Ed25519,
	}
	transaction = []ingest.LedgerTransaction{
		ingest.LedgerTransaction{
			Index:      1,
			UnsafeMeta: hardCodedMeta,
			Envelope: xdr.TransactionEnvelope{
				Type: xdr.EnvelopeTypeEnvelopeTypeTx,
				V1: &xdr.TransactionV1Envelope{
					Tx: xdr.Transaction{
						SourceAccount: testAccount1,
						SeqNum:        112351890582290871,
						Memo: xdr.Memo{
							Type: xdr.MemoTypeMemoText,
							Text: &hardCodedMemoText,
						},
						Fee: 90000,
						Cond: xdr.Preconditions{
							Type: xdr.PreconditionTypePrecondTime,
							TimeBounds: &xdr.TimeBounds{
								MinTime: 0,
								MaxTime: 1594272628,
							},
						},
						Operations: []xdr.Operation{
							xdr.Operation{
								SourceAccount: &testAccount2,
								Body: xdr.OperationBody{
									Type: xdr.OperationTypePathPaymentStrictReceive,
									PathPaymentStrictReceiveOp: &xdr.PathPaymentStrictReceiveOp{
										Destination: destination,
									},
								},
							},
						},
					},
				},
			},
			Result: xdr.TransactionResultPair{
				TransactionHash: hardCodedTransactionHash,
				Result: xdr.TransactionResult{
					FeeCharged: 300,
					Result: xdr.TransactionResultResult{
						Code:    xdr.TransactionResultCodeTxFailed,
						Results: genericResultResults,
					},
				},
			},
		},
		ingest.LedgerTransaction{
			Index:      1,
			UnsafeMeta: hardCodedMeta,
			Envelope: xdr.TransactionEnvelope{
				Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
				FeeBump: &xdr.FeeBumpTransactionEnvelope{
					Tx: xdr.FeeBumpTransaction{
						FeeSource: testAccount3,
						Fee:       7200,
						InnerTx: xdr.FeeBumpTransactionInnerTx{
							Type: xdr.EnvelopeTypeEnvelopeTypeTx,
							V1: &xdr.TransactionV1Envelope{
								Tx: xdr.Transaction{
									SourceAccount: testAccount1,
									SeqNum:        150015399398735997,
									Memo: xdr.Memo{
										Type: xdr.MemoTypeMemoText,
										Text: &hardCodedMemoText,
									},
									Cond: xdr.Preconditions{
										Type: xdr.PreconditionTypePrecondTime,
										TimeBounds: &xdr.TimeBounds{
											MinTime: 0,
											MaxTime: 1594272628,
										},
									},
									Operations: []xdr.Operation{
										xdr.Operation{
											SourceAccount: &testAccount2,
											Body: xdr.OperationBody{
												Type: xdr.OperationTypePathPaymentStrictReceive,
												PathPaymentStrictReceiveOp: &xdr.PathPaymentStrictReceiveOp{
													Destination: destination,
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			Result: xdr.TransactionResultPair{
				TransactionHash: hardCodedTransactionHash,
				Result: xdr.TransactionResult{
					FeeCharged: 300,
					Result: xdr.TransactionResultResult{
						Code: xdr.TransactionResultCodeTxFeeBumpInnerSuccess,
						InnerResultPair: &xdr.InnerTransactionResultPair{
							TransactionHash: hardCodedTransactionHash,
							Result: xdr.InnerTransactionResult{
								FeeCharged: 100,
								Result: xdr.InnerTransactionResultResult{
									Code: xdr.TransactionResultCodeTxSuccess,
									Results: &[]xdr.OperationResult{
										xdr.OperationResult{
											Tr: &xdr.OperationResultTr{
												CreateAccountResult: &xdr.CreateAccountResult{},
											},
										},
									},
								},
							},
						},
						Results: &[]xdr.OperationResult{
							xdr.OperationResult{},
						},
					},
				},
			},
		},
		ingest.LedgerTransaction{
			Index:      1,
			UnsafeMeta: hardCodedMeta,
			Envelope: xdr.TransactionEnvelope{
				Type: xdr.EnvelopeTypeEnvelopeTypeTx,
				V1: &xdr.TransactionV1Envelope{
					Tx: xdr.Transaction{
						SourceAccount: testAccount2,
						SeqNum:        118426953012574851,
						Memo: xdr.Memo{
							Type: xdr.MemoTypeMemoText,
							Text: &hardCodedMemoText,
						},
						Fee: 100,
						Cond: xdr.Preconditions{
							Type: xdr.PreconditionTypePrecondV2,
							V2: &xdr.PreconditionsV2{
								TimeBounds: &xdr.TimeBounds{
									MinTime: 0,
									MaxTime: 1594272628,
								},
								LedgerBounds: &xdr.LedgerBounds{
									MinLedger: 5,
									MaxLedger: 10,
								},
								ExtraSigners: []xdr.SignerKey{signerKey},
							},
						},
						Operations: []xdr.Operation{
							xdr.Operation{
								SourceAccount: &testAccount4,
								Body: xdr.OperationBody{
									Type: xdr.OperationTypePathPaymentStrictReceive,
									PathPaymentStrictReceiveOp: &xdr.PathPaymentStrictReceiveOp{
										Destination: destination,
									},
								},
							},
						},
					},
				},
			},
			Result: xdr.TransactionResultPair{
				TransactionHash: hardCodedTransactionHash,
				Result: xdr.TransactionResult{
					FeeCharged: 100,
					Result: xdr.TransactionResultResult{
						Code:    xdr.TransactionResultCodeTxFailed,
						Results: genericResultResults,
					},
				},
			},
		},
	}
	historyHeader = []xdr.LedgerHeaderHistoryEntry{
		xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{
				LedgerSeq: 30521816,
				ScpValue:  xdr.StellarValue{CloseTime: 1594272522},
			},
		},
		xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{
				LedgerSeq: 30521817,
				ScpValue:  xdr.StellarValue{CloseTime: 1594272522},
			},
		},
		xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{
				LedgerSeq: 30521818,
				ScpValue:  xdr.StellarValue{CloseTime: 1594272522},
			},
		},
	}
	return
}
