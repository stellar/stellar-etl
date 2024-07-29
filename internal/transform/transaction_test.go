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
		{
			negativeSeqInput,
			TransactionOutput{},
			fmt.Errorf("the account's sequence number (-1) is negative for ledger 0; transaction 1 (transaction id=4096)"),
		},
		{
			badFeeChargedInput,
			TransactionOutput{},
			fmt.Errorf("the fee charged (-1) is negative for ledger 0; transaction 1 (transaction id=4096)"),
		},
		{
			badTimeboundInput,
			TransactionOutput{},
			fmt.Errorf("the max time is earlier than the min time (100 < 1594586912) for ledger 0; transaction 1 (transaction id=4096)"),
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
		{
			TxEnvelope:                   "AAAAAgAAAACI4aa0pXFSj6qfJuIObLw/5zyugLRGYwxb7wFSr3B9eAABX5ABjydzAABBtwAAAAEAAAAAAAAAAAAAAABfBqt0AAAAAQAAABdITDVhQ2dvelFISVc3c1NjNVhkY2ZtUgAAAAABAAAAAQAAAAAcR0GXGO76pFs4y38vJVAanjnLg4emNun7zAx0pHcDGAAAAAIAAAAAAAAAAAAAAAAAAAAAAQIDAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFjQq+PAAAAQPRri1y9nM9PVDgCRksW7TJk8p+xG/BCerYtvU4Ffxo9s+7lTCDOeg2ahZSVHfowhCxWozggLEtX4vtMBDu2hAg=",
			TxResult:                     "AAAAAAAAASz/////AAAAAQAAAAAAAAAAAAAAAAAAAAA=",
			TxMeta:                       "AAAAAQAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAAA",
			TxFeeMeta:                    "AAAAAA==",
			TransactionHash:              "a87fef5eeb260269c380f2de456aad72b59bb315aaac777860456e09dac0bafb",
			LedgerSequence:               30521816,
			TransactionID:                131090201534533632,
			Account:                      testAccount1Address,
			AccountSequence:              112351890582290871,
			MaxFee:                       90000,
			FeeCharged:                   300,
			OperationCount:               1,
			CreatedAt:                    correctTime,
			MemoType:                     "MemoTypeMemoText",
			Memo:                         "HL5aCgozQHIW7sSc5XdcfmR",
			TimeBounds:                   "[0,1594272628)",
			Successful:                   false,
			ClosedAt:                     time.Date(2020, time.July, 9, 5, 28, 42, 0, time.UTC),
			ResourceFee:                  0,
			SorobanResourcesInstructions: 0,
			SorobanResourcesReadBytes:    0,
			SorobanResourcesWriteBytes:   0,
			TransactionResultCode:        "TransactionResultCodeTxFailed",
			Signatures:                   []string{"9GuLXL2cz09UOAJGSxbtMmTyn7Eb8EJ6ti29TgV/Gj2z7uVMIM56DZqFlJUd+jCELFajOCAsS1fi+0wEO7aECA=="},
		},
		{
			TxEnvelope:                   "AAAABQAAAQAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAHCAAAAACAAAAAIjhprSlcVKPqp8m4g5svD/nPK6AtEZjDFvvAVKvcH14AAAAAAIU9jYAAAB9AAAAAQAAAAAAAAAAAAAAAF8Gq3QAAAABAAAAF0hMNWFDZ296UUhJVzdzU2M1WGRjZm1SAAAAAAEAAAABAAAAABxHQZcY7vqkWzjLfy8lUBqeOcuDh6Y26fvMDHSkdwMYAAAAAgAAAAAAAAAAAAAAAAAAAAABAgMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABY0KvjwAAAED0a4tcvZzPT1Q4AkZLFu0yZPKfsRvwQnq2Lb1OBX8aPbPu5UwgznoNmoWUlR36MIQsVqM4ICxLV+L7TAQ7toQI",
			TxResult:                     "AAAAAAAAASwAAAABqH/vXusmAmnDgPLeRWqtcrWbsxWqrHd4YEVuCdrAuvsAAAAAAAAAZAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
			TxMeta:                       "AAAAAQAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAAA",
			TxFeeMeta:                    "AAAAAA==",
			TransactionHash:              "a87fef5eeb260269c380f2de456aad72b59bb315aaac777860456e09dac0bafb",
			LedgerSequence:               30521817,
			TransactionID:                131090205829500928,
			Account:                      testAccount1Address,
			AccountSequence:              150015399398735997,
			MaxFee:                       0,
			FeeCharged:                   300,
			OperationCount:               1,
			CreatedAt:                    correctTime,
			MemoType:                     "MemoTypeMemoText",
			Memo:                         "HL5aCgozQHIW7sSc5XdcfmR",
			TimeBounds:                   "[0,1594272628)",
			Successful:                   true,
			InnerTransactionHash:         "a87fef5eeb260269c380f2de456aad72b59bb315aaac777860456e09dac0bafb",
			FeeAccount:                   testAccount5Address,
			FeeAccountMuxed:              "MAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFNZG",
			NewMaxFee:                    7200,
			ClosedAt:                     time.Date(2020, time.July, 9, 5, 28, 42, 0, time.UTC),
			ResourceFee:                  0,
			SorobanResourcesInstructions: 0,
			SorobanResourcesReadBytes:    0,
			SorobanResourcesWriteBytes:   0,
			TransactionResultCode:        "TransactionResultCodeTxFeeBumpInnerSuccess", //inner fee bump success
			Signatures:                   []string{},
		},
		{
			TxEnvelope:                   "AAAAAgAAAAAcR0GXGO76pFs4y38vJVAanjnLg4emNun7zAx0pHcDGAAAAGQBpLyvsiV6gwAAAAIAAAABAAAAAAAAAAAAAAAAXwardAAAAAEAAAAFAAAACgAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAMCAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAABdITDVhQ2dvelFISVc3c1NjNVhkY2ZtUgAAAAABAAAAAQAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAAIAAAAAAAAAAAAAAAAAAAAAAQIDAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFjQq+PAAAAQPRri1y9nM9PVDgCRksW7TJk8p+xG/BCerYtvU4Ffxo9s+7lTCDOeg2ahZSVHfowhCxWozggLEtX4vtMBDu2hAg=",
			TxResult:                     "AAAAAAAAAGT////5AAAAAA==",
			TxMeta:                       "AAAAAQAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAAA",
			TxFeeMeta:                    "AAAAAA==",
			TransactionHash:              "a87fef5eeb260269c380f2de456aad72b59bb315aaac777860456e09dac0bafb",
			LedgerSequence:               30521818,
			TransactionID:                131090210124468224,
			Account:                      testAccount2Address,
			AccountSequence:              118426953012574851,
			MaxFee:                       100,
			FeeCharged:                   100,
			OperationCount:               1,
			CreatedAt:                    correctTime,
			MemoType:                     "MemoTypeMemoText",
			Memo:                         "HL5aCgozQHIW7sSc5XdcfmR",
			TimeBounds:                   "[0,1594272628)",
			Successful:                   false,
			LedgerBounds:                 "[5,10)",
			ExtraSigners:                 pq.StringArray{"GABQEAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB7QL"},
			MinAccountSequenceAge:        null.IntFrom(0),
			MinAccountSequenceLedgerGap:  null.IntFrom(0),
			ClosedAt:                     time.Date(2020, time.July, 9, 5, 28, 42, 0, time.UTC),
			ResourceFee:                  0,
			SorobanResourcesInstructions: 0,
			SorobanResourcesReadBytes:    0,
			SorobanResourcesWriteBytes:   0,
			TransactionResultCode:        "TransactionResultCodeTxInsufficientBalance",
			Signatures:                   []string{"9GuLXL2cz09UOAJGSxbtMmTyn7Eb8EJ6ti29TgV/Gj2z7uVMIM56DZqFlJUd+jCELFajOCAsS1fi+0wEO7aECA=="},
		},
	}
	return
}
func makeTransactionTestInput() (transaction []ingest.LedgerTransaction, historyHeader []xdr.LedgerHeaderHistoryEntry, err error) {
	hardCodedMemoText := "HL5aCgozQHIW7sSc5XdcfmR"
	hardCodedTransactionHash := xdr.Hash([32]byte{0xa8, 0x7f, 0xef, 0x5e, 0xeb, 0x26, 0x2, 0x69, 0xc3, 0x80, 0xf2, 0xde, 0x45, 0x6a, 0xad, 0x72, 0xb5, 0x9b, 0xb3, 0x15, 0xaa, 0xac, 0x77, 0x78, 0x60, 0x45, 0x6e, 0x9, 0xda, 0xc0, 0xba, 0xfb})
	genericResultResults := &[]xdr.OperationResult{
		{
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
		{
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
							{
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
					Signatures: []xdr.DecoratedSignature{
						{
							Hint:      xdr.SignatureHint{99, 66, 175, 143},
							Signature: xdr.Signature{244, 107, 139, 92, 189, 156, 207, 79, 84, 56, 2, 70, 75, 22, 237, 50, 100, 242, 159, 177, 27, 240, 66, 122, 182, 45, 189, 78, 5, 127, 26, 61, 179, 238, 229, 76, 32, 206, 122, 13, 154, 133, 148, 149, 29, 250, 48, 132, 44, 86, 163, 56, 32, 44, 75, 87, 226, 251, 76, 4, 59, 182, 132, 8},
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
		{
			Index:      1,
			UnsafeMeta: hardCodedMeta,
			Envelope: xdr.TransactionEnvelope{
				Type: xdr.EnvelopeTypeEnvelopeTypeTxFeeBump,
				FeeBump: &xdr.FeeBumpTransactionEnvelope{
					Tx: xdr.FeeBumpTransaction{
						FeeSource: testAccount5,
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
										{
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
					Signatures: []xdr.DecoratedSignature{
						{
							Hint:      xdr.SignatureHint{99, 66, 175, 143},
							Signature: xdr.Signature{244, 107, 139, 92, 189, 156, 207, 79, 84, 56, 2, 70, 75, 22, 237, 50, 100, 242, 159, 177, 27, 240, 66, 122, 182, 45, 189, 78, 5, 127, 26, 61, 179, 238, 229, 76, 32, 206, 122, 13, 154, 133, 148, 149, 29, 250, 48, 132, 44, 86, 163, 56, 32, 44, 75, 87, 226, 251, 76, 4, 59, 182, 132, 8},
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
										{
											Tr: &xdr.OperationResultTr{
												CreateAccountResult: &xdr.CreateAccountResult{},
											},
										},
									},
								},
							},
						},
						Results: &[]xdr.OperationResult{{}},
					},
				},
			},
		},
		{
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
							{
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
					Signatures: []xdr.DecoratedSignature{
						{
							Hint:      xdr.SignatureHint{99, 66, 175, 143},
							Signature: xdr.Signature{244, 107, 139, 92, 189, 156, 207, 79, 84, 56, 2, 70, 75, 22, 237, 50, 100, 242, 159, 177, 27, 240, 66, 122, 182, 45, 189, 78, 5, 127, 26, 61, 179, 238, 229, 76, 32, 206, 122, 13, 154, 133, 148, 149, 29, 250, 48, 132, 44, 86, 163, 56, 32, 44, 75, 87, 226, 251, 76, 4, 59, 182, 132, 8},
						},
					},
				},
			},
			Result: xdr.TransactionResultPair{
				TransactionHash: hardCodedTransactionHash,
				Result: xdr.TransactionResult{
					FeeCharged: 100,
					Result: xdr.TransactionResultResult{
						Code:    xdr.TransactionResultCodeTxInsufficientBalance,
						Results: genericResultResults,
					},
				},
			},
		},
	}
	historyHeader = []xdr.LedgerHeaderHistoryEntry{
		{
			Header: xdr.LedgerHeader{
				LedgerSeq: 30521816,
				ScpValue:  xdr.StellarValue{CloseTime: 1594272522},
			},
		},
		{
			Header: xdr.LedgerHeader{
				LedgerSeq: 30521817,
				ScpValue:  xdr.StellarValue{CloseTime: 1594272522},
			},
		},
		{
			Header: xdr.LedgerHeader{
				LedgerSeq: 30521818,
				ScpValue:  xdr.StellarValue{CloseTime: 1594272522},
			},
		},
	}
	return
}
