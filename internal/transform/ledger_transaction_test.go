package transform

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestTransformTx(t *testing.T) {
	type inputStruct struct {
		transaction   ingest.LedgerTransaction
		historyHeader xdr.LedgerHeaderHistoryEntry
	}
	type transformTest struct {
		input      inputStruct
		wantOutput LedgerTransactionOutput
		wantErr    error
	}

	hardCodedLedgerTransaction, hardCodedLedgerHeader, err := makeLedgerTransactionTestInput()
	assert.NoError(t, err)
	hardCodedOutput, err := makeLedgerTransactionTestOutput()
	assert.NoError(t, err)

	tests := []transformTest{}

	for i := range hardCodedLedgerTransaction {
		tests = append(tests, transformTest{
			input:      inputStruct{hardCodedLedgerTransaction[i], hardCodedLedgerHeader[i]},
			wantOutput: hardCodedOutput[i],
			wantErr:    nil,
		})
	}

	for _, test := range tests {
		actualOutput, actualError := TransformLedgerTransaction(test.input.transaction, test.input.historyHeader)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeLedgerTransactionTestOutput() (output []LedgerTransactionOutput, err error) {
	output = []LedgerTransactionOutput{
		{
			TxEnvelope:      "AAAAAgAAAACI4aa0pXFSj6qfJuIObLw/5zyugLRGYwxb7wFSr3B9eAABX5ABjydzAABBtwAAAAEAAAAAAAAAAAAAAABfBqt0AAAAAQAAABdITDVhQ2dvelFISVc3c1NjNVhkY2ZtUgAAAAABAAAAAQAAAAAcR0GXGO76pFs4y38vJVAanjnLg4emNun7zAx0pHcDGAAAAAIAAAAAAAAAAAAAAAAAAAAAAQIDAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
			TxResult:        "qH/vXusmAmnDgPLeRWqtcrWbsxWqrHd4YEVuCdrAuvsAAAAAAAABLP////8AAAABAAAAAAAAAAAAAAAAAAAAAA==",
			TxMeta:          "AAAAAQAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAAA",
			TxFeeMeta:       "AAAAAA==",
			TxLedgerHistory: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABfBqsKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAdG52AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			LedgerSequence:  30521816,
			ClosedAt:        time.Date(2020, time.July, 9, 5, 28, 42, 0, time.UTC),
		},
		{
			TxEnvelope:      "AAAABQAAAABnzACGTDuJFoxqr+C8NHCe0CHFBXLi+YhhNCIILCIpcgAAAAAAABwgAAAAAgAAAACI4aa0pXFSj6qfJuIObLw/5zyugLRGYwxb7wFSr3B9eAAAAAACFPY2AAAAfQAAAAEAAAAAAAAAAAAAAABfBqt0AAAAAQAAABdITDVhQ2dvelFISVc3c1NjNVhkY2ZtUgAAAAABAAAAAQAAAAAcR0GXGO76pFs4y38vJVAanjnLg4emNun7zAx0pHcDGAAAAAIAAAAAAAAAAAAAAAAAAAAAAQIDAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==",
			TxResult:        "qH/vXusmAmnDgPLeRWqtcrWbsxWqrHd4YEVuCdrAuvsAAAAAAAABLAAAAAGof+9e6yYCacOA8t5Faq1ytZuzFaqsd3hgRW4J2sC6+wAAAAAAAABkAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAA==",
			TxMeta:          "AAAAAQAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAAA",
			TxFeeMeta:       "AAAAAA==",
			TxLedgerHistory: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABfBqsKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAdG52QAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			LedgerSequence:  30521817,
			ClosedAt:        time.Date(2020, time.July, 9, 5, 28, 42, 0, time.UTC),
		},
		{
			TxEnvelope:      "AAAAAgAAAAAcR0GXGO76pFs4y38vJVAanjnLg4emNun7zAx0pHcDGAAAAGQBpLyvsiV6gwAAAAIAAAABAAAAAAAAAAAAAAAAXwardAAAAAEAAAAFAAAACgAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAMCAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAABdITDVhQ2dvelFISVc3c1NjNVhkY2ZtUgAAAAABAAAAAQAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAAIAAAAAAAAAAAAAAAAAAAAAAQIDAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
			TxResult:        "qH/vXusmAmnDgPLeRWqtcrWbsxWqrHd4YEVuCdrAuvsAAAAAAAAAZP////8AAAABAAAAAAAAAAAAAAAAAAAAAA==",
			TxMeta:          "AAAAAQAAAAAAAAAgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAACAAAAAwAAAAAAAAAFAQIDBAUGBwgJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFVU1NEAAAAAGtY3WxokwttAx3Fu/riPvoew/C7WMK8jZONR8Hfs75zAAAAHgAAAAAAAYagAAAAAAAAA+gAAAAAAAAB9AAAAAAAAAAZAAAAAAAAAAEAAAAAAAAABQECAwQFBgcICQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABVVNTRAAAAABrWN1saJMLbQMdxbv64j76HsPwu1jCvI2TjUfB37O+cwAAAB4AAAAAAAGKiAAAAAAAAARMAAAAAAAAAfYAAAAAAAAAGgAAAAAAAAAA",
			TxFeeMeta:       "AAAAAA==",
			TxLedgerHistory: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABfBqsKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAdG52gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
			LedgerSequence:  30521818,
			ClosedAt:        time.Date(2020, time.July, 9, 5, 28, 42, 0, time.UTC),
		},
	}
	return
}
func makeLedgerTransactionTestInput() (transaction []ingest.LedgerTransaction, historyHeader []xdr.LedgerHeaderHistoryEntry, err error) {
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
