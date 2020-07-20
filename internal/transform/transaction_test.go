package transform

import (
	"fmt"
	"testing"
	"time"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
	"github.com/stretchr/testify/assert"
)

var genericSourceAccount, _ = xdr.NewMuxedAccount(xdr.CryptoKeyTypeKeyTypeEd25519, xdr.Uint256([32]byte{}))
var genericOperation = xdr.Operation{
	SourceAccount: &genericSourceAccount,
	Body: xdr.OperationBody{
		Type:           xdr.OperationTypeBumpSequence,
		BumpSequenceOp: &xdr.BumpSequenceOp{},
	},
}
var genericEnvelope = xdr.TransactionV1Envelope{
	Tx: xdr.Transaction{
		SourceAccount: genericSourceAccount,
		Memo:          xdr.Memo{},
		Operations: []xdr.Operation{
			genericOperation,
		},
	},
}
var genericLedgerTransaction = ingestio.LedgerTransaction{
	Index: 1,
	Envelope: xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1:   &genericEnvelope,
	},
	Result: utils.CreateSampleResultMeta(true, 10).Result,
}
var genericLedgerHeaderHistoryEntry = xdr.LedgerHeaderHistoryEntry{}

func TestTransformTransaction(t *testing.T) {
	type inputStruct struct {
		transaction   ingestio.LedgerTransaction
		historyHeader xdr.LedgerHeaderHistoryEntry
	}
	type transformTest struct {
		input      inputStruct
		wantOutput TransactionOutput
		wantErr    error
	}
	genericInput := inputStruct{genericLedgerTransaction, genericLedgerHeaderHistoryEntry}
	negativeSeqInput := genericInput
	negativeSeqEnvelope := genericEnvelope
	negativeSeqEnvelope.Tx.SeqNum = xdr.SequenceNumber(-1)
	negativeSeqInput.transaction.Envelope.V1 = &negativeSeqEnvelope

	badTimeboundInput := genericInput
	badTimeboundEnvelope := genericEnvelope
	badTimeboundEnvelope.Tx.TimeBounds = &xdr.TimeBounds{
		MinTime: 1594586912,
		MaxTime: 100,
	}
	badTimeboundInput.transaction.Envelope.V1 = &badTimeboundEnvelope

	badFeeChargedInput := genericInput
	badFeeChargedInput.transaction.Result.Result.FeeCharged = -1

	hardCodedTransaction, hardCodedLedgerHeader, err := prepareHardcodedTransactionTestInput()
	assert.NoError(t, err)
	hardCodedInput := inputStruct{hardCodedTransaction, hardCodedLedgerHeader}
	hardCodedOutput, err := prepareHardcodedTransactionTestOutput()
	assert.NoError(t, err)

	tests := []transformTest{
		transformTest{
			negativeSeqInput,
			TransactionOutput{},
			fmt.Errorf("The account sequence number (-1) is negative for ledger 0; transaction 1"),
		},
		{
			badFeeChargedInput,
			TransactionOutput{},
			fmt.Errorf("The fee charged (-1) is negative for ledger 0; transaction 1"),
		},
		{
			badTimeboundInput,
			TransactionOutput{},
			fmt.Errorf("The max time is earlier than the min time (100 < 1594586912) for ledger 0; transaction 1"),
		},
		{
			hardCodedInput,
			hardCodedOutput,
			nil,
		},
	}

	for _, test := range tests {
		actualOutput, actualError := TransformTransaction(test.input.transaction, test.input.historyHeader)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func prepareHardcodedTransactionTestOutput() (output TransactionOutput, err error) {
	correctTime, err := time.Parse("2006-1-2 15:04:05 MST", "2020-07-09 05:28:42 UTC")
	output = TransactionOutput{
		TransactionHash:  "a87fef5eeb260269c380f2de456aad72b59bb315aaac777860456e09dac0bafb",
		LedgerSequence:   30521816,
		ApplicationOrder: 1,
		Account:          "GCEODJVUUVYVFD5KT4TOEDTMXQ76OPFOQC2EMYYMLPXQCUVPOB6XRWPQ",
		AccountSequence:  112351890582290871,
		MaxFee:           90000,
		FeeCharged:       300,
		OperationCount:   1,
		CreatedAt:        correctTime,
		MemoType:         "MemoTypeMemoText",
		Memo:             "HL5aCgozQHIW7sSc5XdcfmR",
		TimeBounds:       "[0, 1594272628)",
		Successful:       false,
	}
	return
}
func prepareHardcodedTransactionTestInput() (transaction ingestio.LedgerTransaction, historyHeader xdr.LedgerHeaderHistoryEntry, err error) {
	hardCodedTransactionSourceAccount, err := xdr.NewMuxedAccount(xdr.CryptoKeyTypeKeyTypeEd25519, xdr.Uint256([32]byte{0x88, 0xe1, 0xa6, 0xb4, 0xa5, 0x71, 0x52, 0x8f, 0xaa, 0x9f, 0x26, 0xe2, 0xe, 0x6c, 0xbc, 0x3f, 0xe7, 0x3c, 0xae, 0x80, 0xb4, 0x46, 0x63, 0xc, 0x5b, 0xef, 0x1, 0x52, 0xaf, 0x70, 0x7d, 0x78}))
	if err != nil {
		return
	}
	
	hardCodedOperationSourceAccount, err := xdr.NewMuxedAccount(xdr.CryptoKeyTypeKeyTypeEd25519, xdr.Uint256([32]byte{0x1c, 0x47, 0x41, 0x97, 0x18, 0xee, 0xfa, 0xa4, 0x5b, 0x38, 0xcb, 0x7f, 0x2f, 0x25, 0x50, 0x1a, 0x9e, 0x39, 0xcb, 0x83, 0x87, 0xa6, 0x36, 0xe9, 0xfb, 0xcc, 0xc, 0x74, 0xa4, 0x77, 0x3, 0x18}))
	if err != nil {
		return
	}

	hardCodedMemoText := "HL5aCgozQHIW7sSc5XdcfmR"
	hardCodedTransactionHash := xdr.Hash([32]byte{0xa8, 0x7f, 0xef, 0x5e, 0xeb, 0x26, 0x2, 0x69, 0xc3, 0x80, 0xf2, 0xde, 0x45, 0x6a, 0xad, 0x72, 0xb5, 0x9b, 0xb3, 0x15, 0xaa, 0xac, 0x77, 0x78, 0x60, 0x45, 0x6e, 0x9, 0xda, 0xc0, 0xba, 0xfb})
	transaction = ingestio.LedgerTransaction{
		Index: 1,
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: hardCodedTransactionSourceAccount,
					SeqNum:        112351890582290871,
					Memo: xdr.Memo{
						Type: xdr.MemoTypeMemoText,
						Text: &hardCodedMemoText,
					},
					Fee: 90000,
					TimeBounds: &xdr.TimeBounds{
						MinTime: 0,
						MaxTime: 1594272628,
					},
					Operations: []xdr.Operation{
						xdr.Operation{
							SourceAccount: &hardCodedOperationSourceAccount,
							Body: xdr.OperationBody{
								Type:                       xdr.OperationTypePathPaymentStrictReceive,
								PathPaymentStrictReceiveOp: &xdr.PathPaymentStrictReceiveOp{},
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
					Code: xdr.TransactionResultCodeTxFailed,
					Results: &[]xdr.OperationResult{
						xdr.OperationResult{},
					},
				},
			},
		},
	}
	historyHeader = xdr.LedgerHeaderHistoryEntry{
		Header: xdr.LedgerHeader{
			LedgerSeq: 30521816,
			ScpValue:  xdr.StellarValue{CloseTime: 1594272522},
		},
	}
	return
}
