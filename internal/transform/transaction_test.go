package transform

import (
	"fmt"
	"testing"
	"time"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

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
	negativeSeqEnvelope := genericBumpOperationEnvelope
	negativeSeqEnvelope.Tx.SeqNum = xdr.SequenceNumber(-1)
	negativeSeqInput.transaction.Envelope.V1 = &negativeSeqEnvelope

	badTimeboundInput := genericInput
	badTimeboundEnvelope := genericBumpOperationEnvelope
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
		Account:          hardCodedAccountOneAddress,
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
	hardCodedMemoText := "HL5aCgozQHIW7sSc5XdcfmR"
	hardCodedTransactionHash := xdr.Hash([32]byte{0xa8, 0x7f, 0xef, 0x5e, 0xeb, 0x26, 0x2, 0x69, 0xc3, 0x80, 0xf2, 0xde, 0x45, 0x6a, 0xad, 0x72, 0xb5, 0x9b, 0xb3, 0x15, 0xaa, 0xac, 0x77, 0x78, 0x60, 0x45, 0x6e, 0x9, 0xda, 0xc0, 0xba, 0xfb})
	transaction = ingestio.LedgerTransaction{
		Index: 1,
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{
					SourceAccount: hardCodedAccountOne,
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
							SourceAccount: &hardCodedAccountTwo,
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
