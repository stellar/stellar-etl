package transform

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestTransformAsset(t *testing.T) {

	type assetInput struct {
		operation xdr.Operation
		index     int32
		txnIndex  int32
		// transaction xdr.TransactionEnvelope
		lcm xdr.LedgerCloseMeta
	}

	type transformTest struct {
		input      assetInput
		wantOutput AssetOutput
		wantErr    error
	}

	nonPaymentInput := assetInput{
		operation: genericBumpOperation,
		txnIndex:  0,
		index:     0,
		lcm:       genericLedgerCloseMeta,
	}

	tests := []transformTest{
		{
			input:      nonPaymentInput,
			wantOutput: AssetOutput{},
			wantErr:    fmt.Errorf("operation of type 11 cannot issue an asset (id 0)"),
		},
	}

	hardCodedInputTransaction, err := makeAssetTestInput()
	assert.NoError(t, err)
	hardCodedOutputArray := makeAssetTestOutput()

	for i, op := range hardCodedInputTransaction.Envelope.Operations() {
		tests = append(tests, transformTest{
			input: assetInput{
				operation: op,
				index:     int32(i),
				txnIndex:  int32(i),
				lcm:       genericLedgerCloseMeta},
			wantOutput: hardCodedOutputArray[i],
			wantErr:    nil,
		})
	}

	for _, test := range tests {
		actualOutput, actualError := TransformAsset(test.input.operation, test.input.index, test.input.txnIndex, 0, test.input.lcm)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeAssetTestInput() (inputTransaction ingest.LedgerTransaction, err error) {
	inputTransaction = genericLedgerTransaction
	inputEnvelope := genericBumpOperationEnvelope

	inputEnvelope.Tx.SourceAccount = testAccount1

	inputOperations := []xdr.Operation{
		{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: testAccount2,
					Asset:       usdtAsset,
					Amount:      350000000,
				},
			},
		},
		{
			SourceAccount: nil,
			Body: xdr.OperationBody{
				Type: xdr.OperationTypePayment,
				PaymentOp: &xdr.PaymentOp{
					Destination: testAccount3,
					Asset:       nativeAsset,
					Amount:      350000000,
				},
			},
		},
	}

	inputEnvelope.Tx.Operations = inputOperations
	inputTransaction.Envelope.V1 = &inputEnvelope
	return
}

func makeAssetTestOutput() (transformedAssets []AssetOutput) {
	transformedAssets = []AssetOutput{
		{
			AssetCode:      "USDT",
			AssetIssuer:    "GBVVRXLMNCJQW3IDDXC3X6XCH35B5Q7QXNMMFPENSOGUPQO7WO7HGZPA",
			AssetType:      "credit_alphanum4",
			AssetID:        -8205667356306085451,
			ClosedAt:       time.Date(1970, time.January, 1, 0, 0, 10, 0, time.UTC),
			LedgerSequence: 2,
		},
		{
			AssetCode:      "",
			AssetIssuer:    "",
			AssetType:      "native",
			AssetID:        -5706705804583548011,
			ClosedAt:       time.Date(1970, time.January, 1, 0, 0, 10, 0, time.UTC),
			LedgerSequence: 2,
		},
	}
	return
}
