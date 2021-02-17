package transform

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestTransformAsset(t *testing.T) {
	type operationInput struct {
		operation   xdr.Operation
		index       int32
		transaction ingest.LedgerTransaction
	}
	type transformTest struct {
		input      operationInput
		wantOutput AssetOutput
		wantErr    error
	}

	nonPaymentInput := operationInput{
		operation:   genericBumpOperation,
		transaction: genericLedgerTransaction,
		index:       0,
	}

	tests := []transformTest{
		{
			input:      nonPaymentInput,
			wantOutput: AssetOutput{},
			wantErr:    fmt.Errorf("Operation of type 11 cannot issue an asset (id 4096)"),
		},
	}

	hardCodedInputTransaction, err := makeAssetTestInput()
	assert.NoError(t, err)
	hardCodedOutputArray := makeAssetTestOutput()

	for i, op := range hardCodedInputTransaction.Envelope.Operations() {
		tests = append(tests, transformTest{
			input:      operationInput{op, int32(i), hardCodedInputTransaction},
			wantOutput: hardCodedOutputArray[i],
			wantErr:    nil,
		})
	}

	for _, test := range tests {
		actualOutput, actualError := TransformAsset(test.input.operation, test.input.index, test.input.transaction, 0)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeAssetTestInput() (inputTransaction ingest.LedgerTransaction, err error) {
	inputTransaction = genericLedgerTransaction
	inputEnvelope := genericBumpOperationEnvelope

	inputEnvelope.Tx.SourceAccount = testAccount1

	inputOperations := []xdr.Operation{
		xdr.Operation{
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
		xdr.Operation{
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
		AssetOutput{
			AssetCode:   "USDT",
			AssetIssuer: "GBVVRXLMNCJQW3IDDXC3X6XCH35B5Q7QXNMMFPENSOGUPQO7WO7HGZPA",
			AssetType:   "credit_alphanum4",
			AssetID:     1229977787683536144,
		},
		AssetOutput{
			AssetCode:   "",
			AssetIssuer: "",
			AssetType:   "native",
			AssetID:     12638146518625398189,
		},
	}
	return
}
