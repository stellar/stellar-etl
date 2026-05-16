package cdptest

import (
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

func TransactionHash(tx ingest.LedgerTransaction) (*string, error) {
	transactionHash := utils.HashToHexString(tx.Result.TransactionHash)
	return &transactionHash, nil
}

func Account(tx ingest.LedgerTransaction) (*string, error) {
	account, err := utils.GetAccountAddressFromMuxedAccount(tx.Envelope.SourceAccount())
	if err != nil {
		return nil, err
	}

	return &account, nil

}

func TransactionEnvelope(tx ingest.LedgerTransaction) (*string, error) {
	transactionEnvelope, err := xdr.MarshalBase64(tx.Envelope)
	if err != nil {
		return nil, err
	}

	return &transactionEnvelope, nil
}
