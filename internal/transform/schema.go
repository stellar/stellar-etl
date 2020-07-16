package transform

import "time"

//LedgerOutput is a representation of a ledger that aligns with the BigQuery table history_ledgers
type LedgerOutput struct {
	Sequence                   int32     `json:"sequence"` //sequence number of the ledger
	LedgerHash                 string    `json:"ledger_hash"`
	PreviousLedgerHash         string    `json:"previous_ledger_hash"`
	LedgerHeader               []byte    `json:"ledger_header"` //base 64 encoding of the ledger header
	TransactionCount           int32     `json:"transaction_count"`
	OperationCount             int32     `json:"operation_count"` //counts only operations that were a part of successful transactions
	SuccessfulTransactionCount int32     `json:"successful_transaction_count"`
	FailedTransactionCount     int32     `json:"failed_transaction_count"`
	TxSetOperationCount        string    `json:"tx_set_operation_count"` //counts all operations, even those that are part of failed transactions
	ClosedAt                   time.Time `json:"closed_at"`              //UTC timestamp
	TotalCoins                 int64     `json:"total_coins"`
	FeePool                    int64     `json:"fee_pool"`
	BaseFee                    int32     `json:"base_fee"`
	BaseReserve                int32     `json:"base_reserve"`
	MaxTxSetSize               int32     `json:"max_tx_set_size"`
	ProtocolVersion            int32     `json:"protocol_version"`

	/*
		TODO: implement these three fields
			CreatedAt time.Time //timestamp of table entry creation time
			UpdatedAt time.Time //timestamp of table entry update time
			ImporterVersion int32 //version of the ingestion system
	*/
}

//TransactionOutput is a representation of a transaction that aligns with the BigQuery table history_transactions
type TransactionOutput struct {
	TransactionHash  string    `json:"transaction_hash"`
	LedgerSequence   int32     `json:"ledger_sequence"`
	ApplicationOrder int32     `json:"application_order"`
	Account          string    `json:"account"`
	AccountSequence  int64     `json:"account_sequence"`
	MaxFee           int64     `json:"max_fee"`
	FeeCharged       int64     `json:"fee_charged"`
	OperationCount   int32     `json:"operation_count"`
	CreatedAt        time.Time `json:"created_at"`
	MemoType         string    `json:"memo_type"`
	Memo             string    `json:"memo"`
	TimeBounds       string    `json:"time_bounds"`
	Successful       bool      `json:"successful"`

	/*
		TODO: implement
			updated_at time.Time //timestamp of table entry update time
	*/
}
