package transform

import "time"

//LedgerOutput is a representation of a ledger that aligns with the BigQuery table history_ledgers
type LedgerOutput struct {
	Sequence int32 //sequence number of the ledger

	LedgerHash         string
	PreviousLedgerHash string
	LedgerHeader       []byte //base 64 encoding of the ledger header

	TransactionCount int32

	OperationCount             int32 //counts only operations that were a part of successful transactions
	SuccessfulTransactionCount int32
	FailedTransactionCount     int32

	TxSetOperationCount string //counts all operations, even those that are part of failed transactions

	ClosedAt time.Time //UTC timestamp

	TotalCoins      int64
	FeePool         int64
	BaseFee         int32
	BaseReserve     int32
	MaxTxSetSize    int32
	ProtocolVersion int32

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
