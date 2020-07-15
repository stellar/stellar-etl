package transform

import "time"

//LedgerOutput is a representation of a ledger that aligns with the BigQuery table history_ledger
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

type TransactionOutput struct {
	TransactionHash  string
	LedgerSequence   int32
	ApplicationOrder int32

	Account         string
	AccountSequence int32

	MaxFee     int64
	FeeCharged int64

	OperationCount int32
	CreatedAt      time.Time

	MemoType string
	Memo     string

	TimeBounds string
	Successful bool

	/*
		TODO: implement
			updated_at time.Time //timestamp of table entry update time
	*/
}
