package base

type (
	// Transaction id
	TransactionId uint64

	CommitSequenceNumber uint64
)

const (
	InvalidTid TransactionId = 0

	InvalidCsn CommitSequenceNumber = 0
)
