package antfly

// Regenerate the exact complement of the hand-written methods. Unsupported
// capabilities fail explicitly; embedding a nil interface would panic instead.
//go:generate go run ../unsupportedgen -type DoltStorage -pkg antfly -receiver Store -out unsupported_gen.go
//go:generate go run ../unsupportedgen -type Transaction -pkg antfly -receiver transaction -out transaction_unsupported_gen.go
