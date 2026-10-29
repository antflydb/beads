package storage

// This distribution owns its registration separately from upstream's legs.
// Native portable contracts exercise transactional behavior without SQL fixture
// statements; the remaining SQL-shaped role cases are tracked by the ratchet.
func init() {
	registerContractLeg(contractLeg{name: "antfly", wiringRoot: "internal/storage/antfly", adopting: "The native backend runs portable transaction/lease contracts and shared boundary cases; the SQL-shaped fixtures and unsupported domain roles are not adopted yet.", adoptionCeiling: 726})
}
