package pgbarrel

type ReplicationOperation struct {
	Position              string
	Operation, Target     string
	OldColumns, OldValues []string
	NewColumns, NewValues []string
}
