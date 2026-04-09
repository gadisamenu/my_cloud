package model

type NodeStatus int

const (
	Alive NodeStatus = iota
	Dead
)

type StorageNode struct {
	ID string
	Address string
	Capacity int64
	Used int64
	Status NodeStatus
}

