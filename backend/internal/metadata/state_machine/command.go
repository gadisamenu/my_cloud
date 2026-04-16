package statemachine

type CommandType string

const (
	CreateFile CommandType = "CREATE_FILE"
	DeleteFile CommandType = "DELETE_FILE"
	UpdateChunks CommandType = "UPDATE_CHUNKS"
)

type Command struct {
	Type CommandType
	Data []byte // json payload
}
