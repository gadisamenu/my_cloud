package statemachine

import "encoding/json"

func EncodeCommand(cmd Command) []byte {
	data, _ := json.Marshal(cmd)
	return data
}

func DecodeCommand(data []byte) Command {
	var cmd Command
	json.Unmarshal(data, &cmd)
	return cmd
}