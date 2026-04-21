package persistence

type DiskStorage struct {
	logFile string
	stateFile string
}

func NewDistStorage(logFile string, stateFile string) *DiskStorage {
	return &DiskStorage{
		logFile: logFile,
		stateFile: stateFile,
	}
}