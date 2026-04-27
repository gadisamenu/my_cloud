package persistence

type DiskStorage struct {
	logFile string
	stateFile string
	snapshotFile string
}

func NewDistStorage(logFile string, stateFile string, snapshotFile string) *DiskStorage {
	return &DiskStorage{
		logFile: logFile,
		stateFile: stateFile,
		snapshotFile: snapshotFile,
	}
}