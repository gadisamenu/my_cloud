package model

import "time"

type Object struct {
    BucketId string
    Key string
    Version int
    Size int64
    Chunks []ChunkRef
    CreatedAt time.Time
}