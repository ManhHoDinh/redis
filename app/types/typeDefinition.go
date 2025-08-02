package types

import "time"

type Entry struct {
	Value string
	ExpiryTime time.Time
} 
type BlockingRequest struct {
	Key     string
	Ch      chan string
	Timeout time.Duration
}
