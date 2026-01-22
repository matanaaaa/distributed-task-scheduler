package model

import "time"

type Status string

const (
	StatusQueued  Status = "queued"
	StatusRunning Status = "running"
	StatusSuccess Status = "success"
	StatusFailed  Status = "failed"
)

type Task struct {
	ID        string
	Type      string
	Priority  string
	UseDocker bool
	DependsOn string

	Status    Status
	ZipName   string
	CreatedAt time.Time
	UpdatedAt time.Time
}
