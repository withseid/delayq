package model

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/withseid/delayq"
)

// const DeletedSpaceTopic = "deleted_space"

type DeletedSpace struct {
	SpaceID string
}

func (d *DeletedSpace) Topic() string {
	return "deleted_space"
}

func (d *DeletedSpace) Execute(ctx context.Context, job *delayq.Job) error {
	ds := DeletedSpace{}
	err := json.Unmarshal(job.Boday, &ds)
	if err != nil {
		return err
	}
	fmt.Println("DeletedSpace job info: ", job)

	return nil
}
