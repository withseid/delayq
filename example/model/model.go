package model

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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

	time.Sleep(time.Second * 4)

	fmt.Println("DeletedSpace: ", ds)
	return nil
}
