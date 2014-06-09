package grid

import (
	_ "fmt"
)

type ClientApi struct {
}

func (this *ClientApi) CreateJob(jobDef JobDefinition, jobID *JobID) (err error) {
	*jobID, err = InsertJob(jobDef)
	return err
}
