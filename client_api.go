package grid

import (
	"fmt"
)

type ClientApi struct{}

func (this *ClientApi) CreateJob(jobDef JobDefinition, id *JobID) error {
	fmt.Println("created job", jobDef)
	return nil
}
