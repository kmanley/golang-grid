package grid

import (
	_ "fmt"
)

type ClientApi struct {
}

func (this *ClientApi) CreateJob(jobDef JobDefinition, jobID *JobID) (err error) {
	// TODO: consider how to extract client IP/port, client hostname, and potentially
	// username if we use a secure transport. Or maybe this is too much of a hassle;
	// we could make the client lib supply this instead...
	*jobID, err = CreateJob(&jobDef)
	return err
}
