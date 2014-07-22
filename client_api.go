package grid

import (
	_ "fmt"
	"github.com/ant0ine/go-json-rest/rest"
	"github.com/braintree/manners"
	"github.com/golang/glog"
	"log"
	//	"net/http"
)

/*
type ClientApi struct {
}

func (this *ClientApi) CreateJob(jobDef JobDefinition, jobID *JobID) (err error) {
	// TODO: consider how to extract client IP/port, client hostname, and potentially
	// username if we use a secure transport. Or maybe this is too much of a hassle;
	// we could make the client lib supply this instead...
	*jobID, err = CreateJob(&jobDef)
	return err
}
*/

type clientApi struct {
	server *manners.GracefulServer
}

func (this *clientApi) CreateJob(w rest.ResponseWriter, r *rest.Request) {
	log.Print("CreateJob")
	w.WriteJson(1)
}

/*
func waitForShutdown(server *manners.GracefulServer) {
	shutdown := <- Model.ShutdownFlag
	server <-
}
*/

var api *clientApi

func init() {
	api = new(clientApi)
}

func StartClientApi() {
	handler := rest.ResourceHandler{
		EnableRelaxedContentType: true,
	}
	//api := clientApi{}
	err := handler.SetRoutes(
		rest.RouteObjectMethod("POST", "/jobs", api, "CreateJob"),
	)
	if err != nil {
		log.Fatal(err)
	}
	api.server = manners.NewServer()
	glog.Info("client api starting")
	api.server.ListenAndServe(":8080", &handler)
}

func ShutdownClientApi() {
	glog.Info("shutting down client api...")
	api.server.Shutdown <- true
	glog.Info("client api stopped")
}
