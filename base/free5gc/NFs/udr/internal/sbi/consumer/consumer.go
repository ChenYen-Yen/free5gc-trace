package consumer

import (
	"github.com/free5gc/openapi/nrf/NFManagement"
	"github.com/free5gc/udr/pkg/app"

	//add
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type Consumer struct {
	app.App

	*NrfService
}

func NewConsumer(udr app.App) *Consumer {
	configuration := NFManagement.NewConfiguration()
	configuration.SetBasePath(udr.Context().NrfUri)
	nrfService := &NrfService{
		nfMngmntClients: make(map[string]*NFManagement.APIClient),
	}

	return &Consumer{
		App:        udr,
		NrfService: nrfService,
	}
}

// add
func newOtelHTTPClient() *http.Client {
	return &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
		Timeout:   30 * time.Second,
	}
}
