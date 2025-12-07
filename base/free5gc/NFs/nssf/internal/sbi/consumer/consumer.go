package consumer

import (
	"github.com/free5gc/nssf/pkg/app"
	"github.com/free5gc/openapi/nrf/NFManagement"
	sbi_metrics "github.com/free5gc/util/metrics/sbi"

	//add
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type Consumer struct {
	app.NssfApp

	*NrfService
}

func NewConsumer(nssf app.NssfApp) *Consumer {
	configuration := NFManagement.NewConfiguration()
	configuration.SetBasePath(nssf.Context().NrfUri)
	configuration.SetMetrics(sbi_metrics.SbiMetricHook)

	//add: 使用帶有 OTel 的 HTTP client
	configuration.SetHTTPClient(newOtelHTTPClient())

	nrfService := &NrfService{
		nrfNfMgmtClient: NFManagement.NewAPIClient(configuration),
	}

	return &Consumer{
		NssfApp:    nssf,
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
