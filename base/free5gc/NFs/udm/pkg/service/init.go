package service

import (
	"context"
	"io"
	"os"
	"runtime/debug"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/nrf/NFManagement"
	udm_context "github.com/free5gc/udm/internal/context"
	"github.com/free5gc/udm/internal/logger"
	"github.com/free5gc/udm/internal/sbi"
	"github.com/free5gc/udm/internal/sbi/consumer"
	"github.com/free5gc/udm/internal/sbi/processor"
	"github.com/free5gc/udm/pkg/app"
	"github.com/free5gc/udm/pkg/factory"
	"github.com/free5gc/util/metrics"
	"github.com/free5gc/util/metrics/utils"

	//add
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

var _ app.App = &UdmApp{}

type UdmApp struct {
	udmCtx *udm_context.UDMContext
	cfg    *factory.Config

	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	metricsServer *metrics.Server
	sbiServer     *sbi.Server

	consumer  *consumer.Consumer
	processor *processor.Processor
}

func NewApp(ctx context.Context, cfg *factory.Config, tlsKeyLogPath string) (*UdmApp, error) {
	udm := &UdmApp{
		cfg: cfg,
		wg:  sync.WaitGroup{},
	}
	udm.SetLogEnable(cfg.GetLogEnable())
	udm.SetLogLevel(cfg.GetLogLevel())
	udm.SetReportCaller(cfg.GetLogReportCaller())
	udm_context.Init()

	consumer, err := consumer.NewConsumer(udm)
	if err != nil {
		return udm, err
	}
	udm.consumer = consumer

	processor, err_p := processor.NewProcessor(udm)
	if err_p != nil {
		return udm, err_p
	}
	udm.processor = processor

	udm.ctx, udm.cancel = context.WithCancel(ctx)
	udm.udmCtx = udm_context.GetSelf()

	if udm.sbiServer, err = sbi.NewServer(udm, tlsKeyLogPath); err != nil {
		return nil, err
	}

	features := map[utils.MetricTypeEnabled]bool{utils.SBI: true}
	customMetrics := make(map[utils.MetricTypeEnabled][]prometheus.Collector)
	if cfg.AreMetricsEnabled() {
		if udm.metricsServer, err = metrics.NewServer(
			getInitMetrics(cfg, features, customMetrics), tlsKeyLogPath, logger.InitLog); err != nil {
			return nil, err
		}
	}

	return udm, nil
}

func getInitMetrics(
	cfg *factory.Config,
	features map[utils.MetricTypeEnabled]bool,
	customMetrics map[utils.MetricTypeEnabled][]prometheus.Collector,
) metrics.InitMetrics {
	metricsInfo := metrics.Metrics{
		BindingIPv4: cfg.GetMetricsBindingAddr(),
		Scheme:      cfg.GetMetricsScheme(),
		Namespace:   cfg.GetMetricsNamespace(),
		Port:        cfg.GetMetricsPort(),
		Tls: metrics.Tls{
			Key: cfg.GetMetricsCertKeyPath(),
			Pem: cfg.GetMetricsCertPemPath(),
		},
	}

	return metrics.NewInitMetrics(metricsInfo, "udm", features, customMetrics)
}

func (a *UdmApp) SetLogEnable(enable bool) {
	logger.MainLog.Infof("Log enable is set to [%v]", enable)
	if enable && logger.Log.Out == os.Stderr {
		return
	} else if !enable && logger.Log.Out == io.Discard {
		return
	}

	a.cfg.SetLogEnable(enable)
	if enable {
		logger.Log.SetOutput(os.Stderr)
	} else {
		logger.Log.SetOutput(io.Discard)
	}
}

func (a *UdmApp) SetLogLevel(level string) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		logger.MainLog.Warnf("Log level [%s] is invalid", level)
		return
	}

	logger.MainLog.Infof("Log level is set to [%s]", level)
	if lvl == logger.Log.GetLevel() {
		return
	}

	a.cfg.SetLogLevel(level)
	logger.Log.SetLevel(lvl)
}

func (a *UdmApp) SetReportCaller(reportCaller bool) {
	logger.MainLog.Infof("Report Caller is set to [%v]", reportCaller)
	if reportCaller == logger.Log.ReportCaller {
		return
	}
	a.cfg.SetLogReportCaller(reportCaller)
	logger.Log.SetReportCaller(reportCaller)
}

func (a *UdmApp) Start() {
	//add
	ctx := a.ctx

	tp, err := initTracerProvider(ctx, "udm")
	if err != nil {
		logger.AppLog.Warnf("Failed to init tracer provider: %+v", err)
		// tracing 掛了不影響 AUSF 本身啟動，這裡你可以選擇 return 或是繼續跑
		// return
	}
	if tp != nil {
		defer func() {
			_ = tp.Shutdown(ctx)
		}()
	}
	logger.InitLog.Infoln("Server started")

	a.wg.Add(1)
	go a.listenShutdownEvent()

	if err := a.sbiServer.Run(context.Background(), &a.wg); err != nil {
		logger.MainLog.Fatalf("Run SBI server failed: %+v", err)
	}

	if a.cfg.AreMetricsEnabled() && a.metricsServer != nil {
		go func() {
			a.metricsServer.Run(&a.wg)
		}()
	}

	a.WaitRoutineStopped()
}

func (a *UdmApp) listenShutdownEvent() {
	defer func() {
		if p := recover(); p != nil {
			// Print stack for panic to log. Fatalf() will let program exit.
			logger.MainLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
		a.wg.Done()
	}()

	<-a.ctx.Done()
	a.terminateProcedure()
}

func (a *UdmApp) CallServerStop() {
	if a.sbiServer != nil {
		a.sbiServer.Stop()
	}
	if a.metricsServer != nil {
		a.metricsServer.Stop()
		logger.MainLog.Infof("UDM Metrics Server terminated")
	}
}

func (a *UdmApp) Terminate() {
	a.cancel()
}

func (a *UdmApp) terminateProcedure() {
	logger.MainLog.Infof("Terminating UDM...")
	a.CallServerStop()

	// deregister with NRF
	err := a.Consumer().SendDeregisterNFInstance()
	if err != nil {
		switch apiErr := err.(type) {
		case openapi.GenericOpenAPIError:
			switch errModel := apiErr.Model().(type) {
			case NFManagement.DeregisterNFInstanceError:
				pd := &errModel.ProblemDetails
				logger.InitLog.Errorf("Deregister NF instance Failed Problem[%+v]", pd)
			case error:
				logger.InitLog.Errorf("Deregister NF instance Error[%+v]", err)
			}
		case error:
			logger.InitLog.Errorf("Deregister NF instance Error[%+v]", err)
		}
	} else {
		logger.InitLog.Infof("Deregister from NRF successfully")
	}

	logger.MainLog.Infof("UDM SBI Server terminated")
}

func (a *UdmApp) WaitRoutineStopped() {
	a.wg.Wait()
	logger.MainLog.Infof("UDM App is terminated")
}

func (a *UdmApp) CancelContext() context.Context {
	return a.ctx
}

func (a *UdmApp) Config() *factory.Config {
	return a.cfg
}

func (a *UdmApp) Context() *udm_context.UDMContext {
	return a.udmCtx
}

func (a *UdmApp) Consumer() *consumer.Consumer {
	return a.consumer
}

func (a *UdmApp) Processor() *processor.Processor {
	return a.processor
}

func initTracerProvider(ctx context.Context, serviceName string) (*sdktrace.TracerProvider, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return nil, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT not set")
	}

	// HTTP OTLP client，endpoint 例如 "tempo:4318"
	client := otlptracehttp.NewClient(
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)

	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(
		ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	return tp, nil
}
