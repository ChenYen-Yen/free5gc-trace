package service

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/nrf/NFManagement"
	smf_context "github.com/free5gc/smf/internal/context"
	"github.com/free5gc/smf/internal/logger"
	"github.com/free5gc/smf/internal/sbi"
	"github.com/free5gc/smf/internal/sbi/consumer"
	"github.com/free5gc/smf/internal/sbi/processor"
	"github.com/free5gc/smf/pkg/app"
	"github.com/free5gc/smf/pkg/factory"
	"github.com/free5gc/util/metrics"
	"github.com/free5gc/util/metrics/utils"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

type SmfAppInterface interface {
	app.App

	Consumer() *consumer.Consumer
	Processor() *processor.Processor
}

var SMF SmfAppInterface

type SmfApp struct {
	SmfAppInterface

	cfg    *factory.Config
	smfCtx *smf_context.SMFContext
	ctx    context.Context
	cancel context.CancelFunc

	sbiServer     *sbi.Server
	metricsServer *metrics.Server
	consumer      *consumer.Consumer
	processor     *processor.Processor
	wg            sync.WaitGroup

	pfcpStart     func(*SmfApp)
	pfcpTerminate func()
}

func GetApp() SmfAppInterface {
	return SMF
}

func NewApp(
	ctx context.Context, cfg *factory.Config, tlsKeyLogPath string,
	pfcpStart func(*SmfApp), pfcpTerminate func(),
) (*SmfApp, error) {
	smf_context.Init()
	smf := &SmfApp{
		cfg:           cfg,
		wg:            sync.WaitGroup{},
		pfcpStart:     pfcpStart,
		pfcpTerminate: pfcpTerminate,
		smfCtx:        smf_context.GetSelf(),
	}
	smf.SetLogEnable(cfg.GetLogEnable())
	smf.SetLogLevel(cfg.GetLogLevel())
	smf.SetReportCaller(cfg.GetLogReportCaller())

	// Initialize consumer
	consumer, err := consumer.NewConsumer(smf)
	if err != nil {
		return nil, err
	}
	smf.consumer = consumer

	// Initialize processor
	processor, err := processor.NewProcessor(smf)
	if err != nil {
		return nil, err
	}
	smf.processor = processor

	// TODO: Initialize sbi server
	sbiServer, err := sbi.NewServer(smf, tlsKeyLogPath)
	if err != nil {
		return nil, err
	}
	smf.sbiServer = sbiServer

	features := map[utils.MetricTypeEnabled]bool{utils.SBI: true}
	customMetrics := make(map[utils.MetricTypeEnabled][]prometheus.Collector)
	if cfg.AreMetricsEnabled() {
		if smf.metricsServer, err = metrics.NewServer(
			getInitMetrics(cfg, features, customMetrics), tlsKeyLogPath, logger.InitLog); err != nil {
			return nil, err
		}
	}

	smf.ctx, smf.cancel = context.WithCancel(ctx)

	// for PFCP
	smfContext := smf_context.GetSelf()
	smfContext.PfcpContext, smfContext.PfcpCancelFunc = context.WithCancel(smf.ctx)

	SMF = smf

	return smf, nil
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

	return metrics.NewInitMetrics(metricsInfo, "smf", features, customMetrics)
}

func (a *SmfApp) Config() *factory.Config {
	return a.cfg
}

func (a *SmfApp) Context() *smf_context.SMFContext {
	return a.smfCtx
}

func (a *SmfApp) CancelContext() context.Context {
	return a.ctx
}

func (a *SmfApp) Consumer() *consumer.Consumer {
	return a.consumer
}

func (a *SmfApp) Processor() *processor.Processor {
	return a.processor
}

func (a *SmfApp) SetLogEnable(enable bool) {
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

func (a *SmfApp) SetLogLevel(level string) {
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

func (a *SmfApp) SetReportCaller(reportCaller bool) {
	logger.MainLog.Infof("Report Caller is set to [%v]", reportCaller)
	if reportCaller == logger.Log.ReportCaller {
		return
	}

	a.cfg.SetLogReportCaller(reportCaller)
	logger.Log.SetReportCaller(reportCaller)
}

func (a *SmfApp) Start() {
	ctx := a.ctx

	logger.MainLog.Infoln("Initializing OpenTelemetry tracer...")
	tp, err := initTracerProvider(ctx, "smf")
	if err != nil {
		logger.MainLog.Warnf("Failed to init tracer provider: %+v", err)
		tp = nil
	} else {
		logger.MainLog.Infoln("OpenTelemetry tracer initialized successfully")
		defer func() {
			if err := tp.Shutdown(context.Background()); err != nil {
				logger.MainLog.Warnf("Error shutting down tracer provider: %+v", err)
			}
		}()
	}

	logger.InitLog.Infoln("Server started")

	err = a.sbiServer.Run(context.Background(), &a.wg)
	if err != nil {
		logger.MainLog.Errorf("sbi server run error %+v", err)
	}

	a.wg.Add(1)
	go a.listenShutDownEvent()

	if a.cfg.AreMetricsEnabled() && a.metricsServer != nil {
		go func() {
			a.metricsServer.Run(&a.wg)
		}()
	}

	// Initialize PFCP server
	a.pfcpStart(a)

	a.WaitRoutineStopped()
}

func (a *SmfApp) listenShutDownEvent() {
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

func (a *SmfApp) Terminate() {
	a.cancel()
}

func (a *SmfApp) terminateProcedure() {
	logger.MainLog.Infof("Terminating SMF...")
	a.pfcpTerminate()
	// deregister with NRF
	err := a.Consumer().SendDeregisterNFInstance()
	if err != nil {
		switch apiErr := err.(type) {
		case openapi.GenericOpenAPIError:
			switch errModel := apiErr.Model().(type) {
			case NFManagement.DeregisterNFInstanceError:
				pd := &errModel.ProblemDetails
				logger.MainLog.Errorf("Deregister NF instance Failed Problem[%+v]", pd)
			case error:
				logger.MainLog.Errorf("Deregister NF instance Error[%+v]", err)
			}
		case error:
			logger.MainLog.Errorf("Deregister NF instance Error[%+v]", err)
		}
	} else {
		logger.MainLog.Infof("Deregister from NRF successfully")
	}

	a.sbiServer.Stop()
	logger.MainLog.Infof("SMF SBI Server terminated")
	if a.metricsServer != nil {
		a.metricsServer.Stop()
		logger.MainLog.Infof("SMF Metrics Server terminated")
	}
}

func (a *SmfApp) WaitRoutineStopped() {
	a.wg.Wait()
	logger.MainLog.Infof("SMF App is terminated")
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
