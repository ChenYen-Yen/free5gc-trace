package service

import (
	"context"
	"io"
	"os"
	"runtime/debug"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	ausf_context "github.com/free5gc/ausf/internal/context"
	"github.com/free5gc/ausf/internal/logger"
	"github.com/free5gc/ausf/internal/sbi"
	"github.com/free5gc/ausf/internal/sbi/consumer"
	"github.com/free5gc/ausf/internal/sbi/processor"
	"github.com/free5gc/ausf/pkg/app"
	"github.com/free5gc/ausf/pkg/factory"
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

var AUSF *AusfApp

var _ app.App = &AusfApp{}

type AusfApp struct {
	ausfCtx *ausf_context.AUSFContext
	cfg     *factory.Config

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	sbiServer     *sbi.Server
	metricsServer *metrics.Server
	consumer      *consumer.Consumer
	processor     *processor.Processor
}

func NewApp(ctx context.Context, cfg *factory.Config, tlsKeyLogPath string) (*AusfApp, error) {
	ausf := &AusfApp{
		cfg: cfg,
		wg:  sync.WaitGroup{},
	}
	ausf.SetLogEnable(cfg.GetLogEnable())
	ausf.SetLogLevel(cfg.GetLogLevel())
	ausf.SetReportCaller(cfg.GetLogReportCaller())
	ausf_context.Init()

	processor, err_p := processor.NewProcessor(ausf)
	if err_p != nil {
		return ausf, err_p
	}
	ausf.processor = processor

	consumer, err := consumer.NewConsumer(ausf)
	if err != nil {
		return ausf, err
	}
	ausf.consumer = consumer

	ausf.ctx, ausf.cancel = context.WithCancel(ctx)
	ausf.ausfCtx = ausf_context.GetSelf()

	if ausf.sbiServer, err = sbi.NewServer(ausf, tlsKeyLogPath); err != nil {
		return nil, err
	}

	features := map[utils.MetricTypeEnabled]bool{utils.SBI: true}
	customMetrics := make(map[utils.MetricTypeEnabled][]prometheus.Collector)
	if cfg.AreMetricsEnabled() {
		if ausf.metricsServer, err = metrics.NewServer(
			getInitMetrics(cfg, features, customMetrics), tlsKeyLogPath, logger.InitLog); err != nil {
			return nil, err
		}
	}

	AUSF = ausf

	return ausf, nil
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

	return metrics.NewInitMetrics(metricsInfo, "ausf", features, customMetrics)
}

func (a *AusfApp) CancelContext() context.Context {
	return a.ctx
}

func (a *AusfApp) Consumer() *consumer.Consumer {
	return a.consumer
}

func (a *AusfApp) Processor() *processor.Processor {
	return a.processor
}

func (a *AusfApp) Context() *ausf_context.AUSFContext {
	return a.ausfCtx
}

func (a *AusfApp) Config() *factory.Config {
	return a.cfg
}

func (a *AusfApp) SetLogEnable(enable bool) {
	logger.MainLog.Infof("Log enable is set to [%v]", enable)
	if enable && logger.Log.Out == os.Stderr {
		return
	} else if !enable && logger.Log.Out == io.Discard {
		return
	}

	a.Config().SetLogEnable(enable)
	if enable {
		logger.Log.SetOutput(os.Stderr)
	} else {
		logger.Log.SetOutput(io.Discard)
	}
}

func (a *AusfApp) SetLogLevel(level string) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		logger.MainLog.Warnf("Log level [%s] is invalid", level)
		return
	}

	logger.MainLog.Infof("Log level is set to [%s]", level)
	if lvl == logger.Log.GetLevel() {
		return
	}

	a.Config().SetLogLevel(level)
	logger.Log.SetLevel(lvl)
}

func (a *AusfApp) SetReportCaller(reportCaller bool) {
	logger.MainLog.Infof("Report Caller is set to [%v]", reportCaller)
	if reportCaller == logger.Log.ReportCaller {
		return
	}

	a.Config().SetLogReportCaller(reportCaller)
	logger.Log.SetReportCaller(reportCaller)
}

func (a *AusfApp) Start() {
	//add
	ctx := a.ctx

	tp, err := initTracerProvider(ctx, "ausf")
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

func (a *AusfApp) listenShutdownEvent() {
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

func (a *AusfApp) Terminate() {
	a.cancel()
}

func (a *AusfApp) terminateProcedure() {
	logger.MainLog.Infof("Terminating AUSF...")
	a.CallServerStop()

	// deregister with NRF
	problemDetails, err := a.Consumer().SendDeregisterNFInstance()
	if problemDetails != nil {
		logger.MainLog.Errorf("Deregister NF instance Failed Problem[%+v]", problemDetails)
	} else if err != nil {
		logger.MainLog.Errorf("Deregister NF instance Error[%+v]", err)
	} else {
		logger.MainLog.Infof("Deregister from NRF successfully")
	}
	logger.MainLog.Infof("CHF SBI Server terminated")
}

func (a *AusfApp) CallServerStop() {
	if a.sbiServer != nil {
		a.sbiServer.Shutdown()
	}
	if a.metricsServer != nil {
		a.metricsServer.Stop()
		logger.MainLog.Infof("AUSF Metrics Server terminated")
	}
}

func (a *AusfApp) WaitRoutineStopped() {
	a.wg.Wait()
	logger.MainLog.Infof("AUSF App is terminated")
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
