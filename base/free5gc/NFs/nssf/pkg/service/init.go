/*
 * NSSF Service
 */

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

	nssf_context "github.com/free5gc/nssf/internal/context"
	"github.com/free5gc/nssf/internal/logger"
	"github.com/free5gc/nssf/internal/sbi"
	"github.com/free5gc/nssf/internal/sbi/consumer"
	"github.com/free5gc/nssf/internal/sbi/processor"
	"github.com/free5gc/nssf/pkg/app"
	"github.com/free5gc/nssf/pkg/factory"
	"github.com/free5gc/util/metrics"
	"github.com/free5gc/util/metrics/utils"

	// add
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

type NssfApp struct {
	cfg     *factory.Config
	nssfCtx *nssf_context.NSSFContext

	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	sbiServer     *sbi.Server
	metricsServer *metrics.Server
	processor     *processor.Processor
	consumer      *consumer.Consumer
}

var _ app.NssfApp = &NssfApp{}

func NewApp(ctx context.Context, cfg *factory.Config, tlsKeyLogPath string) (*NssfApp, error) {
	nssf_context.InitNssfContext()

	nssf := &NssfApp{
		cfg:     cfg,
		wg:      sync.WaitGroup{},
		nssfCtx: nssf_context.GetSelf(),
	}
	nssf.SetLogEnable(cfg.GetLogEnable())
	nssf.SetLogLevel(cfg.GetLogLevel())
	nssf.SetReportCaller(cfg.GetLogReportCaller())

	nssf.ctx, nssf.cancel = context.WithCancel(ctx)

	processor := processor.NewProcessor(nssf)
	nssf.processor = processor

	consumer := consumer.NewConsumer(nssf)
	nssf.consumer = consumer

	sbiServer := sbi.NewServer(nssf, tlsKeyLogPath)
	nssf.sbiServer = sbiServer

	features := map[utils.MetricTypeEnabled]bool{utils.SBI: true}
	customMetrics := make(map[utils.MetricTypeEnabled][]prometheus.Collector)
	if cfg.AreMetricsEnabled() {
		var err error
		if nssf.metricsServer, err = metrics.NewServer(
			getInitMetrics(cfg, features, customMetrics), tlsKeyLogPath, logger.InitLog); err != nil {
			return nil, err
		}
	}

	return nssf, nil
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

	return metrics.NewInitMetrics(metricsInfo, "NSSF", features, customMetrics)
}

func (a *NssfApp) Config() *factory.Config {
	return a.cfg
}

func (a *NssfApp) Context() *nssf_context.NSSFContext {
	return a.nssfCtx
}

func (a *NssfApp) Processor() *processor.Processor {
	return a.processor
}

func (a *NssfApp) Consumer() *consumer.Consumer {
	return a.consumer
}

func (a *NssfApp) SetLogEnable(enable bool) {
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

func (a *NssfApp) SetLogLevel(level string) {
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

func (a *NssfApp) SetReportCaller(reportCaller bool) {
	logger.MainLog.Infof("Report Caller is set to [%v]", reportCaller)
	if reportCaller == logger.Log.ReportCaller {
		return
	}

	a.cfg.SetLogReportCaller(reportCaller)
	logger.Log.SetReportCaller(reportCaller)
}

func (a *NssfApp) registerToNrf(ctx context.Context) error {
	nssfContext := a.nssfCtx

	var err error
	_, nssfContext.NfId, err = a.consumer.SendRegisterNFInstance(ctx, nssfContext)
	if err != nil {
		return fmt.Errorf("failed to register NSSF to NRF: %s", err.Error())
	}

	return nil
}

func (a *NssfApp) deregisterFromNrf() {
	problemDetails, err := a.consumer.SendDeregisterNFInstance(a.nssfCtx.NfId)
	if problemDetails != nil {
		logger.InitLog.Errorf("Deregister NF instance Failed Problem[%+v]", problemDetails)
	} else if err != nil {
		logger.InitLog.Errorf("Deregister NF instance Error[%+v]", err)
	} else {
		logger.InitLog.Infof("Deregister from NRF successfully")
	}
}

func (a *NssfApp) Start() {
	//add
	ctx := a.ctx

	tp, tpErr := initTracerProvider(ctx, "ausf")
	if tpErr != nil {
		logger.AppLog.Warnf("Failed to init tracer provider: %+v", tpErr)
		// tracing 掛了不影響 AUSF 本身啟動，這裡你可以選擇 return 或是繼續跑
		// return
	}
	if tp != nil {
		defer func() {
			_ = tp.Shutdown(ctx)
		}()
	}

	err := a.registerToNrf(a.ctx)
	if err != nil {
		logger.MainLog.Errorf("register to NRF failed: %+v", err)
	} else {
		logger.MainLog.Infoln("register to NRF successfully")
	}

	// Graceful deregister when panic
	defer func() {
		if p := recover(); p != nil {
			a.deregisterFromNrf()
			logger.InitLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
	}()

	a.sbiServer.Run(&a.wg)

	if a.cfg.AreMetricsEnabled() && a.metricsServer != nil {
		go func() {
			a.metricsServer.Run(&a.wg)
		}()
	}

	go a.listenShutdown(a.ctx)
	a.Wait()
}

func (a *NssfApp) listenShutdown(ctx context.Context) {
	<-ctx.Done()
	a.terminateProcedure()
}

func (a *NssfApp) Terminate() {
	a.cancel()
}

func (a *NssfApp) terminateProcedure() {
	logger.MainLog.Infof("Terminating NSSF...")
	a.deregisterFromNrf()
	a.sbiServer.Shutdown()
	if a.metricsServer != nil {
		a.metricsServer.Stop()
		logger.MainLog.Infof("NSSF Metrics Server terminated")
	}
}

func (a *NssfApp) Wait() {
	a.wg.Wait()
	logger.MainLog.Infof("NSSF terminated")
}

func initTracerProvider(ctx context.Context, serviceName string) (*sdktrace.TracerProvider, error) {
	// 1. 建立 console trace exporter（輸出到 stdout）
	exporter, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),     // 輸出成比較好讀的 JSON
		stdouttrace.WithWriter(os.Stdout), // 預設就是 Stdout，其實可省略
	)
	if err != nil {
		return nil, err
	}

	// 2. 設定 Resource（service.name 很重要）
	res, err := resource.New(
		ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}

	// 3. 建立 TracerProvider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	// 4. 設為 global
	otel.SetTracerProvider(tp)
	// 如有需要，這裡也可以設定 Propagator（例如 W3C TraceContext）
	// otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	return tp, nil
}
