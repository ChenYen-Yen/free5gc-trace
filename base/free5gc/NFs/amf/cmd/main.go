package main

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"syscall"

	//add
	"log"

	"github.com/urfave/cli/v2"

	"github.com/free5gc/amf/internal/logger"
	"github.com/free5gc/amf/pkg/factory"
	"github.com/free5gc/amf/pkg/service"
	logger_util "github.com/free5gc/util/logger"
	"github.com/free5gc/util/version"

	//add
	"go.opentelemetry.io/otel"
	//"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	//"google.golang.org/grpc"
)

var AMF *service.AmfApp

func main() {
	//add
	shutdown := initTracer()
	defer func() {
		_ = shutdown(context.Background())
	}()

	{
		tracer := otel.Tracer("amf-main")
		ctx, span := tracer.Start(context.Background(), "AMF Startup Test")
		span.AddEvent("AMF startup span test")
		span.End()

		// 強制 flush 一次（可選，但 debug 時很好用）
		if tp, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); ok {
			_ = tp.ForceFlush(ctx)
		}
	}

	defer func() {
		if p := recover(); p != nil {
			// Print stack for panic to log. Fatalf() will let program exit.
			logger.MainLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
	}()

	app := cli.NewApp()
	app.Name = "amf"
	app.Usage = "5G Access and Mobility Management Function (AMF)"
	app.Action = action
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Usage:   "Load configuration from `FILE`",
		},
		&cli.StringSliceFlag{
			Name:    "log",
			Aliases: []string{"l"},
			Usage:   "Output NF log to `FILE`",
		},
	}
	if err := app.Run(os.Args); err != nil {
		logger.MainLog.Errorf("AMF Run error: %v\n", err)
		return
	}
}

func action(cliCtx *cli.Context) error {
	tlsKeyLogPath, err := initLogFile(cliCtx.StringSlice("log"))
	if err != nil {
		return err
	}

	logger.MainLog.Infoln("AMF version: ", version.GetVersion())

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh  // Wait for interrupt signal to gracefully shutdown
		cancel() // Notify each goroutine and wait them stopped
	}()

	cfg, err := factory.ReadConfig(cliCtx.String("config"))
	if err != nil {
		return err
	}
	factory.AmfConfig = cfg

	amf, err := service.NewApp(ctx, cfg, tlsKeyLogPath)
	if err != nil {
		return err
	}
	AMF = amf

	amf.Start()

	return nil
}

func initLogFile(logNfPath []string) (string, error) {
	logTlsKeyPath := ""

	for _, path := range logNfPath {
		if err := logger_util.LogFileHook(logger.Log, path); err != nil {
			return "", err
		}

		if logTlsKeyPath != "" {
			continue
		}

		nfDir, _ := filepath.Split(path)
		tmpDir := filepath.Join(nfDir, "key")
		if err := os.MkdirAll(tmpDir, 0o775); err != nil {
			logger.InitLog.Errorf("Make directory %s failed: %+v", tmpDir, err)
			return "", err
		}
		_, name := filepath.Split(factory.AmfDefaultTLSKeyLogPath)
		logTlsKeyPath = filepath.Join(tmpDir, name)
	}

	return logTlsKeyPath, nil
}

// add
func initTracer() func(context.Context) error {
	logger.AppLog.Infoln("Tracer start")

	// OTLP endpoint 可以用環境變數控制，或寫死
	// 現在使用 console exporter（stdouttrace），這個 endpoint 暫時只作為參考或日後切回 OTLP 使用。
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4317" // 例如接 OTel Collector
	}
	// 這行只是為了讓 endpoint 不會變成未使用變數，同時在 log 裡保留資訊：
	log.Printf("OTEL_EXPORTER_OTLP_ENDPOINT (for future OTLP use): %s", endpoint)

	// exporter, err := otlptracegrpc.New(
	// 	context.Background(),
	// 	otlptracegrpc.WithEndpoint(endpoint),
	// 	otlptracegrpc.WithInsecure(),
	// 	otlptracegrpc.WithDialOption(grpc.WithBlock()),
	// )

	// 改成使用 console exporter，把 trace 直接印到 stdout
	exporter, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(), // 輸出格式比較好讀
	)
	if err != nil {
		log.Fatalf("failed to create stdout trace exporter: %v", err)
	}

	// service 資訊
	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String("free5gc-amf"),
		),
	)
	if err != nil {
		log.Fatalf("failed to create resource: %v", err)
	}

	// TracerProvider（可調整 sample ratio）
	tp := sdktrace.NewTracerProvider(
		//sdktrace.WithBatcher(exporter),
		sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter)),
		sdktrace.WithResource(res),
		// sdktrace.WithSampler(sdktrace.TraceIDRatioBased(0.1)), // 例如只抽樣 10%
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)

	return tp.Shutdown
}
