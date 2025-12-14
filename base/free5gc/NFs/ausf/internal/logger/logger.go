package logger

import (
	"context"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"

	logger_util "github.com/free5gc/util/logger"
)

var (
	Log          *logrus.Logger
	NfLog        *logrus.Entry
	MainLog      *logrus.Entry
	InitLog      *logrus.Entry
	CfgLog       *logrus.Entry
	CtxLog       *logrus.Entry
	SBILog       *logrus.Entry
	GinLog       *logrus.Entry
	ConsumerLog  *logrus.Entry
	UeAuthLog    *logrus.Entry
	Auth5gAkaLog *logrus.Entry
	AuthELog     *logrus.Entry
	UtilLog      *logrus.Entry
	AppLog       *logrus.Entry
)

func init() {
	fieldsOrder := []string{
		logger_util.FieldNF,
		logger_util.FieldCategory,
	}

	Log = logger_util.New(fieldsOrder)
	// use JSON formatter so structured fields (trace_id/span_id) appear as JSON keys
	Log.SetFormatter(&logrus.JSONFormatter{})
	NfLog = Log.WithField(logger_util.FieldNF, "AUSF")
	MainLog = NfLog.WithField(logger_util.FieldCategory, "Main")
	InitLog = NfLog.WithField(logger_util.FieldCategory, "Init")
	CfgLog = NfLog.WithField(logger_util.FieldCategory, "CFG")
	CtxLog = NfLog.WithField(logger_util.FieldCategory, "CTX")
	SBILog = NfLog.WithField(logger_util.FieldCategory, "SBI")
	GinLog = NfLog.WithField(logger_util.FieldCategory, "GIN")
	ConsumerLog = NfLog.WithField(logger_util.FieldCategory, "Consumer")
	UeAuthLog = NfLog.WithField(logger_util.FieldCategory, "UeAuth")
	Auth5gAkaLog = NfLog.WithField(logger_util.FieldCategory, "5gAka")
	AuthELog = NfLog.WithField(logger_util.FieldCategory, "Eap")
	UtilLog = NfLog.WithField(logger_util.FieldCategory, "Util")
	AppLog = NfLog.WithField(logger_util.FieldCategory, "AppLog")
}

func WithTraceContext(ctx context.Context, base *logrus.Entry) *logrus.Entry {
	if ctx == nil || base == nil {
		return base
	}

	span := trace.SpanFromContext(ctx)
	sc := span.SpanContext()
	if !sc.IsValid() {
		return base
	}

	return base.WithFields(logrus.Fields{
		"trace_id": sc.TraceID().String(),
		"span_id":  sc.SpanID().String(),
	})
}
