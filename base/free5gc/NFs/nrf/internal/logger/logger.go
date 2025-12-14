package logger

import (
	"context"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"

	logger_util "github.com/free5gc/util/logger"
)

var (
	Log         *logrus.Logger
	NfLog       *logrus.Entry
	MainLog     *logrus.Entry
	InitLog     *logrus.Entry
	CfgLog      *logrus.Entry
	CtxLog      *logrus.Entry
	SBILog      *logrus.Entry
	GinLog      *logrus.Entry
	ConsumerLog *logrus.Entry
	NfmLog      *logrus.Entry
	AccTokenLog *logrus.Entry
	DiscLog     *logrus.Entry
	UtilLog     *logrus.Entry
	AppLog      *logrus.Entry
)

func init() {
	fieldsOrder := []string{
		logger_util.FieldNF,
		logger_util.FieldCategory,
	}

	Log = logger_util.New(fieldsOrder)
	// use JSON formatter so structured fields (trace_id/span_id) appear as JSON keys
	Log.SetFormatter(&logrus.JSONFormatter{})
	NfLog = Log.WithField(logger_util.FieldNF, "NRF")
	MainLog = NfLog.WithField(logger_util.FieldCategory, "Main")
	InitLog = NfLog.WithField(logger_util.FieldCategory, "Init")
	CfgLog = NfLog.WithField(logger_util.FieldCategory, "CFG")
	CtxLog = NfLog.WithField(logger_util.FieldCategory, "CTX")
	SBILog = NfLog.WithField(logger_util.FieldCategory, "SBI")
	GinLog = NfLog.WithField(logger_util.FieldCategory, "GIN")
	ConsumerLog = NfLog.WithField(logger_util.FieldCategory, "Consumer")
	NfmLog = NfLog.WithField(logger_util.FieldCategory, "NFM")
	AccTokenLog = NfLog.WithField(logger_util.FieldCategory, "Token")
	DiscLog = NfLog.WithField(logger_util.FieldCategory, "DISC")
	UtilLog = NfLog.WithField(logger_util.FieldCategory, "Util")
	AppLog = NfLog.WithField(logger_util.FieldCategory, "App")
}

// WithTraceContext extracts trace_id and span_id from context and binds them to the logger
func WithTraceContext(ctx context.Context, log *logrus.Entry) *logrus.Entry {
	if ctx == nil {
		return log
	}

	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return log
	}

	spanCtx := span.SpanContext()
	return log.WithFields(logrus.Fields{
		"trace_id": spanCtx.TraceID().String(),
		"span_id":  spanCtx.SpanID().String(),
	})
}
