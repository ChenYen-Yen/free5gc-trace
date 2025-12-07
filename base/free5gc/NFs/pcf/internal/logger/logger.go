package logger

import (
	"context"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"

	logger_util "github.com/free5gc/util/logger"
)

var (
	Log           *logrus.Logger
	NfLog         *logrus.Entry
	MainLog       *logrus.Entry
	InitLog       *logrus.Entry
	CfgLog        *logrus.Entry
	CtxLog        *logrus.Entry
	GinLog        *logrus.Entry
	SBILog        *logrus.Entry
	AmPolicyLog   *logrus.Entry
	BdtPolicyLog  *logrus.Entry
	ConsumerLog   *logrus.Entry
	CallbackLog   *logrus.Entry
	OamLog        *logrus.Entry
	PolicyAuthLog *logrus.Entry
	ProcLog       *logrus.Entry
	SmPolicyLog   *logrus.Entry
	UtilLog       *logrus.Entry
)

func init() {
	fieldsOrder := []string{
		logger_util.FieldNF,
		logger_util.FieldCategory,
	}

	Log = logger_util.New(fieldsOrder)
	NfLog = Log.WithField(logger_util.FieldNF, "PCF")
	MainLog = NfLog.WithField(logger_util.FieldCategory, "Main")
	InitLog = NfLog.WithField(logger_util.FieldCategory, "Init")
	CfgLog = NfLog.WithField(logger_util.FieldCategory, "CFG")
	CtxLog = NfLog.WithField(logger_util.FieldCategory, "CTX")
	GinLog = NfLog.WithField(logger_util.FieldCategory, "GIN")
	SBILog = NfLog.WithField(logger_util.FieldCategory, "SBI")
	AmPolicyLog = NfLog.WithField(logger_util.FieldCategory, "AmPol")
	BdtPolicyLog = NfLog.WithField(logger_util.FieldCategory, "BdtPol")
	ConsumerLog = NfLog.WithField(logger_util.FieldCategory, "Consumer")
	CallbackLog = NfLog.WithField(logger_util.FieldCategory, "Callback")
	OamLog = NfLog.WithField(logger_util.FieldCategory, "Oam")
	PolicyAuthLog = NfLog.WithField(logger_util.FieldCategory, "PolAuth")
	ProcLog = NfLog.WithField(logger_util.FieldCategory, "Proc")
	SmPolicyLog = NfLog.WithField(logger_util.FieldCategory, "SMpolicy")
	UtilLog = NfLog.WithField(logger_util.FieldCategory, "Util")
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
