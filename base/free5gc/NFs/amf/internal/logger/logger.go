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
	GinLog      *logrus.Entry
	NgapLog     *logrus.Entry
	HandlerLog  *logrus.Entry
	HttpLog     *logrus.Entry
	GmmLog      *logrus.Entry
	MtLog       *logrus.Entry
	ProducerLog *logrus.Entry
	SBILog      *logrus.Entry
	LocationLog *logrus.Entry
	CommLog     *logrus.Entry
	CallbackLog *logrus.Entry
	UtilLog     *logrus.Entry
	NasLog      *logrus.Entry
	ConsumerLog *logrus.Entry
	EeLog       *logrus.Entry
	AppLog      *logrus.Entry
)

const (
	FieldRanAddr     string = "ran_addr"
	FieldAmfUeNgapID string = "amf_ue_ngap_id"
	FieldSupi        string = "supi"
)

func init() {
	fieldsOrder := []string{
		logger_util.FieldNF,
		logger_util.FieldCategory,
	}

	Log = logger_util.New(fieldsOrder)
	// use JSON formatter so structured fields (traceId/spanId) appear as JSON keys
	Log.SetFormatter(&logrus.JSONFormatter{})
	NfLog = Log.WithField(logger_util.FieldNF, "AMF")
	MainLog = NfLog.WithField(logger_util.FieldCategory, "Main")
	InitLog = NfLog.WithField(logger_util.FieldCategory, "Init")
	CfgLog = NfLog.WithField(logger_util.FieldCategory, "CFG")
	CtxLog = NfLog.WithField(logger_util.FieldCategory, "CTX")
	GinLog = NfLog.WithField(logger_util.FieldCategory, "GIN")
	NgapLog = NfLog.WithField(logger_util.FieldCategory, "Ngap")
	HandlerLog = NfLog.WithField(logger_util.FieldCategory, "Handler")
	HttpLog = NfLog.WithField(logger_util.FieldCategory, "Http")
	GmmLog = NfLog.WithField(logger_util.FieldCategory, "Gmm")
	MtLog = NfLog.WithField(logger_util.FieldCategory, "Mt")
	ProducerLog = NfLog.WithField(logger_util.FieldCategory, "Producer")
	SBILog = NfLog.WithField(logger_util.FieldCategory, "SBI")
	LocationLog = NfLog.WithField(logger_util.FieldCategory, "Location")
	CommLog = NfLog.WithField(logger_util.FieldCategory, "Comm")
	CallbackLog = NfLog.WithField(logger_util.FieldCategory, "Callback")
	UtilLog = NfLog.WithField(logger_util.FieldCategory, "Util")
	NasLog = NfLog.WithField(logger_util.FieldCategory, "Nas")
	ConsumerLog = NfLog.WithField(logger_util.FieldCategory, "Consumer")
	EeLog = NfLog.WithField(logger_util.FieldCategory, "Ee")

	//add
	AppLog = NfLog.WithField(logger_util.FieldCategory, "AppLog")
}

// WithTraceContext extracts trace/span from context.Context and attaches to base logger.
// This is a helper to reuse WithTrace logic when you only have context.Context.
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
