package consumer

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"

	//add
	"context"
	"net/http"
	"time"

	amf_context "github.com/free5gc/amf/internal/context"
	"github.com/free5gc/amf/internal/logger"
	"github.com/free5gc/nas/nasType"
	"github.com/free5gc/openapi"
	Nausf_UEAuthentication "github.com/free5gc/openapi/ausf/UEAuthentication"
	"github.com/free5gc/openapi/models"
	sbi_metrics "github.com/free5gc/util/metrics/sbi"

	//add
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type nausfService struct {
	consumer *Consumer

	UEAuthenticationMu sync.RWMutex

	UEAuthenticationClients map[string]*Nausf_UEAuthentication.APIClient
}

func (s *nausfService) getUEAuthenticationClient(uri string) *Nausf_UEAuthentication.APIClient {
	if uri == "" {
		return nil
	}
	s.UEAuthenticationMu.RLock()
	client, ok := s.UEAuthenticationClients[uri]
	if ok {
		s.UEAuthenticationMu.RUnlock()
		return client
	}

	configuration := Nausf_UEAuthentication.NewConfiguration()
	configuration.SetBasePath(uri)
	configuration.SetMetrics(sbi_metrics.SbiMetricHook)
	configuration.SetHTTPClient(newOtelHTTPClient()) //add
	client = Nausf_UEAuthentication.NewAPIClient(configuration)

	s.UEAuthenticationMu.RUnlock()
	s.UEAuthenticationMu.Lock()
	defer s.UEAuthenticationMu.Unlock()
	s.UEAuthenticationClients[uri] = client
	return client
}

func (s *nausfService) SendUEAuthenticationAuthenticateRequest(ue *amf_context.AmfUe,
	resynchronizationInfo *models.ResynchronizationInfo,
) (*models.UeAuthenticationCtx, *models.ProblemDetails, error) {
	client := s.getUEAuthenticationClient(ue.AusfUri)
	if client == nil {
		return nil, nil, openapi.ReportError("ausf not found")
	}

	amfSelf := amf_context.GetSelf()

	//add
	baseCtx := ue.TraceContext
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	ctx, _, err := amf_context.GetSelf().GetTokenCtx(models.ServiceName_NAUSF_AUTH, models.NrfNfManagementNfType_AUSF)
	if err != nil {
		return nil, nil, err
	}

	tracer := otel.Tracer("amf-sbi")
	spanCtx, span := tracer.Start(baseCtx, "AMF → AUSF: SendUEAuthenticationAuthenticateRequest")
	span.SetAttributes(
		attribute.String("target.nf", "AUSF"),
		attribute.String("ue.supi", ue.Supi),
		attribute.String("ue.suci", ue.Suci),
	)
	defer span.End()

	ctxForHTTP := trace.ContextWithSpan(ctx, span)
	ue.TraceContext = spanCtx
	ue.GmmLog = logger.WithTraceContext(spanCtx, ue.GmmLog)

	servedGuami := amfSelf.ServedGuamiList[0]

	var authInfo models.AuthenticationInfo
	authInfo.SupiOrSuci = ue.Suci
	if mnc, err := strconv.Atoi(servedGuami.PlmnId.Mnc); err != nil {
		return nil, nil, err
	} else {
		authInfo.ServingNetworkName = fmt.Sprintf("5G:mnc%03d.mcc%s.3gppnetwork.org", mnc, servedGuami.PlmnId.Mcc)
	}
	if resynchronizationInfo != nil {
		authInfo.ResynchronizationInfo = resynchronizationInfo
	}

	authReq := Nausf_UEAuthentication.UeAuthenticationsPostRequest{
		AuthenticationInfo: &authInfo,
	}

	//add
	// res, localErr := client.DefaultApi.UeAuthenticationsPost(ctx, &authReq)
	res, localErr := client.DefaultApi.UeAuthenticationsPost(ctxForHTTP, &authReq)
	if localErr == nil {
		return &res.UeAuthenticationCtx, nil, nil
	} else {
		switch errType := localErr.(type) {
		// API error
		case openapi.GenericOpenAPIError:
			switch errModel := errType.Model().(type) {
			case Nausf_UEAuthentication.UeAuthenticationsPostError:
				return nil, &errModel.ProblemDetails, localErr
			case error:
				return nil, openapi.ProblemDetailsSystemFailure(errModel.Error()), nil
			default:
				return nil, nil, openapi.ReportError("openapi error")
			}
		case error:
			//add
			return nil, openapi.ProblemDetailsSystemFailure(errType.Error()), localErr
			// return nil, openapi.ProblemDetailsSystemFailure(errType.Error()), err
		default:
			return nil, nil, openapi.ReportError("server no response")
		}
	}
}

func (s *nausfService) SendAuth5gAkaConfirmRequest(ue *amf_context.AmfUe, resStar string) (
	*models.ConfirmationDataResponse, *models.ProblemDetails, error,
) {
	var ausfUri string
	var confirmUri *url.URL
	var err error
	if len(ue.AuthenticationCtx.Links["5g-aka"]) > 0 {
		confirmUri, err = url.Parse(ue.AuthenticationCtx.Links["5g-aka"][0].Href)
	}
	if err != nil {
		return nil, nil, err
	} else {
		ausfUri = fmt.Sprintf("%s://%s", confirmUri.Scheme, confirmUri.Host)
	}

	client := s.getUEAuthenticationClient(ausfUri)
	if client == nil {
		return nil, nil, openapi.ReportError("ausf not found")
	}

	//add
	baseCtx := ue.TraceContext
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	ctx, _, err := amf_context.GetSelf().GetTokenCtx(models.ServiceName_NAUSF_AUTH, models.NrfNfManagementNfType_AUSF)
	if err != nil {
		return nil, nil, err
	}

	tracer := otel.Tracer("amf-sbi")
	spanCtx, span := tracer.Start(baseCtx, "AMF → AUSF: SendUEAuthenticationAuthenticateRequest")
	span.SetAttributes(
		attribute.String("target.nf", "AUSF"),
		attribute.String("ue.supi", ue.Supi),
		attribute.String("ue.suci", ue.Suci),
	)
	defer span.End()

	ctxForHTTP := trace.ContextWithSpan(ctx, span)
	ue.TraceContext = spanCtx
	ue.GmmLog = logger.WithTraceContext(spanCtx, ue.GmmLog)

	// confirmUri.RequestURI() = "/nausf-auth/v1/ue-authentications/{authctxId}/5g-aka-confirmation"
	// splituri = ["","nausf-auth","ue-authentications",{authctxId},"5g-aka-confirmation"]
	// authctxId = {authctxId}
	splituri := strings.Split(confirmUri.RequestURI(), "/")
	authctxId := ""
	if len(splituri) > 4 {
		authctxId = splituri[4]
	} else {
		return nil, nil, fmt.Errorf("authctxId is nil")
	}

	confirmData := &Nausf_UEAuthentication.UeAuthenticationsAuthCtxId5gAkaConfirmationPutRequest{
		AuthCtxId: &authctxId,
		ConfirmationData: &models.ConfirmationData{
			ResStar: resStar,
		},
	}
	//add
	// confirmResult, localErr := client.DefaultApi.UeAuthenticationsAuthCtxId5gAkaConfirmationPut(
	// 	ctx, confirmData)
	confirmResult, localErr := client.DefaultApi.UeAuthenticationsAuthCtxId5gAkaConfirmationPut(
		ctxForHTTP, confirmData)

	if localErr == nil {
		return &confirmResult.ConfirmationDataResponse, nil, nil
	} else {
		switch err := localErr.(type) {
		// API error
		case openapi.GenericOpenAPIError:
			switch errModel := err.Model().(type) {
			case Nausf_UEAuthentication.UeAuthenticationsAuthCtxId5gAkaConfirmationPutError:
				return nil, &errModel.ProblemDetails, localErr
			case error:
				return nil, openapi.ProblemDetailsSystemFailure(errModel.Error()), nil
			default:
				return nil, nil, openapi.ReportError("openapi error")
			}
		case error:
			return nil, openapi.ProblemDetailsSystemFailure(err.Error()), nil
		default:
			return nil, nil, openapi.ReportError("server no response")
		}
	}
}

func (s *nausfService) SendEapAuthConfirmRequest(ue *amf_context.AmfUe, eapMsg nasType.EAPMessage) (
	response *models.EapSession, problemDetails *models.ProblemDetails, err1 error,
) {
	var confirmUri *url.URL
	var err error
	if len(ue.AuthenticationCtx.Links["eap-session"]) > 0 {
		confirmUri, err = url.Parse(ue.AuthenticationCtx.Links["eap-session"][0].Href)
	}
	if err != nil {
		logger.ConsumerLog.Errorf("url Parse failed: %+v", err)
	}
	ausfUri := fmt.Sprintf("%s://%s", confirmUri.Scheme, confirmUri.Host)

	client := s.getUEAuthenticationClient(ausfUri)
	if client == nil {
		return nil, nil, openapi.ReportError("ausf not found")
	}

	// confirmUri.RequestURI() = "/nausf-auth/v1/ue-authentications/{authctxId}/eap-session"
	// splituri = ["","nausf-auth","ue-authentications",{authctxId},"eap-session"]
	// authctxId = {authctxId}
	splituri := strings.Split(confirmUri.RequestURI(), "/")
	authctxId := ""
	if len(splituri) > 4 {
		authctxId = splituri[4]
	} else {
		return nil, nil, fmt.Errorf("authctxId is nil")
	}

	eapSessionReq := Nausf_UEAuthentication.EapAuthMethodRequest{
		AuthCtxId: &authctxId,
		EapSession: &models.EapSession{
			EapPayload: base64.StdEncoding.EncodeToString(eapMsg.GetEAPMessage()),
		},
	}

	//add
	baseCtx := ue.TraceContext
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	ctx, _, err := amf_context.GetSelf().GetTokenCtx(models.ServiceName_NAUSF_AUTH, models.NrfNfManagementNfType_AUSF)
	if err != nil {
		return nil, nil, err
	}

	tracer := otel.Tracer("amf-sbi")
	spanCtx, span := tracer.Start(baseCtx, "AMF → AUSF: SendEapAuthConfirmRequest")
	span.SetAttributes(
		attribute.String("target.nf", "AUSF"),
		attribute.String("ue.supi", ue.Supi),
		attribute.String("ue.suci", ue.Suci),
	)
	defer span.End()

	ctxForHTTP := trace.ContextWithSpan(ctx, span)
	ue.TraceContext = spanCtx
	ue.GmmLog = logger.WithTraceContext(spanCtx, ue.GmmLog)

	//add
	// eapSession, localErr := client.DefaultApi.EapAuthMethod(ctx, &eapSessionReq)
	eapSession, localErr := client.DefaultApi.EapAuthMethod(ctxForHTTP, &eapSessionReq)

	if localErr == nil {
		response = &eapSession.EapSession
	} else {
		err = localErr
		switch errType := localErr.(type) {
		// API error
		case openapi.GenericOpenAPIError:
			switch errModel := errType.Model().(type) {
			case Nausf_UEAuthentication.EapAuthMethodError:
				problemDetails = &errModel.ProblemDetails
			case error:
				problemDetails = openapi.ProblemDetailsSystemFailure(errModel.Error())
			default:
				err = openapi.ReportError("openapi error")
			}
		case error:
			problemDetails = openapi.ProblemDetailsSystemFailure(errType.Error())
		default:
			err = openapi.ReportError("server no response")
		}
	}

	return response, problemDetails, err
}

// add
func newOtelHTTPClient() *http.Client {
	return &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
		Timeout:   30 * time.Second,
	}
}
