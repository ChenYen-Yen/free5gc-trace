package consumer

import (
	"sync"
	"time"

	ausf_context "github.com/free5gc/ausf/internal/context"
	"github.com/free5gc/ausf/internal/logger"
	"github.com/free5gc/openapi/models"
	Nudm_UEAU "github.com/free5gc/openapi/udm/UEAuthentication"
	sbi_metrics "github.com/free5gc/util/metrics/sbi"

	//add
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// add
var udmTracer = otel.Tracer("ausf-sbi")

type nudmService struct {
	consumer *Consumer

	ueauMu sync.RWMutex

	ueauClients map[string]*Nudm_UEAU.APIClient
}

func (s *nudmService) getUdmUeauClient(uri string) *Nudm_UEAU.APIClient {
	if uri == "" {
		return nil
	}
	s.ueauMu.RLock()
	client, ok := s.ueauClients[uri]
	if ok {
		s.ueauMu.RUnlock()
		return client
	}

	configuration := Nudm_UEAU.NewConfiguration()
	configuration.SetBasePath(uri)
	configuration.SetMetrics(sbi_metrics.SbiMetricHook)
	client = Nudm_UEAU.NewAPIClient(configuration)

	s.ueauMu.RUnlock()
	s.ueauMu.Lock()
	defer s.ueauMu.Unlock()
	s.ueauClients[uri] = client
	return client
}

func (s *nudmService) SendAuthResultToUDM(
	baseCtx context.Context, //add
	id string,
	authType models.UdmUeauAuthType,
	success bool,
	servingNetworkName, udmUrl string,
) (error, context.Context) {
	timeNow := time.Now()
	timePtr := &timeNow

	self := s.consumer.Context()

	authEvent := models.AuthEvent{
		TimeStamp:          timePtr,
		AuthType:           authType,
		Success:            success,
		ServingNetworkName: servingNetworkName,
		NfInstanceId:       self.GetSelfID(),
	}

	client := s.getUdmUeauClient(udmUrl)

	ctx, _, err := ausf_context.GetSelf().GetTokenCtx(models.ServiceName_NUDM_UEAU, models.NrfNfManagementNfType_UDM)
	if err != nil {
		return err, nil
	}

	//add
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	spanCtx, span := udmTracer.Start(baseCtx, "AUSF → UDM: SendAuthResultToUDM")
	span.SetAttributes(
		attribute.String("target.nf", "UDM"),
		attribute.String("ausf.id", id),
		attribute.String("udm.url", udmUrl),
	)
	defer span.End()
	ctxForHTTP := trace.ContextWithSpan(ctx, span)

	request := &Nudm_UEAU.ConfirmAuthRequest{
		Supi:      &id,        // Make sure this is correctly referenced
		AuthEvent: &authEvent, // Make sure this is correctly referenced
	}

	//add
	// _, confirmAuthErr := client.ConfirmAuthApi.ConfirmAuth(ctx, request)
	_, confirmAuthErr := client.ConfirmAuthApi.ConfirmAuth(ctxForHTTP, request)
	if confirmAuthErr != nil {
		logger.ConsumerLog.Errorf("Error in ConfirmAuth: %v", confirmAuthErr)
	}

	return confirmAuthErr, spanCtx
}

func (s *nudmService) GenerateAuthDataApi(
	baseCtx context.Context, //add
	udmUrl string,
	supiOrSuci string,
	authInfoReq models.AuthenticationInfoRequest,
) (*models.UdmUeauAuthenticationInfoResult, *models.ProblemDetails, error, context.Context) {
	client := s.getUdmUeauClient(udmUrl)

	ctx, pd, err := ausf_context.GetSelf().GetTokenCtx(models.ServiceName_NUDM_UEAU, models.NrfNfManagementNfType_UDM)
	if err != nil {
		return nil, pd, err, nil
	}

	//add
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	spanCtx, span := udmTracer.Start(baseCtx, "AUSF → UDM: GenerateAuthData")
	span.SetAttributes(
		attribute.String("target.nf", "UDM"),
		attribute.String("ausf.supi_or_suci", supiOrSuci),
		attribute.String("udm.url", udmUrl),
	)
	defer span.End()
	ctxForHTTP := trace.ContextWithSpan(ctx, span)

	udmAuthInfoReq := models.UdmUeauAuthenticationInfoRequest{
		SupportedFeatures:     authInfoReq.SupportedFeatures,
		ServingNetworkName:    authInfoReq.ServingNetworkName,
		ResynchronizationInfo: authInfoReq.ResynchronizationInfo,
		AusfInstanceId:        authInfoReq.AusfInstanceId,
		CellCagInfo:           authInfoReq.CellCagInfo,
		N5gcInd:               authInfoReq.N5gcInd,
	}

	request := &Nudm_UEAU.GenerateAuthDataRequest{
		SupiOrSuci:                       &supiOrSuci,
		UdmUeauAuthenticationInfoRequest: &udmAuthInfoReq,
	}

	//add
	// rsp, err := client.GenerateAuthDataApi.GenerateAuthData(ctx, request)
	rsp, err := client.GenerateAuthDataApi.GenerateAuthData(ctxForHTTP, request)
	if err != nil {
		var problemDetails models.ProblemDetails
		if rsp == nil {
			problemDetails.Cause = "NO_RESPONSE_FROM_SERVER"
		} else if rsp.UdmUeauAuthenticationInfoResult.AuthenticationVector == nil {
			problemDetails.Cause = "AV_GENERATION_PROBLEM"
		} else {
			problemDetails.Cause = "UPSTREAM_SERVER_ERROR"
		}
		return nil, &problemDetails, err, nil
	}
	authInfoResult := rsp.UdmUeauAuthenticationInfoResult

	span.SetAttributes(
		attribute.String("udm.auth_type", string(authInfoResult.AuthType)),
	)

	return &authInfoResult, nil, nil, spanCtx
}
