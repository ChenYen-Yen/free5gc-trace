package processor

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/free5gc/openapi"
	"github.com/free5gc/openapi/models"
	Nudr_DataRepository "github.com/free5gc/openapi/udr/DataRepository"
	udm_context "github.com/free5gc/udm/internal/context"
	"github.com/free5gc/udm/internal/logger"
	"github.com/free5gc/util/metrics/sbi"

	//add
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ue_context_managemanet_service
func (p *Processor) GetAmf3gppAccessProcedure(baseCtx context.Context, c *gin.Context, ueID string, supportedFeatures string) {
	//add
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	_, span := tracer.Start(baseCtx, "UDM → UDR: GetAmf3gppAccessProcedure")
	span.SetAttributes(
		attribute.String("nf", "udm"),
		attribute.String("ue.id", ueID),
		attribute.String("supported_features", supportedFeatures),
	)
	defer span.End()

	ctx, pd, err := p.Context().GetTokenCtx(models.ServiceName_NUDR_DR, models.NrfNfManagementNfType_UDR)
	if err != nil {
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(int(pd.Status), pd)
		return
	}
	ctxForHTTP := trace.ContextWithSpan(ctx, span)

	var queryAmfContext3gppRequest Nudr_DataRepository.QueryAmfContext3gppRequest
	queryAmfContext3gppRequest.UeId = &ueID
	queryAmfContext3gppRequest.SupportedFeatures = &supportedFeatures

	clientAPI, err := p.Consumer().CreateUDMClientToUDR(ueID)
	if err != nil {
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	amf3GppAccessRegistration, err := clientAPI.AMF3GPPAccessRegistrationDocumentApi.
		QueryAmfContext3gpp(ctxForHTTP, &queryAmfContext3gppRequest) //add
	if err != nil {
		apiError, ok := err.(openapi.GenericOpenAPIError)
		if ok {
			c.Set(sbi.IN_PB_DETAILS_CTX_STR, http.StatusText(apiError.ErrorStatus))
			c.JSON(apiError.ErrorStatus, apiError.RawBody)
			return
		}
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	c.JSON(http.StatusOK, amf3GppAccessRegistration.Amf3GppAccessRegistration)
}

func (p *Processor) GetAmfNon3gppAccessProcedure(baseCtx context.Context, c *gin.Context, queryAmfContextNon3gppParamOpts Nudr_DataRepository.
	QueryAmfContextNon3gppRequest, ueID string,
) {
	//add
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	_, span := tracer.Start(baseCtx, "UDM → UDR: GetAmfNon3gppAccessProcedure")
	span.SetAttributes(
		attribute.String("nf", "udm"),
		attribute.String("ue.id", ueID),
	)
	defer span.End()

	ctx, pd, err := p.Context().GetTokenCtx(models.ServiceName_NUDR_DR, models.NrfNfManagementNfType_UDR)
	ctxForHTTP := trace.ContextWithSpan(ctx, span)

	if err != nil {
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(int(pd.Status), pd)
	}
	clientAPI, err := p.Consumer().CreateUDMClientToUDR(ueID)
	if err != nil {
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}
	amfNon3GppAccessRegistrationResponse, err := clientAPI.AMFNon3GPPAccessRegistrationDocumentApi.
		QueryAmfContextNon3gpp(ctxForHTTP, &queryAmfContextNon3gppParamOpts) //add
	if err != nil {
		apiError, ok := err.(openapi.GenericOpenAPIError)
		if ok {
			c.Set(sbi.IN_PB_DETAILS_CTX_STR, http.StatusText(apiError.ErrorStatus))
			c.JSON(apiError.ErrorStatus, apiError.RawBody)
			return
		}
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	c.JSON(http.StatusOK, amfNon3GppAccessRegistrationResponse.AmfNon3GppAccessRegistration)
}

func (p *Processor) RegistrationAmf3gppAccessProcedure(baseCtx context.Context, c *gin.Context,
	registerRequest models.Amf3GppAccessRegistration,
	ueID string,
) {
	//add
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	_, span := tracer.Start(baseCtx, "UDM → UDR/AMF: RegistrationAmf3gppAccessProcedure")
	span.SetAttributes(
		attribute.String("nf", "udm"),
		attribute.String("ue.id", ueID),
	)
	defer span.End()

	ctx, pd, err := p.Context().GetTokenCtx(models.ServiceName_NUDR_DR, models.NrfNfManagementNfType_UDR)
	if err != nil {
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(int(pd.Status), pd)
		return
	}
	ctxForHTTP := trace.ContextWithSpan(ctx, span)
	// TODO: EPS interworking with N26 is not supported yet in this stage
	var oldAmf3GppAccessRegContext *models.Amf3GppAccessRegistration
	var ue *udm_context.UdmUeContext

	if p.Context().UdmAmf3gppRegContextExists(ueID) {
		ue, _ = p.Context().UdmUeFindBySupi(ueID)
		oldAmf3GppAccessRegContext = ue.Amf3GppAccessRegistration
	}

	p.Context().CreateAmf3gppRegContext(ueID, registerRequest)

	clientAPI, err := p.Consumer().CreateUDMClientToUDR(ueID)
	if err != nil {
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	var createAmfContext3gppRequest Nudr_DataRepository.CreateAmfContext3gppRequest
	createAmfContext3gppRequest.UeId = &ueID
	createAmfContext3gppRequest.Amf3GppAccessRegistration = &registerRequest
	_, err = clientAPI.AMF3GPPAccessRegistrationDocumentApi.CreateAmfContext3gpp(ctxForHTTP, //add
		&createAmfContext3gppRequest)
	if err != nil {
		apiError, ok := err.(openapi.GenericOpenAPIError)
		if ok {
			c.Set(sbi.IN_PB_DETAILS_CTX_STR, http.StatusText(apiError.ErrorStatus))
			c.JSON(apiError.ErrorStatus, apiError.RawBody)
			return
		}
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	// TS 23.502 4.2.2.2.2 14d: UDM initiate a Nudm_UECM_DeregistrationNotification to the old AMF
	// corresponding to the same (e.g. 3GPP) access, if one exists
	if oldAmf3GppAccessRegContext != nil {
		if !ue.SameAsStoredGUAMI3gpp(*oldAmf3GppAccessRegContext.Guami) {
			// Based on TS 23.502 4.2.2.2.2, If the serving NF removal reason indicated by the UDM is Initial Registration,
			// the old AMF invokes the Nsmf_PDUSession_ReleaseSMContext (SM Context ID). Thus we give different
			// dereg cause based on registration parameter from serving AMF
			deregReason := models.UdmUecmDeregistrationReason_UE_REGISTRATION_AREA_CHANGE
			if registerRequest.InitialRegistrationInd {
				deregReason = models.UdmUecmDeregistrationReason_UE_INITIAL_REGISTRATION
			}
			deregistData := models.UdmUecmDeregistrationData{
				DeregReason: deregReason,
				AccessType:  models.AccessType__3_GPP_ACCESS,
			}

			go func() {
				traceLog := logger.WithTraceContext(ctxForHTTP, logger.UecmLog)
				traceLog.Infof("Send DeregNotify to old AMF GUAMI=%v", oldAmf3GppAccessRegContext.Guami)
				span.SetAttributes(
					attribute.Bool("to_AMF", true),
				)
				pd := p.SendOnDeregistrationNotification(ctxForHTTP, ueID, //add
					oldAmf3GppAccessRegContext.DeregCallbackUri,
					deregistData) // Deregistration Notify Triggered
				if pd != nil {
					traceLog.Errorf("RegistrationAmf3gppAccess: send DeregNotify fail %v", pd)
				}
			}()
		}

		c.JSON(http.StatusOK, registerRequest)
	} else {
		udmUe, _ := p.Context().UdmUeFindBySupi(ueID)
		c.Header("Location", udmUe.GetLocationURI(udm_context.LocationUriAmf3GppAccessRegistration))
		c.JSON(http.StatusCreated, registerRequest)
	}
}

func (p *Processor) RegisterAmfNon3gppAccessProcedure(baseCtx context.Context, c *gin.Context,
	registerRequest models.AmfNon3GppAccessRegistration,
	ueID string,
) {
	//add
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	_, span := tracer.Start(baseCtx, "UDM → UDR/AMF: RegisterAmfNon3gppAccessProcedure")
	span.SetAttributes(
		attribute.String("nf", "udm"),
		attribute.String("ue.id", ueID),
	)
	defer span.End()
	ctx, pd, err := p.Context().GetTokenCtx(models.ServiceName_NUDR_DR, models.NrfNfManagementNfType_UDR)
	if err != nil {
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(int(pd.Status), pd)
		return
	}
	ctxForHTTP := trace.ContextWithSpan(ctx, span)
	var oldAmfNon3GppAccessRegContext *models.AmfNon3GppAccessRegistration
	if p.Context().UdmAmfNon3gppRegContextExists(ueID) {
		ue, _ := p.Context().UdmUeFindBySupi(ueID)
		oldAmfNon3GppAccessRegContext = ue.AmfNon3GppAccessRegistration
	}

	p.Context().CreateAmfNon3gppRegContext(ueID, registerRequest)

	clientAPI, err := p.Consumer().CreateUDMClientToUDR(ueID)
	if err != nil {
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	var createAmfContextNon3gppRequest Nudr_DataRepository.CreateAmfContextNon3gppRequest
	createAmfContextNon3gppRequest.UeId = &ueID
	createAmfContextNon3gppRequest.AmfNon3GppAccessRegistration = &registerRequest

	_, err = clientAPI.AMFNon3GPPAccessRegistrationDocumentApi.CreateAmfContextNon3gpp(
		ctxForHTTP, &createAmfContextNon3gppRequest) //add
	if err != nil {
		apiError, ok := err.(openapi.GenericOpenAPIError)
		if ok {
			c.Set(sbi.IN_PB_DETAILS_CTX_STR, http.StatusText(apiError.ErrorStatus))
			c.JSON(apiError.ErrorStatus, apiError.RawBody)
			return
		}
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	// TS 23.502 4.2.2.2.2 14d: UDM initiate a Nudm_UECM_DeregistrationNotification to the old AMF
	// corresponding to the same (e.g. 3GPP) access, if one exists
	if oldAmfNon3GppAccessRegContext != nil {
		deregistData := models.UdmUecmDeregistrationData{
			DeregReason: models.UdmUecmDeregistrationReason_UE_INITIAL_REGISTRATION,
			AccessType:  models.AccessType_NON_3_GPP_ACCESS,
		}

		go func() {
			traceLog := logger.WithTraceContext(ctxForHTTP, logger.UecmLog)
			traceLog.Infof("Send DeregNotify to old AMF GUAMI=%v", oldAmfNon3GppAccessRegContext.Guami)
			span.SetAttributes(
				attribute.Bool("to_AMF", true),
			)
			pd := p.SendOnDeregistrationNotification(ctxForHTTP, ueID, oldAmfNon3GppAccessRegContext.DeregCallbackUri, //add
				deregistData) // Deregistration Notify Triggered
			if pd != nil {
				traceLog.Errorf("RegisterAmfNon3gppAccess: send DeregNotify fail %v", pd)
			}
		}()

		c.JSON(http.StatusOK, registerRequest)
	} else {
		udmUe, _ := p.Context().UdmUeFindBySupi(ueID)
		c.Header("Location", udmUe.GetLocationURI(udm_context.LocationUriAmfNon3GppAccessRegistration))
		c.JSON(http.StatusCreated, registerRequest)
	}
}

func (p *Processor) UpdateAmf3gppAccessProcedure(
	baseCtx context.Context,
	c *gin.Context,
	request models.Amf3GppAccessRegistrationModification,
	ueID string,
) {
	//add
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	_, span := tracer.Start(baseCtx, "UDM → UDR: UpdateAmf3gppAccessProcedure")
	span.SetAttributes(
		attribute.String("nf", "udm"),
		attribute.String("ue.id", ueID),
	)
	defer span.End()

	ctx, pd, err := p.Context().GetTokenCtx(models.ServiceName_NUDR_DR, models.NrfNfManagementNfType_UDR)
	if err != nil {
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(int(pd.Status), pd)
		return
	}
	ctxForHTTP := trace.ContextWithSpan(ctx, span)
	traceLog := logger.WithTraceContext(ctxForHTTP, logger.UecmLog)
	var patchItemReqArray []models.PatchItem
	currentContext := p.Context().GetAmf3gppRegContext(ueID)
	if currentContext == nil {
		traceLog.Errorln("[UpdateAmf3gppAccess] Empty Amf3gppRegContext")
		problemDetails := &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "CONTEXT_NOT_FOUND",
		}
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	if request.Guami != nil {
		udmUe, _ := p.Context().UdmUeFindBySupi(ueID)
		if udmUe.SameAsStoredGUAMI3gpp(*request.Guami) { // deregistration
			traceLog.Infoln("UpdateAmf3gppAccess - deregistration")
			request.PurgeFlag = true
		} else {
			traceLog.Errorln("INVALID_GUAMI")
			problemDetails := &models.ProblemDetails{
				Status: http.StatusForbidden,
				Cause:  "INVALID_GUAMI",
			}
			c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
			c.JSON(int(problemDetails.Status), problemDetails)
			return
		}

		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "guami"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = *request.Guami
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.PurgeFlag {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "purgeFlag"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.PurgeFlag
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.Pei != "" {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "pei"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.Pei
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.ImsVoPs != "" {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "imsVoPs"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.ImsVoPs
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.BackupAmfInfo != nil {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "backupAmfInfo"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.BackupAmfInfo
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	clientAPI, err := p.Consumer().CreateUDMClientToUDR(ueID)
	if err != nil {
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	var amfContext3gppRequest Nudr_DataRepository.AmfContext3gppRequest
	amfContext3gppRequest.UeId = &ueID
	amfContext3gppRequest.PatchItem = patchItemReqArray
	_, err = clientAPI.AMF3GPPAccessRegistrationDocumentApi.AmfContext3gpp(ctxForHTTP, //add
		&amfContext3gppRequest)
	if err != nil {
		if apiErr, ok := err.(openapi.GenericOpenAPIError); ok {
			if amfContext3gppErr, ok2 := apiErr.Model().(Nudr_DataRepository.AmfContext3gppError); ok2 {
				problem := amfContext3gppErr.ProblemDetails
				c.Set(sbi.IN_PB_DETAILS_CTX_STR, problem.Cause)
				c.JSON(int(problem.Status), problem)
				return
			}
		}
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	if request.PurgeFlag {
		udmUe, _ := p.Context().UdmUeFindBySupi(ueID)
		udmUe.Amf3GppAccessRegistration = nil
	}

	c.Status(http.StatusNoContent)
}

func (p *Processor) UpdateAmfNon3gppAccessProcedure(
	baseCtx context.Context, //add
	c *gin.Context,
	request models.AmfNon3GppAccessRegistrationModification,
	ueID string,
) {
	//add
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	_, span := tracer.Start(baseCtx, "UDM → UDR: UpdateAmfNon3gppAccessProcedure")
	span.SetAttributes(
		attribute.String("nf", "udm"),
		attribute.String("ue.id", ueID),
	)
	defer span.End()

	ctx, pd, err := p.Context().GetTokenCtx(models.ServiceName_NUDR_DR, models.NrfNfManagementNfType_UDR)
	if err != nil {
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(int(pd.Status), pd)
		return
	}
	ctxForHTTP := trace.ContextWithSpan(ctx, span)

	traceLog := logger.WithTraceContext(ctxForHTTP, logger.UecmLog)
	var patchItemReqArray []models.PatchItem
	currentContext := p.Context().GetAmfNon3gppRegContext(ueID)
	if currentContext == nil {
		traceLog.Errorln("[UpdateAmfNon3gppAccess] Empty AmfNon3gppRegContext")
		problemDetails := &models.ProblemDetails{
			Status: http.StatusNotFound,
			Cause:  "CONTEXT_NOT_FOUND",
		}
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	if request.Guami != nil {
		udmUe, _ := p.Context().UdmUeFindBySupi(ueID)
		if udmUe.SameAsStoredGUAMINon3gpp(*request.Guami) { // deregistration
			traceLog.Infoln("UpdateAmfNon3gppAccess - deregistration")
			request.PurgeFlag = true
		} else {
			traceLog.Errorln("INVALID_GUAMI")
			problemDetails := &models.ProblemDetails{
				Status: http.StatusForbidden,
				Cause:  "INVALID_GUAMI",
			}
			c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
			c.JSON(int(problemDetails.Status), problemDetails)
			return
		}

		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "guami"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = *request.Guami
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.PurgeFlag {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "purgeFlag"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.PurgeFlag
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.Pei != "" {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "pei"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.Pei
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.ImsVoPs != "" {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "imsVoPs"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.ImsVoPs
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	if request.BackupAmfInfo != nil {
		var patchItemTmp models.PatchItem
		patchItemTmp.Path = "/" + "backupAmfInfo"
		patchItemTmp.Op = models.PatchOperation_REPLACE
		patchItemTmp.Value = request.BackupAmfInfo
		patchItemReqArray = append(patchItemReqArray, patchItemTmp)
	}

	clientAPI, err := p.Consumer().CreateUDMClientToUDR(ueID)
	if err != nil {
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}
	var amfContextNon3gppRequest Nudr_DataRepository.AmfContextNon3gppRequest
	amfContextNon3gppRequest.UeId = &ueID
	amfContextNon3gppRequest.PatchItem = patchItemReqArray
	_, err = clientAPI.AMFNon3GPPAccessRegistrationDocumentApi.AmfContextNon3gpp(ctxForHTTP, //add
		&amfContextNon3gppRequest)
	if err != nil {
		if apiErr, ok := err.(openapi.GenericOpenAPIError); ok {
			if amfContextNon3gppErr, ok2 := apiErr.Model().(Nudr_DataRepository.AmfContextNon3gppError); ok2 {
				problem := amfContextNon3gppErr.ProblemDetails
				c.Set(sbi.IN_PB_DETAILS_CTX_STR, problem.Cause)
				c.JSON(int(problem.Status), problem)
				return
			}
		}
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	c.Status(http.StatusNoContent)
}

func (p *Processor) DeregistrationSmfRegistrationsProcedure(
	baseCtx context.Context, //add
	c *gin.Context,
	ueID string,
	pduSessionID string,
) {
	//add
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	_, span := tracer.Start(baseCtx, "UDM → UDR: DeregistrationSmfRegistrationsProcedure")
	span.SetAttributes(
		attribute.String("nf", "udm"),
		attribute.String("ue.id", ueID),
	)
	defer span.End()

	ctx, pd, err := p.Context().GetTokenCtx(models.ServiceName_NUDR_DR, models.NrfNfManagementNfType_UDR)
	if err != nil {
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(int(pd.Status), pd)
		return
	}

	ctxForHTTP := trace.ContextWithSpan(ctx, span)
	clientAPI, err := p.Consumer().CreateUDMClientToUDR(ueID)
	if err != nil {
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	var pduSessionIDInt32 int32
	num, err := strconv.ParseInt(pduSessionID, 10, 32)
	if err != nil {
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	pduSessionIDInt32 = int32(num)
	var deleteSmfRegistrationRequest Nudr_DataRepository.DeleteSmfRegistrationRequest
	deleteSmfRegistrationRequest.UeId = &ueID
	deleteSmfRegistrationRequest.PduSessionId = &pduSessionIDInt32
	_, err = clientAPI.SMFRegistrationDocumentApi.DeleteSmfRegistration(ctxForHTTP, &deleteSmfRegistrationRequest) //add
	if err != nil {
		apiError, ok := err.(openapi.GenericOpenAPIError)
		if ok {
			c.Set(sbi.IN_PB_DETAILS_CTX_STR, http.StatusText(apiError.ErrorStatus))
			c.JSON(apiError.ErrorStatus, apiError.RawBody)
			return
		}
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	c.Status(http.StatusNoContent)
}

func (p *Processor) RegistrationSmfRegistrationsProcedure(
	baseCtx context.Context, //add
	c *gin.Context,
	smfRegistration *models.SmfRegistration,
	ueID string,
	pduSessionID string,
) {
	//add
	if baseCtx == nil {
		baseCtx = context.Background()
	}

	_, span := tracer.Start(baseCtx, "UDM → UDR: RegistrationSmfRegistrationsProcedure")
	span.SetAttributes(
		attribute.String("nf", "udm"),
		attribute.String("ue.id", ueID),
	)
	defer span.End()

	ctx, pd, err := p.Context().GetTokenCtx(models.ServiceName_NUDR_DR, models.NrfNfManagementNfType_UDR)
	if err != nil {
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, pd.Cause)
		c.JSON(int(pd.Status), pd)
		return
	}
	ctxForHTTP := trace.ContextWithSpan(ctx, span)
	traceLog := logger.WithTraceContext(ctxForHTTP, logger.UecmLog)
	contextExisted := false
	p.Context().CreateSmfRegContext(ueID, pduSessionID)
	if !p.Context().UdmSmfRegContextNotExists(ueID) {
		contextExisted = true
	}

	pduID64, err := strconv.ParseInt(pduSessionID, 10, 32)
	if err != nil {
		traceLog.Errorln(err.Error())
	}
	pduID32 := int32(pduID64)

	var createSmfContext3gppRequest Nudr_DataRepository.CreateOrUpdateSmfRegistrationRequest
	createSmfContext3gppRequest.UeId = &ueID
	createSmfContext3gppRequest.SmfRegistration = smfRegistration
	createSmfContext3gppRequest.PduSessionId = &pduID32

	clientAPI, err := p.Consumer().CreateUDMClientToUDR(ueID)
	if err != nil {
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}
	_, err = clientAPI.SMFRegistrationDocumentApi.CreateOrUpdateSmfRegistration(ctxForHTTP, &createSmfContext3gppRequest) //add
	if err != nil {
		apiError, ok := err.(openapi.GenericOpenAPIError)
		if ok {
			c.Set(sbi.IN_PB_DETAILS_CTX_STR, http.StatusText(apiError.ErrorStatus))
			c.JSON(apiError.ErrorStatus, apiError.RawBody)
			return
		}
		problemDetails := openapi.ProblemDetailsSystemFailure(err.Error())
		c.Set(sbi.IN_PB_DETAILS_CTX_STR, problemDetails.Cause)
		c.JSON(int(problemDetails.Status), problemDetails)
		return
	}

	if contextExisted {
		c.Status(http.StatusNoContent)
	} else {
		udmUe, _ := p.Context().UdmUeFindBySupi(ueID)
		c.Header("Location", udmUe.GetLocationURI(udm_context.LocationUriSmfRegistration))
		c.JSON(http.StatusCreated, smfRegistration)
	}
}
