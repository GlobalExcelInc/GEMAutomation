SELECT FORMAT(evt.UTCEVENTDATE,'yyyyMMdd')+'T'+FORMAT(evt.UTCEVENTDATE,'HHmmss')+'.000Z' AS EventTime,
		(evt.EventDuration/1000000) AS EventDuration,
		CASE 
			WHEN src.SOURCE = 'PV-LOB-11298-PROD_claimportal - Claimant Portal' THEN 'Application'
			WHEN src.SOURCE = 'PV-LOB-11546-PROD_gem-portal-net - GEM_Portal_Net' THEN 'Application'
			WHEN src.SOURCE = 'application - E_Repricing' THEN 'Application'
			WHEN src.SOURCE = 'PV-LOB-11512-PROD_eis-webagent - eIS_WebAgent' THEN 'Application'
			WHEN src.SOURCE ='PV-LOB-11515-PROD_vgtactual - VGTActuals' THEN 'Application'
			WHEN src.SOURCE = 'EDIToPDF-WS - Production' THEN 'Web Services'
			WHEN src.SOURCE = 'PV-LOB-12014-PROD_eis-email-ws - eIS_WS' THEN 'Web Services'
			WHEN src.SOURCE = 'web-services/accpac - E_Repricing' THEN 'Web Services'
			WHEN src.SOURCE = 'web-services/e-Repricing - E_Repricing' THEN 'Web Services'
			WHEN src.SOURCE = 'PV-LOB-11513-PROD_eis-ws - eIS_WS' THEN 'Web Services'
			ELSE src.SOURCE
		END AS ApplicationType,
		CASE 
			WHEN src.SOURCE = 'PV-LOB-11298-PROD_claimportal - Claimant Portal' THEN 'Claimant Portal'
			WHEN src.SOURCE = 'PV-LOB-11546-PROD_gem-portal-net - GEM_Portal_Net' THEN 'GEM Portal'
			WHEN src.SOURCE = 'application - E_Repricing' THEN 'e-Repricing'
			WHEN src.SOURCE = 'PV-LOB-11512-PROD_eis-webagent - eIS_WebAgent' THEN 'eIS Web Agent'
			WHEN src.SOURCE = 'PV-LOB-11515-PROD_vgtactual - VGTActuals' THEN 'VGT Actuals Import'
			WHEN src.SOURCE = 'EDIToPDF-WS - Production' THEN 'EDIToPDF WS'
			WHEN src.SOURCE = 'PV-LOB-12014-PROD_eis-email-ws - eIS_WS' THEN 'eIS Email WS'
			WHEN src.SOURCE = 'web-services/accpac - E_Repricing' THEN 'e-Repricing ACCPAC WS'
			WHEN src.SOURCE = 'web-services/e-Repricing - E_Repricing' THEN 'e-Repricing Core WS'
			WHEN src.SOURCE = 'PV-LOB-11513-PROD_eis-ws - eIS_WS' THEN 'eIS WS'
			ELSE src.SOURCE
		END AS ApplicationName,
		evt.EventClassType AS EventType,
		evt.RootNodeName AS SourceName,
		evt.Description AS Message,
		evt.Aspect AS Aspect,
		ISNULL(evt.CATEGORY,'N/A') AS Category,
		REPLACE(srv.MACHINE,'etfsinc.com\','') AS ComputerName,
		ISNULL(usr.NAME,'N/A') AS UserName,
		rsrc.RESOURCEURI AS ResourceName,
		egrp.DESCRIPTION AS EventGroupDescription,
		ed.VALUE AS RootFunction
FROM apm.Event evt LEFT JOIN [apm].[MACHINE] srv ON evt.MACHINEID = srv.MACHINEID 
LEFT JOIN apm.USERS usr ON evt.USERID = usr.USERID
INNER JOIN apm.SOURCE src ON evt.SOURCEID = src.SOURCEID
INNER JOIN apm.RESOURCE rsrc ON evt.RESOURCEID = rsrc.RESOURCEID
INNER JOIN apm.EVENTGROUP egrp ON evt.EVENTGROUPID = egrp.EVENTGROUPID
INNER JOIN apm.EVENTDETAIL ed ON evt.EVENTID = ed.EVENTID AND ed.NAME = 'ROOTFUNCTION'	
WHERE DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()), evt.UTCEVENTDATE) >= @EventStartTime AND
	DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()), evt.UTCEVENTDATE) < @EventEndTime