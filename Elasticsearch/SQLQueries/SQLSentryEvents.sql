SELECT tl.RemoteObjectID AS EventType,
th.NormalizedTextData,
tl.[Database],
tl.Computer AS ClientComputer,
tl.Application,
tl.Operator AS UserName,
(tl.Duration/10000000) AS Duration,
tl.CPU,
tl.Reads,
tl.Writes,
tl.RunStatus,
FORMAT(DATEADD(mi, DATEDIFF(mi,GETDATE() ,GETUTCDATE()), tl.StartTime),'yyyyMMdd')+'T'+FORMAT(DATEADD(mi, DATEDIFF(mi, GETDATE(),GETUTCDATE() ), tl.StartTime),'HHmmss')+'.000Z' AS EventTime,
CASE 
WHEN th.NormalizedTextData LIKE '%trace_getdata%' THEN 'Monitoring'
	WHEN th.NormalizedTextData LIKE '%BACKUP%' THEN 'Maintenance'
	WHEN th.NormalizedTextData LIKE '%STATISTICS%' THEN 'Maintenance'
	WHEN th.NormalizedTextData LIKE '%INDEX%' THEN 'Maintenance'
                WHEN th.NormalizedTextData LIKE '%updatestats%' THEN 'Maintenance'
                WHEN th.NormalizedTextData LIKE '%sys.%' THEN 'Monitoring'
	WHEN th.NormalizedTextData LIKE '%repl%' THEN 'Replication'
WHEN th.NormalizedTextData LIKE '%sp_server_diagnostics%' THEN 'Monitoring'
WHEN th.NormalizedTextData LIKE '%sp_readrequest%' THEN 'Replication'
WHEN th.NormalizedTextData LIKE '%sp_MSdistribution%' THEN 'Replication'
WHEN th.NormalizedTextData LIKE '%syncobj_%' THEN 'Replication'
WHEN th.NormalizedTextData LIKE '%waitfor delay @waittime%' THEN 'CDC'
	ELSE 'Application Query'
END AS QueryType,
esc.ObjectName AS ServerName
FROM dbo.vwMetaHistorySqlServerTraceLog (nolock) tl 
INNER JOIN dbo.PerformanceAnalysisTraceHash (nolock) th ON tl.NormalizedTextMD5 = th.NormalizedTextMD5
INNER JOIN EventSource (nolock) es ON tl.EventSourceId = es.ObjectId
INNER JOIN EventSourceConnection (nolock) esc ON es.EventSourceConnectionID = esc.ObjectId
WHERE (esc.ObjectName LIKE 'SRV%') AND
tl.StartTime >= @EventStartTime AND
	tl.StartTime < @EventEndTime