SELECT @@SERVERNAME AS ServerName,
    j.name as 'JobName',
    s.step_id as 'StepId',
    s.step_name as 'StepName',
    CASE 
	WHEN h.run_status  = 0 THEN 'Failed'
	WHEN h.run_status  = 1 THEN 'Succeeded'
	WHEN h.run_status  = 2 THEN 'Retry'
	WHEN h.run_status  = 3 THEN 'Canceled'
	WHEN h.run_status  = 4 THEN 'In Progress'
    END AS RunStatus,
    h.instance_id AS InstanceId,
FORMAT(DATEADD(mi, DATEDIFF(mi,GETDATE() ,GETUTCDATE()), msdb.dbo.agent_datetime(run_date, run_time)),'yyyyMMdd')+'T'+FORMAT(DATEADD(mi, DATEDIFF(mi, GETDATE(),GETUTCDATE() ), msdb.dbo.agent_datetime(run_date, run_time)),'HHmmss')+'.000Z' as 'StepStartTime',
FORMAT(DATEADD(mi, DATEDIFF(mi,GETDATE() ,GETUTCDATE()), DATEADD(second, ((run_duration/10000 * 3600) + ((run_duration%10000)/100*60) + (run_duration%10000)%100), msdb.dbo.agent_datetime(run_date, run_time))),'yyyyMMdd')+'T'+FORMAT(DATEADD(mi, DATEDIFF(mi, GETDATE(),GETUTCDATE() ), DATEADD(second, ((run_duration/10000 * 3600) + ((run_duration%10000)/100*60) + (run_duration%10000)%100), msdb.dbo.agent_datetime(run_date, run_time))),'HHmmss')+'.000Z' as 'StepEndTime',
    ((run_duration/10000 * 3600) + ((run_duration%10000)/100*60) + (run_duration%10000)%100) as 'stepdurationinseconds',
	h.message,
	s.retry_attempts AS RetryAttemps
FROM msdb.dbo.sysjobs j 
INNER JOIN msdb.dbo.sysjobsteps s 
    ON j.job_id = s.job_id
INNER JOIN msdb.dbo.sysjobhistory h 
    ON s.job_id = h.job_id 
    AND s.step_id = h.step_id 
    AND h.step_id <> 0
WHERE j.enabled = 1 AND
DATEADD(second, ((run_duration/10000 * 3600) + ((run_duration%10000)/100*60) + (run_duration%10000)%100), msdb.dbo.agent_datetime(run_date, run_time)) >= @EventStartTime  AND
DATEADD(second, ((run_duration/10000 * 3600) + ((run_duration%10000)/100*60) + (run_duration%10000)%100), msdb.dbo.agent_datetime(run_date, run_time)) < @EventEndTime  AND
h.run_status <> 4