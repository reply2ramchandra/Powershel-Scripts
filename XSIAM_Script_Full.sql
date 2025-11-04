USE [master]

GO

CREATE SERVER AUDIT [Server_Audit_Specific]
TO FILE 
(	FILEPATH = N'D:\SQLAudit'
	,MAXSIZE = 100 MB
	,MAX_FILES = 10
	,RESERVE_DISK_SPACE = OFF
) WITH (QUEUE_DELAY = 1000, ON_FAILURE = CONTINUE)

GO



ALTER SERVER AUDIT Server_Audit_Specific
WITH (STATE = ON);

--************************ Server Audit Specifications CONFIGURATION ******************
USE [master]
GO

CREATE SERVER AUDIT SPECIFICATION [ServerAuditSpecification]
FOR SERVER AUDIT [Server_Audit_Specific]
ADD (DATABASE_OBJECT_ACCESS_GROUP),
ADD (SCHEMA_OBJECT_ACCESS_GROUP),
ADD (DATABASE_ROLE_MEMBER_CHANGE_GROUP),
ADD (SERVER_ROLE_MEMBER_CHANGE_GROUP),
ADD (AUDIT_CHANGE_GROUP),
ADD (FAILED_DATABASE_AUTHENTICATION_GROUP),
ADD (DATABASE_LOGOUT_GROUP),
ADD (SUCCESSFUL_DATABASE_AUTHENTICATION_GROUP),
ADD (DATABASE_OBJECT_PERMISSION_CHANGE_GROUP),
ADD (SCHEMA_OBJECT_PERMISSION_CHANGE_GROUP),
ADD (SERVER_OBJECT_PERMISSION_CHANGE_GROUP),
ADD (DATABASE_PRINCIPAL_IMPERSONATION_GROUP),
ADD (SERVER_PRINCIPAL_IMPERSONATION_GROUP),
ADD (FAILED_LOGIN_GROUP),
ADD (SUCCESSFUL_LOGIN_GROUP),
ADD (LOGOUT_GROUP),
ADD (DATABASE_CHANGE_GROUP),
ADD (DATABASE_OBJECT_CHANGE_GROUP),
ADD (DATABASE_PRINCIPAL_CHANGE_GROUP),
ADD (SCHEMA_OBJECT_CHANGE_GROUP),
ADD (SERVER_OBJECT_CHANGE_GROUP),
ADD (SERVER_PRINCIPAL_CHANGE_GROUP),
ADD (APPLICATION_ROLE_CHANGE_PASSWORD_GROUP),
ADD (LOGIN_CHANGE_PASSWORD_GROUP),
ADD (DATABASE_OWNERSHIP_CHANGE_GROUP),
ADD (DATABASE_OBJECT_OWNERSHIP_CHANGE_GROUP),
ADD (SCHEMA_OBJECT_OWNERSHIP_CHANGE_GROUP),
ADD (SERVER_OBJECT_OWNERSHIP_CHANGE_GROUP),
ADD (USER_CHANGE_PASSWORD_GROUP),
ADD (USER_DEFINED_AUDIT_GROUP)
WITH (STATE = ON)
GO

--****************************** Create SQL Authenticate login *****************
USE [master]
GO
CREATE LOGIN [XSIAMAudit] WITH PASSWORD=N'G7v!rX2#pL9@tQ', DEFAULT_DATABASE=[master], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO
USE [Admin]
GO
CREATE USER [XSIAMAudit] FOR LOGIN [XSIAMAudit]
GO
USE [Admin]
GO
ALTER ROLE [db_datareader] ADD MEMBER [XSIAMAudit]
GO



--****************** Create a Table to Store Audit Data ******************
USE Admin
GO

CREATE TABLE AuditLogData ( 

    event_time DATETIME, 

    action_id NVARCHAR(100), 

    succeeded BIT, 

    session_id INT, 

    server_principal_name NVARCHAR(100), 

    database_name NVARCHAR(100), 

    statement NVARCHAR(MAX) 
); 
go

WAITFOR DELAY '00:00:5';
go

--*************************** Insert Data from Audit File ************************
--Replace the path with your actual audit log folder: 

USE Admin
GO

INSERT INTO AuditLogData 

SELECT 

    event_time, 

    action_id, 

    succeeded, 

    session_id, 

    server_principal_name, 

    database_name, 

    statement 

FROM sys.fn_get_audit_file('D:\SQLAudit\\*.sqlaudit', DEFAULT, DEFAULT); 
go


WAITFOR DELAY '00:00:10';
go


-- ******************************** Create Job ********************************
USE [msdb]
GO

/****** Object:  Job [XSIAM_ReadAuditLogs]    Script Date: 8/6/2025 1:42:31 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 8/6/2025 1:42:31 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'XSIAM_ReadAuditLogs', 
		@enabled=1, 
		@notify_level_eventlog=2, 
		@notify_level_email=2, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'Read audit logs and update the update the AuduitlogData table', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'sa', 
		--@notify_email_operator_name=N'ICT_SQL', 
		@job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Insert audit log data into AuditLogData table]    Script Date: 8/6/2025 1:42:31 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Insert audit log data into AuditLogData table', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE Admin
GO

INSERT INTO AuditLogData (event_time, action_id, succeeded, session_id, server_principal_name, database_name, statement) 

SELECT 

    event_time, 

    action_id, 

    succeeded, 

    session_id, 

    server_principal_name, 

    database_name, 

    statement 

FROM sys.fn_get_audit_file(''D:\SQLAudit\\*.sqlaudit'', DEFAULT, DEFAULT); ', 
		@database_name=N'Admin', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Delete older than 7 days data]    Script Date: 8/6/2025 1:42:31 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Delete older than 7 days data', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'USE Admin
GO
DECLARE @BatchSize INT = 10000; 
 
 WHILE 1 = 1 
 BEGIN 
     DELETE TOP (@BatchSize) 
     FROM AuditLogData 
     WHERE event_time < DATEADD(DAY, -7, GETDATE()); 
 
     IF @@ROWCOUNT < @BatchSize 
         BREAK; 
     -- Optional: Add a delay to reduce load 
  
 END;', 
--DELETE FROM AuditLogData
--WHERE event_time < DATEADD(DAY, -7, GETDATE());', 
		@database_name=N'Admin', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'@ Every 8 Hours', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=8, 
		@freq_subday_interval=8, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20250801, 
		@active_end_date=99991231, 
		@active_start_time=700, 
		@active_end_time=235959, 
		@schedule_uid=N'da4ea54a-aa96-40c5-90bf-a5580b93105f'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO
-- ************************** Count from Table before job ****************
	USE Admin
	GO

	Select count(*) as Row_count_before_job_start from AuditLogData;
	go


-- ********************** Start Job ****************
EXEC msdb.dbo.sp_start_job @job_name = 'XSIAM_ReadAuditLogs';
go

-- ******************Job execution status  *******************

WAITFOR DELAY '00:00:15';
go


SELECT TOP 1
    j.name AS JobName,
    --h.run_date AS LastRunDate,
    --h.run_time AS LastRunTime,
    CASE h.run_status
        WHEN 0 THEN 'Failed'
        WHEN 1 THEN 'Succeeded'
        WHEN 2 THEN 'Retry'
        WHEN 3 THEN 'Canceled'
        WHEN 4 THEN 'In Progress'
        ELSE 'Unknown'
    END AS LastRunStatus
    --h.message AS RunMessage
FROM 
    msdb.dbo.sysjobs j
INNER JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
WHERE 
    j.name = 'XSIAM_ReadAuditLogs'
    AND h.step_id = 0  -- Only job outcome, not individual steps
ORDER BY 
    h.run_date DESC, h.run_time DESC;
	go

    	-- ************************** Count from Table after job ****************
	USE Admin
	GO

	Select count(*) as Row_count_After_job_complete from AuditLogData;
	go


--***********************************Port and IP ****************
SELECT 
    local_net_address AS IPAddress,
    local_tcp_port AS Port
FROM 
    sys.dm_exec_connections
WHERE 
    session_id = @@SPID;
	go
	-- ********************** Start Job ****************
EXEC msdb.dbo.sp_start_job @job_name = 'XSIAM_ReadAuditLogs';
go

WAITFOR DELAY '00:00:15';
go

    	-- ************************** Count from Table after job ****************
	USE Admin
	GO

	Select count(*) as Row_count_After_job_complete_2nd_time from AuditLogData;
	go

	--*************************************** Create a View for Easy Access **************************
USE Admin
GO

CREATE VIEW vw_AuditLogSummary AS 

SELECT 

    event_time, 

    server_principal_name, 

    database_name, 

    action_id, 

    succeeded, 

    statement 

FROM AuditLogData; 
go
