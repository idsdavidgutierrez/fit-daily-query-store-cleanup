SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO
/*
Name: RunStatementSyncWithTimeLimit
Purpose:
	Creates a temporarily SQL agent job to run a SQL command.
	The agent job stops if it is still running after @TimeLimitInSeconds seconds.
	The original use case of this procedure was to run an ALTER DATABASE command at low priority to avoid 1222 errors.
	Errors from the agent job itself will not be reported so you should verify that the code ran successfully.
	It is not recommended to put complicated T-SQL in @SQLCommand, especially if the code has multiple transactions.
License: MIT
Author: Joe Obbish
Full Source Code: https://github.com/idsdavidgutierrez/fit-daily-query-store-cleanup
Parameters:
	@JobDescription - short description added to the temporary job name
	@SQLCommand - T-SQL command to run as part of the agent job
	@TimeLimitInSeconds - the number of seconds to wait before stopping the agent job, if necessary
*/
CREATE OR ALTER PROCEDURE [RunStatementSyncWithTimeLimit] (
	@JobDescription NVARCHAR(50),
	@SQLCommand NVARCHAR(4000),
	@TimeLimitInSeconds INT
)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE
		@JobName SYSNAME = N'_' + @JobDescription + N'_' + CONVERT(NVARCHAR(36), NEWID()),
		@StopTimeUTC DATETIME;

	EXEC msdb.dbo.sp_add_job
		@job_name = @JobName,
		@description = @JobDescription,
		@delete_level = 3; -- always delete the job no matter what

	EXEC msdb.dbo.sp_add_jobstep
		@job_name = @JobName,
		@step_name = N'Statement from RunStatementSyncWithTimeLimit',
		@command = @SQLCommand,
		@database_name = N'master';

	EXEC msdb.dbo.sp_add_jobserver @job_name = @JobName;

	EXEC msdb.dbo.sp_start_job @job_name = @JobName;

	SET @StopTimeUTC = DATEADD(SECOND, @TimeLimitInSeconds, GETUTCDATE());
	
	WHILE GETUTCDATE() <= @StopTimeUTC AND EXISTS (
		SELECT 1
		FROM msdb.dbo.sysjobs
		WHERE name = @JobName
	)
	BEGIN
		WAITFOR DELAY '00:00:01';
	END;

	IF GETUTCDATE() > @StopTimeUTC
	BEGIN
		BEGIN TRY
			-- in some situations, sp_stop_job will not always stop the job
			-- if you find yourself in this situation, you may want to add a KILL statement as well
			EXEC msdb.dbo.sp_stop_job @job_name = @JobName;
			EXEC msdb.dbo.sp_delete_job @job_name = @JobName;
		END TRY
		BEGIN CATCH
			-- job might have stopped on its own before we could call stop, so ignore some errors:
			-- "The specified @job_name ('%') does not exist."
			-- "SQLServerAgent Error: Request to stop job % (from %) refused because the job is not currently running."
			IF ERROR_NUMBER() NOT IN (22022, 14262)
			BEGIN;
				THROW;
			END;
		END CATCH;
	END;

	RETURN;
END;
