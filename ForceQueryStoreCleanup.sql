SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
GO
/*
Name: ForceQueryStoreCleanup
Purpose:
	Uses sized-based cleanup to stagger query store purging during a defined time period set by an administrator.
	It is recommended to schedule ForceQueryStoreCleanup to run daily outside of business hours.
	A database is only included if query store is in read-write mode, size-based cleanip is enabled, and time-based cleanup is disabled.

	For example, consider running the procedure with default parameters and processing a database with a 2000 MB max size.
	The database's max size will be temporarily reduced to 0.5 * 2000 = 1000 MB for the purposes of cleanup.
	If there is more than 900 MB (90%) of query store data then SQL Server's cleanup process will delete data until 800 MB (80%) remains.
	The database's max size is then restored to 2000 MB.

	You can read about Microsoft's size-based cleanup here: https://learn.microsoft.com/en-us/sql/relational-databases/performance/manage-the-query-store?view=sql-server-ver16&tabs=ssms#query-store-maximum-size
License: MIT
Author: Joe Obbish
Full Source Code: https://github.com/idsdavidgutierrez/fit-daily-query-store-cleanup
Parameters:
	@MinutesToRun - the maximum runtime of the procedure. query store cleanup will be staggered during execution
	@PercentageToKeep - the percentage of the query store's max size to keep for cleanup
*/
CREATE OR ALTER PROCEDURE [ForceQueryStoreCleanup] (
	@MinutesToRun INT = 240,	
	@PercentageToKeep INT = 50 -- 50 is 50%
)
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	SET DEADLOCK_PRIORITY -10; -- we retry attempts to change the query store max storage size so let other processes win

	-- by default, the Query Store data flush interval is set to 15 minutes by Microsoft
	-- we allow 60 minutes for size-based cleanup, which runs along with the data flush, to complete
	-- you may need to change this value if you use a non-default value for the Query Store data flush interval
	-- or size-based cleanup takes longer than expected on your system
	-- TODO: use extended events to figure out when size-based cleanup finishes instead of guessing
	DECLARE @MinutesToPurgeOneDatabase INT = 60;

	DECLARE
		@DatabaseName SYSNAME,
		@Target_max_storage_size_mb BIGINT,
		@New_max_storage_size_mb BIGINT,
		@ProcedureStopTimeUTC DATETIME,
		@ProcedureRanOutofTime BIT,
		@QueryStoreSizeDecreased BIT,
		@SQLToCheckQueryStoreSettings NVARCHAR(4000),
		@SQLToChangeMaxStorageSize NVARCHAR(4000),
		@SQLToVerifyQueryStoreSettings NVARCHAR(4000),
		@ErrorMessage NVARCHAR(2048);

	SET @ProcedureStopTimeUTC = DATEADD(MINUTE, @MinutesToRun, GETUTCDATE());

	DECLARE @DatabasesToCleanup TABLE (
		[DatabaseName] SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
		[max_storage_size_mb] BIGINT NOT NULL,
		PRIMARY KEY ([DatabaseName])
	);

	DECLARE @Errors TABLE (
		DatabaseName SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
		ErrorNumber INT NULL,
		ErrorMessage NVARCHAR(4000) COLLATE DATABASE_DEFAULT NULL
	);


	DECLARE DatabasesWithQueryStore CURSOR LOCAL FAST_FORWARD FOR
	SELECT d.[name]
	FROM sys.databases d
	WHERE d.[is_query_store_on] = 1
	AND d.[state] = 0
	ORDER BY d.[name];

	OPEN DatabasesWithQueryStore;
	FETCH NEXT FROM DatabasesWithQueryStore INTO @DatabaseName;
	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @SQLToCheckQueryStoreSettings = N'SELECT @Target_max_storage_size_mb = [max_storage_size_mb] 
FROM ' + QUOTENAME(@DatabaseName) + N'.sys.database_query_store_options
WHERE [actual_state] = 2 AND [stale_query_threshold_days] = 0 AND [size_based_cleanup_mode] = 1';

		SET @Target_max_storage_size_mb = NULL;
		EXEC sp_executesql
			@SQLToCheckQueryStoreSettings,
			N'@Target_max_storage_size_mb BIGINT OUTPUT',
			@Target_max_storage_size_mb OUTPUT;

		IF @Target_max_storage_size_mb IS NOT NULL
		BEGIN		
			INSERT INTO @DatabasesToCleanup ([DatabaseName], [max_storage_size_mb])
			VALUES (@DatabaseName, @Target_max_storage_size_mb);
		END;

		FETCH NEXT FROM DatabasesWithQueryStore INTO @DatabaseName;
	END;

	CLOSE DatabasesWithQueryStore;
	DEALLOCATE DatabasesWithQueryStore;

	IF NOT EXISTS (SELECT 1 FROM @DatabasesToCleanup)
	BEGIN
		RETURN;
	END;
	
	DECLARE @CleanupSteps TABLE (
		[DatabaseName] SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
		[max_storage_size_mb] BIGINT NOT NULL,
		[StepStartTimeUTC] DATETIME NOT NULL,
		[QueryStoreSizeDecreased] BIT NOT NULL,
		PRIMARY KEY ([DatabaseName])
	);

	-- try to evenly spread out the work
	INSERT INTO @CleanupSteps ([DatabaseName], [max_storage_size_mb], [StepStartTimeUTC], [QueryStoreSizeDecreased])
	SELECT
		[DatabaseName],
		FLOOR(0.01 * @PercentageToKeep * [max_storage_size_mb]),
		DATEADD(SECOND, ZeroBasedRN * 60.0 * (@MinutesToRun - @MinutesToPurgeOneDatabase) / (SELECT COUNT_BIG(*) FROM @DatabasesToCleanup), GETUTCDATE()),
		1
	FROM
	(
		SELECT 
			[DatabaseName],
			[max_storage_size_mb],
			-1 + ROW_NUMBER() OVER (ORDER BY [max_storage_size_mb] DESC, [DatabaseName] ASC) ZeroBasedRN
		FROM @DatabasesToCleanup
	) q;

	WHILE GETUTCDATE() <= @ProcedureStopTimeUTC AND EXISTS (SELECT 1 FROM @CleanupSteps)
	BEGIN
		WHILE EXISTS (SELECT 1 FROM @CleanupSteps WHERE [StepStartTimeUTC] <= GETUTCDATE())
		BEGIN
			SELECT TOP (1)
				@DatabaseName = DatabaseName,
				@Target_max_storage_size_mb = [max_storage_size_mb],
				@QueryStoreSizeDecreased = [QueryStoreSizeDecreased]
			FROM @CleanupSteps
			WHERE [StepStartTimeUTC] <= GETUTCDATE()
			ORDER BY StepStartTimeUTC ASC;

			SET @SQLToChangeMaxStorageSize = N'ALTER DATABASE ' + QUOTENAME(@DatabaseName) + N' SET QUERY_STORE
(MAX_STORAGE_SIZE_MB = ' + CAST(@Target_max_storage_size_mb AS NVARCHAR(20)) + N')';

			SET @SQLToVerifyQueryStoreSettings = N'SELECT @New_max_storage_size_mb = [max_storage_size_mb] 
FROM ' + QUOTENAME(@DatabaseName) + N'.sys.database_query_store_options';

			SET @New_max_storage_size_mb = NULL;
			BEGIN TRY
			-- changing the max storage size can fail due to reasons beyond our control, such as lock timeouts
			-- "CREATE DATABASE, ALTER DATABASE, and DROP DATABASE statements do not honor the SET LOCK_TIMEOUT setting."
			-- to avoid 1222 errors, cancel the attempt after 5 seconds
				EXEC RunStatementSyncWithTimeLimit
					N'[ForceQueryStoreCleanup]', 
					@SQLToChangeMaxStorageSize,
					5;

				-- verify that the max storage size was changed by the temporary agent job
				-- instead of trying to read errors from the temporary agent job
				EXEC sp_executesql
					@SQLToVerifyQueryStoreSettings,
					N'@New_max_storage_size_mb BIGINT OUTPUT',
					@New_max_storage_size_mb OUTPUT;
			END TRY
			BEGIN CATCH
				INSERT INTO @Errors (DatabaseName, ErrorNumber, ErrorMessage)
				VALUES (@DatabaseName, ERROR_NUMBER(), ERROR_MESSAGE());
			END CATCH;

			IF @New_max_storage_size_mb = @Target_max_storage_size_mb
			BEGIN
				DELETE FROM @CleanupSteps
				WHERE [DatabaseName] = @DatabaseName;

				-- create another cleanup step to restore the max storage size back to the original value
				IF @QueryStoreSizeDecreased = 1
				BEGIN
					INSERT INTO @CleanupSteps ([DatabaseName], [max_storage_size_mb], [StepStartTimeUTC], [QueryStoreSizeDecreased])
					SELECT @DatabaseName, [max_storage_size_mb], DATEADD(MINUTE, @MinutesToPurgeOneDatabase, GETUTCDATE()), 0
					FROM @DatabasesToCleanup
					WHERE DatabaseName = @DatabaseName;
				END;
			END
			ELSE
			BEGIN
				-- changing the max storage size failed for some reason, so try again shortly
				UPDATE @CleanupSteps
				SET StepStartTimeUTC = DATEADD(SECOND, 30, StepStartTimeUTC)
				WHERE [DatabaseName] = @DatabaseName;
			END;
		END;

		WAITFOR DELAY '00:00:30';
	END;


	-- if we ran out of time then try to reset max storage to the original value for the remaining databases
	SET DEADLOCK_PRIORITY 10; -- failing agent jobs at this point can leave databases with the incorrect value for [max_storage_size_mb]

	DECLARE DatabasesToReset CURSOR LOCAL FAST_FORWARD FOR
	SELECT d.DatabaseName, d.[max_storage_size_mb]
	FROM @DatabasesToCleanup d
	WHERE EXISTS (
		SELECT 1
		FROM @CleanupSteps c
		WHERE c.DatabaseName = d.DatabaseName
	);

	OPEN DatabasesToReset;
	FETCH NEXT FROM DatabasesToReset INTO @DatabaseName, @Target_max_storage_size_mb;
	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @ProcedureRanOutofTime = 1;
		SET @SQLToChangeMaxStorageSize = N'ALTER DATABASE ' + QUOTENAME(@DatabaseName) + N' SET QUERY_STORE
(MAX_STORAGE_SIZE_MB = ' + CAST(@Target_max_storage_size_mb AS NVARCHAR(20)) + N')';

		-- try a 15 second timeout for the last attempt
		-- don't do more than one attempt per database because if we get to this point
		-- then changing the max storage size failed many times already
		BEGIN TRY
			EXEC RunStatementSyncWithTimeLimit
				N'[ForceQueryStoreCleanup]', 
				@SQLToChangeMaxStorageSize,
				15;
		END TRY
		BEGIN CATCH
			INSERT INTO @Errors (DatabaseName, ErrorNumber, ErrorMessage)
			VALUES (@DatabaseName, ERROR_NUMBER(), ERROR_MESSAGE());
		END CATCH;

		FETCH NEXT FROM DatabasesToReset INTO @DatabaseName, @Target_max_storage_size_mb;
	END;

	CLOSE DatabasesToReset;
	DEALLOCATE DatabasesToReset;

	IF EXISTS (SELECT 1 FROM @Errors)
	BEGIN
		SELECT @ErrorMessage = CASE WHEN @ProcedureRanOutofTime = 1 THEN N'We ran out of time to process all databases. ' ELSE N'' END +		
			STRING_AGG(CONCAT(N'Error number ', ErrorNumber, N' for database ', QUOTENAME(DatabaseName), N': ', ErrorMessage), N' | ')		
		FROM @Errors;

		-- note: this may be crowded out by "Job '%' started successfully." messages, so you may want to log errors using a different method
		THROW 49218345, @ErrorMessage, 1;
	END;

	RETURN;
END;
