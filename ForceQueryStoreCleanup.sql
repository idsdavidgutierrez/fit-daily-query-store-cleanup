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
	A database is only included if size-based clean is enabled, time-based cleanup is disabled, and query store is in read-write mode.

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
CREATE OR ALTER PROCEDURE dbo.ForceQueryStoreCleanup (
	@MinutesToRun INT = 240,	
	@PercentageToKeep INT = 50 -- 50 is 50%
)
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	SET DEADLOCK_PRIORITY 10; -- quitting early can leave databases with the incorrect value for [max_storage_size_mb]

	-- by default, the Query Store data flush interval is set to 15 minutes by Microsoft
	-- we allow 35 minutes for size-based cleanup, which runs along with the data flush, to complete
	-- you may need to change this value if you use a non-default value for the Query Store data flush interval
	DECLARE @MinutesToPurgeOneDatabase INT = 35;

	DECLARE
		@DatabaseName SYSNAME,
		@Target_max_storage_size_mb BIGINT,
		@StepStartTimeUTC DATETIME,
		@DatabaseCount BIGINT,
		@StepDistanceSeconds INT,
		@SQLToCheckQueryStoreSettings NVARCHAR(4000),
		@SQLToChangeMaxStorageSize NVARCHAR(4000),
		@SleepSeconds INT,
		@SQLToSleep NVARCHAR(4000),
		@ErrorMessage NVARCHAR(2048);

	CREATE TABLE #DatabasesToCleanup (
		[DatabaseName] SYSNAME COLLATE DATABASE_DEFAULT NOT NULL,
		[max_storage_size_mb] BIGINT NOT NULL
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
	AND d.[state] = 0;

	OPEN DatabasesWithQueryStore;
	FETCH NEXT FROM DatabasesWithQueryStore INTO @DatabaseName;
	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @SQLToCheckQueryStoreSettings = N'INSERT INTO #DatabasesToCleanup ([DatabaseName], [max_storage_size_mb])
SELECT @DatabaseName, [max_storage_size_mb] 
FROM ' + QUOTENAME(@DatabaseName) + N'.sys.database_query_store_options
WHERE [actual_state] = 2 AND [stale_query_threshold_days] = 0 AND [size_based_cleanup_mode] = 1
OPTION (RECOMPILE)'; -- avoid plan cache pollution caused by using a temp table in dynamic SQL

		EXEC sp_executesql
			@SQLToCheckQueryStoreSettings,
			N'@DatabaseName SYSNAME',
			@DatabaseName;

		FETCH NEXT FROM DatabasesWithQueryStore INTO @DatabaseName;
	END;

	CLOSE DatabasesWithQueryStore;
	DEALLOCATE DatabasesWithQueryStore;

	SELECT @DatabaseCount = COUNT_BIG(*)
	FROM #DatabasesToCleanup;

	SET @StepDistanceSeconds =
	CASE
		WHEN @DatabaseCount > 1
		THEN 60 * (@MinutesToRun - @MinutesToPurgeOneDatabase) / (@DatabaseCount - 1)
		ELSE 0 
	END;


	DECLARE CleanupSteps CURSOR LOCAL FAST_FORWARD FOR
	SELECT
		[DatabaseName],
		ca.Target_max_storage_size_mb,
		StepStartTimeUTC
	FROM
	(
		SELECT 
			[DatabaseName],
			[max_storage_size_mb],
			-1 + ROW_NUMBER() OVER (ORDER BY [max_storage_size_mb] DESC, [DatabaseName] ASC) ZeroBasedRN
		FROM #DatabasesToCleanup
	) q
	CROSS APPLY (
		VALUES
			(
				DATEADD(SECOND, ZeroBasedRN * @StepDistanceSeconds, GETUTCDATE()),
				FLOOR(0.01 * @PercentageToKeep * [max_storage_size_mb])
			),
			(
				DATEADD(SECOND, 60 * @MinutesToPurgeOneDatabase + ZeroBasedRN * @StepDistanceSeconds, GETUTCDATE()),
				[max_storage_size_mb]
			)
	) ca (StepStartTimeUTC, Target_max_storage_size_mb)
	ORDER BY StepStartTimeUTC;

	OPEN CleanupSteps;
	FETCH NEXT FROM CleanupSteps INTO @DatabaseName, @Target_max_storage_size_mb, @StepStartTimeUTC;
	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF GETUTCDATE() < @StepStartTimeUTC
		BEGIN
			SET @SleepSeconds = 1 + DATEDIFF_BIG(SECOND, GETUTCDATE(), @StepStartTimeUTC);
			SET @SQLToSleep = N'WAITFOR DELAY ''' + CAST(CAST(DATEADD(SECOND, @SleepSeconds, '00:00:00') AS TIME(3)) AS NVARCHAR(20)) + '''';

			EXEC sp_executesql @SQLToSleep;
		END;

		SET @SQLToChangeMaxStorageSize = N'ALTER DATABASE ' + QUOTENAME(@DatabaseName) + N' SET QUERY_STORE
(MAX_STORAGE_SIZE_MB = ' + CAST(@Target_max_storage_size_mb AS NVARCHAR(20)) + N')';

		-- this can fail due to reasons beyond our control, such as lock timeouts
		-- "CREATE DATABASE, ALTER DATABASE, and DROP DATABASE statements do not honor the SET LOCK_TIMEOUT setting."
		BEGIN TRY
			EXEC sp_executesql @SQLToChangeMaxStorageSize;
		END TRY
		BEGIN CATCH
			INSERT INTO @Errors (DatabaseName, ErrorNumber, ErrorMessage)
			VALUES (@DatabaseName, ERROR_NUMBER(), ERROR_MESSAGE());
		END CATCH;

		FETCH NEXT FROM CleanupSteps INTO @DatabaseName, @Target_max_storage_size_mb, @StepStartTimeUTC;
	END;

	CLOSE CleanupSteps;
	DEALLOCATE CleanupSteps;


	-- if there was an error, try to reset [max_storage_size_mb]
	DECLARE DatabasesToReset CURSOR LOCAL FAST_FORWARD FOR
	SELECT d.DatabaseName, d.[max_storage_size_mb]
	FROM #DatabasesToCleanup d
	WHERE EXISTS (
		SELECT 1
		FROM @Errors e
		WHERE e.DatabaseName = d.DatabaseName
	);

	OPEN DatabasesToReset;
	FETCH NEXT FROM DatabasesToReset INTO @DatabaseName, @Target_max_storage_size_mb;
	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @SQLToChangeMaxStorageSize = N'ALTER DATABASE ' + QUOTENAME(@DatabaseName) + N' SET QUERY_STORE
(MAX_STORAGE_SIZE_MB = ' + CAST(@Target_max_storage_size_mb AS NVARCHAR(20)) + N')';

		BEGIN TRY
			EXEC sp_executesql @SQLToChangeMaxStorageSize;
		END TRY
		BEGIN CATCH
			-- :^)
		END CATCH;

		FETCH NEXT FROM DatabasesToReset INTO @DatabaseName, @Target_max_storage_size_mb;
	END;

	CLOSE DatabasesToReset;
	DEALLOCATE DatabasesToReset;

	IF EXISTS (SELECT 1 FROM @Errors)
	BEGIN
		SELECT @ErrorMessage = STRING_AGG(CONCAT(N'Error number ', ErrorNumber, N' for database ', QUOTENAME(DatabaseName), N': ', ErrorMessage), N' | ')		
		FROM @Errors;

		THROW 49218345, @ErrorMessage, 1;
	END;

	RETURN;
END;
GO
