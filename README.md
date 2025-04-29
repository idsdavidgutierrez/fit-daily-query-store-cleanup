Automatic time-based and size-based Query Store cleanup can cause transaction log file size spikes which can lead to increased Availability Group latency on some systems. This procedure is designed to force Query Store clean up to occur outside of business hours and spreads out the work to minimize latency. 

A database is only included if query store is in read-write mode, size-based cleanip is enabled, and time-based cleanup is disabled.

Tested on SQL Server 2022 and SQL Server 2019 but is likely to work on SQL Server 2017 and SQL Server 2016 as well.

You can read about Microsoft's size-based cleanup here: https://learn.microsoft.com/en-us/sql/relational-databases/performance/manage-the-query-store?view=sql-server-ver16&tabs=ssms#query-store-maximum-size
