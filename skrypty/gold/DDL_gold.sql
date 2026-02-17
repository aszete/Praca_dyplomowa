-- gold metadata
IF OBJECT_ID('gold.metadata') IS NULL
BEGIN
    CREATE TABLE gold.metadata (
        log_id INT IDENTITY(1,1) PRIMARY KEY NONCLUSTERED, -- PK is usually non-clustered in logs
        table_name VARCHAR(100),
        source_layer VARCHAR(20) NOT NULL DEFAULT 'silver',
        load_start_time DATETIME2,
        load_end_time DATETIME2,
        duration_seconds AS DATEDIFF(SECOND, load_start_time, load_end_time),
        rows_inserted INT,
        status VARCHAR(20),
        error_message NVARCHAR(MAX)
    );
END;
