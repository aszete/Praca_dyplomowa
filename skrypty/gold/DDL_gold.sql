-- Gold metadata
CREATE TABLE gold.metadata (
    metadata_id INT IDENTITY(1,1) PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    table_type VARCHAR(50) NULL, -- 'fact', 'dimension', 'aggregate'
    load_start_time DATETIME2 NULL,
    load_end_time DATETIME2 NULL,
    batch_id VARCHAR(50) NOT NULL,
    rows_processed INT NULL,
    status VARCHAR(50) NOT NULL,
    error_message VARCHAR(MAX) NULL,
    created_at DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- YES - Add indexes
CREATE INDEX IX_gold_metadata_table_status 
ON gold.metadata(table_name, status, load_start_time DESC);
