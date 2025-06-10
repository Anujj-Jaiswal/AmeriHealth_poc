Create Database Prototype_db;
Use Prototype_db;


-- Create table for pipeline logs
CREATE TABLE PipelineLogs (
    Pipeline_name VARCHAR(100),
    Run_start DATETIME,
    Run_end DATETIME,
    Duration VARCHAR(20),
    Triggered VARCHAR(50),
    Status VARCHAR(50),
    Error TEXT,
    Run VARCHAR(100),
    Parameter VARCHAR(100),
    Run_ID VARCHAR(100)
);

-- Insert sample data
INSERT INTO PipelineLogs VALUES
('Pipeline_9', '2025-06-01 18:23:00', '2025-06-01 19:53:00', '1:30:00', 'Event', 'Cancelled', 'OutOfMemoryError', 'Pipeline_9_run_1', 'execution_type=Medicaid', 'run_80952'),
('Pipeline_7', '2025-06-02 13:04:00', '2025-06-02 14:29:00', '1:25:00', 'Manual', 'Failed', 'Timeout Error on Job 2', 'Pipeline_7_run_2', 'execution_type=Medicaid', 'run_40815'),
('Pipeline_3', '2025-06-03 19:35:00', '2025-06-03 20:44:00', '1:09:00', 'Manual', 'Queued', NULL, 'Pipeline_3_run_3', 'execution_type=Medicaid', 'run_35111'),
('Pipeline_9', '2025-06-03 16:15:00', '2025-06-03 17:07:00', '0:52:00', 'Schedule', 'Success', NULL, 'Pipeline_9_run_4', 'execution_type=Medicare', 'run_15464'),
('Pipeline_7', '2025-06-01 23:46:00', '2025-06-02 01:02:00', '1:16:00', 'Manual', 'Queued', NULL, 'Pipeline_7_run_5', 'execution_type=Medicaid', 'run_60536'),
('Pipeline_8', '2025-06-02 20:42:00', '2025-06-02 20:58:00', '0:16:00', 'Manual', 'Success', NULL, 'Pipeline_8_run_6', 'execution_type=Medicaid', 'run_21663');

Select * from PipelineLogs;


