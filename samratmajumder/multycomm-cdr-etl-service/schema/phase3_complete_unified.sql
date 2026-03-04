
-- ===================================================================
-- CDR DATABASE MODEL - PHASE 3 COMPLETE UNIFIED SCHEMA
-- ===================================================================
-- 
-- This unified schema combines all Phase 2 and Phase 3 enhancements
-- into a single, comprehensive database structure for CDR analytics.
-- 
-- DEPLOYMENT INSTRUCTIONS:
-- 1. Backup your existing database before running this script
-- 2. Run this script on your target database
-- 3. Update ETL scripts to use the new unified structure
-- 4. Run verification queries to ensure data integrity
-- 
-- FEATURES:
-- - Complete star schema with all dimensions and facts
-- - ERP/Zoho integration capabilities
-- - Campaign analytics and customer journey tracking
-- - Call quality monitoring and performance metrics
-- - Infrastructure monitoring and compliance tracking
-- ===================================================================

-- Set proper SQL mode for compatibility
SET sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO';

-- ===================================================================
-- SECTION 1: CORE DIMENSIONS (Phase 2 + Enhanced)
-- ===================================================================

-- Date dimension for temporal analytics
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY COMMENT 'YYYYMMDD format',
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter TINYINT NOT NULL,
    month TINYINT NOT NULL,
    day_of_week VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN DEFAULT FALSE,
    fiscal_year INT,
    fiscal_quarter TINYINT,
    
    INDEX idx_year (year),
    INDEX idx_month (month),
    INDEX idx_quarter (quarter),
    INDEX idx_weekend (is_weekend)
) COMMENT='Date dimension for temporal analytics';

-- Time of day dimension for hourly analytics
CREATE TABLE IF NOT EXISTS dim_time_of_day (
    time_key INT PRIMARY KEY COMMENT 'HHMMSS format',
    full_time TIME NOT NULL,
    hour TINYINT NOT NULL,
    minute TINYINT NOT NULL,
    second TINYINT DEFAULT 0,
    time_period ENUM('Early Morning', 'Morning', 'Afternoon', 'Evening', 'Night') NOT NULL,
    business_hours BOOLEAN DEFAULT FALSE,
    
    INDEX idx_hour (hour),
    INDEX idx_time_period (time_period),
    INDEX idx_business_hours (business_hours)
) COMMENT='Time dimension for hourly analytics';

-- Enhanced user dimension with agent capabilities
CREATE TABLE IF NOT EXISTS dim_users (
    user_key INT AUTO_INCREMENT PRIMARY KEY,
    user_number VARCHAR(50) UNIQUE NOT NULL,
    user_name VARCHAR(200),
    country_code INT,
    country_name VARCHAR(100) DEFAULT 'Unknown',
    is_agent BOOLEAN DEFAULT FALSE,
    
    -- Phase 3 enhancements
    employee_id VARCHAR(50),
    department VARCHAR(100),
    team VARCHAR(100),
    skill_level ENUM('Beginner', 'Intermediate', 'Advanced', 'Expert'),
    languages JSON COMMENT 'Array of supported languages',
    hire_date DATE,
    agent_status ENUM('Active', 'Inactive', 'On Leave', 'Terminated') DEFAULT 'Active',
    supervisor_user_key INT,
    hourly_rate DECIMAL(8,2),
    max_concurrent_calls TINYINT DEFAULT 1,
    preferred_queues JSON COMMENT 'Array of preferred queue IDs',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_user_number (user_number),
    INDEX idx_is_agent (is_agent),
    INDEX idx_department (department),
    INDEX idx_team (team),
    INDEX idx_agent_status (agent_status),
    FOREIGN KEY (supervisor_user_key) REFERENCES dim_users(user_key)
) COMMENT='Enhanced user dimension with agent capabilities';

-- Call disposition dimension
CREATE TABLE IF NOT EXISTS dim_call_disposition (
    disposition_key INT AUTO_INCREMENT PRIMARY KEY,
    call_direction ENUM('INBOUND', 'OUTBOUND') NOT NULL,
    hangup_cause VARCHAR(50),
    disposition VARCHAR(100) NOT NULL,
    subdisposition_1 VARCHAR(100),
    subdisposition_2 VARCHAR(100),
    
    -- Enhanced categorization
    disposition_category ENUM('Answered', 'Abandoned', 'Busy', 'No Answer', 'Failed', 'Other'),
    requires_followup BOOLEAN DEFAULT FALSE,
    is_successful BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_call_direction (call_direction),
    INDEX idx_disposition (disposition),
    INDEX idx_category (disposition_category),
    INDEX idx_successful (is_successful)
) COMMENT='Call disposition dimension with enhanced categorization';

-- System dimension for infrastructure tracking
CREATE TABLE IF NOT EXISTS dim_system (
    system_key INT AUTO_INCREMENT PRIMARY KEY,
    switch_hostname VARCHAR(200) NOT NULL,
    app_name VARCHAR(100),
    realm VARCHAR(100),
    
    -- Enhanced system information
    system_type ENUM('Primary', 'Secondary', 'Backup', 'Test'),
    location VARCHAR(100),
    maintenance_window VARCHAR(50),
    system_status ENUM('Active', 'Maintenance', 'Offline') DEFAULT 'Active',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_switch_hostname (switch_hostname),
    INDEX idx_system_type (system_type),
    INDEX idx_system_status (system_status)
) COMMENT='System dimension for infrastructure tracking';

-- Campaign dimension
CREATE TABLE IF NOT EXISTS dim_campaigns (
    campaign_key INT AUTO_INCREMENT PRIMARY KEY,
    campaign_id VARCHAR(100) UNIQUE NOT NULL,
    campaign_name VARCHAR(200) NOT NULL,
    
    -- Enhanced campaign information
    campaign_type ENUM('Inbound', 'Outbound', 'Preview', 'Predictive', 'Blended', 'Survey', 'Follow-up'),
    campaign_status ENUM('Active', 'Paused', 'Completed', 'Cancelled') DEFAULT 'Active',
    start_date DATE,
    end_date DATE,
    expected_volume INT,
    target_audience VARCHAR(200),
    campaign_owner VARCHAR(100),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_campaign_id (campaign_id),
    INDEX idx_campaign_type (campaign_type),
    INDEX idx_campaign_status (campaign_status)
) COMMENT='Campaign dimension with enhanced tracking';

-- Queue dimension
CREATE TABLE IF NOT EXISTS dim_queues (
    queue_key INT AUTO_INCREMENT PRIMARY KEY,
    queue_id VARCHAR(100) NOT NULL,
    queue_name VARCHAR(200) UNIQUE NOT NULL,
    
    -- Enhanced queue information
    queue_type ENUM('Inbound', 'Outbound', 'Blended', 'Support', 'Sales', 'Technical'),
    priority_level TINYINT DEFAULT 5 COMMENT '1=Highest, 10=Lowest',
    max_wait_time INT COMMENT 'Maximum wait time in seconds',
    service_level_target DECIMAL(5,2) COMMENT 'Service level target percentage',
    queue_manager VARCHAR(100),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_queue_id (queue_id),
    INDEX idx_queue_name (queue_name),
    INDEX idx_queue_type (queue_type),
    INDEX idx_priority_level (priority_level)
) COMMENT='Queue dimension with enhanced management';

-- ===================================================================
-- SECTION 2: PHASE 3 ENHANCED DIMENSIONS
-- ===================================================================

-- ERP Cases dimension for CRM integration
CREATE TABLE IF NOT EXISTS dim_erp_cases (
    erp_case_key INT AUTO_INCREMENT PRIMARY KEY,
    case_id VARCHAR(100) UNIQUE NOT NULL COMMENT 'ERP/Zoho case ID',
    case_type ENUM('Support', 'Sales', 'Lead', 'Complaint', 'Inquiry', 'Other') NOT NULL,
    case_status ENUM('Open', 'In Progress', 'Resolved', 'Closed', 'Cancelled') NOT NULL,
    priority ENUM('Low', 'Medium', 'High', 'Critical') NOT NULL,
    
    created_at DATETIME,
    updated_at DATETIME,
    resolved_at DATETIME,
    
    assigned_agent VARCHAR(50),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    description TEXT,
    resolution TEXT,
    
    customer_satisfaction_score TINYINT COMMENT '1-5 rating',
    resolution_time_hours INT COMMENT 'Time to resolution in hours',
    
    created_at_dim TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at_dim TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_case_id (case_id),
    INDEX idx_case_status (case_status),
    INDEX idx_priority (priority),
    INDEX idx_assigned_agent (assigned_agent),
    INDEX idx_case_type (case_type)
) COMMENT='ERP cases dimension for CRM integration';

-- Opportunities dimension for sales tracking
CREATE TABLE IF NOT EXISTS dim_opportunities (
    opportunity_key INT AUTO_INCREMENT PRIMARY KEY,
    opportunity_id VARCHAR(100) UNIQUE NOT NULL,
    lead_source VARCHAR(100),
    opportunity_type ENUM('Lead', 'Prospect', 'Qualified', 'Hot', 'Cold', 'Warm') NOT NULL,
    stage ENUM('New', 'Contacted', 'Qualified', 'Proposal', 'Negotiation', 'Closed Won', 'Closed Lost') NOT NULL,
    
    expected_value DECIMAL(12,2),
    probability TINYINT COMMENT 'Percentage probability',
    
    created_at DATETIME,
    expected_close_date DATE,
    actual_close_date DATE,
    
    assigned_agent VARCHAR(50),
    lead_score TINYINT COMMENT 'Lead scoring 1-100',
    
    industry VARCHAR(100),
    company_size ENUM('Small', 'Medium', 'Large', 'Enterprise'),
    notes TEXT,
    
    created_at_dim TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at_dim TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_opportunity_id (opportunity_id),
    INDEX idx_stage (stage),
    INDEX idx_assigned_agent (assigned_agent),
    INDEX idx_lead_source (lead_source),
    INDEX idx_opportunity_type (opportunity_type)
) COMMENT='Opportunities dimension for sales tracking';

-- Customers dimension for customer analytics
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(100) UNIQUE NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    customer_type ENUM('Individual', 'Business', 'Enterprise', 'Government') NOT NULL,
    
    phone_number VARCHAR(50),
    email VARCHAR(200),
    country VARCHAR(100),
    city VARCHAR(100),
    industry VARCHAR(100),
    company_name VARCHAR(200),
    
    customer_since DATE,
    lifetime_value DECIMAL(12,2),
    risk_level ENUM('Low', 'Medium', 'High') DEFAULT 'Medium',
    
    preferred_language VARCHAR(50),
    time_zone VARCHAR(50),
    notes TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_customer_id (customer_id),
    INDEX idx_phone_number (phone_number),
    INDEX idx_email (email),
    INDEX idx_customer_type (customer_type),
    INDEX idx_country (country),
    INDEX idx_risk_level (risk_level)
) COMMENT='Customers dimension for customer analytics';

-- Call quality dimension
CREATE TABLE IF NOT EXISTS dim_call_quality (
    quality_key INT AUTO_INCREMENT PRIMARY KEY,
    call_id VARCHAR(100) UNIQUE NOT NULL,
    
    audio_quality_score TINYINT COMMENT '1-5 rating',
    connection_quality ENUM('Excellent', 'Good', 'Fair', 'Poor') NOT NULL,
    
    dropped_call BOOLEAN DEFAULT FALSE,
    transfer_count TINYINT DEFAULT 0,
    hold_count TINYINT DEFAULT 0,
    total_hold_time INT DEFAULT 0 COMMENT 'Total hold time in seconds',
    silence_duration INT DEFAULT 0 COMMENT 'Silence duration in seconds',
    talk_over_instances TINYINT DEFAULT 0,
    
    background_noise_level ENUM('None', 'Low', 'Medium', 'High'),
    echo_detected BOOLEAN DEFAULT FALSE,
    latency_ms INT,
    packet_loss_percent DECIMAL(5,2),
    jitter_ms DECIMAL(8,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_call_id (call_id),
    INDEX idx_audio_quality_score (audio_quality_score),
    INDEX idx_connection_quality (connection_quality),
    INDEX idx_dropped_call (dropped_call)
) COMMENT='Call quality dimension for quality monitoring';

-- Media servers dimension
CREATE TABLE IF NOT EXISTS dim_media_servers (
    media_server_key INT AUTO_INCREMENT PRIMARY KEY,
    server_hostname VARCHAR(255) UNIQUE NOT NULL,
    server_ip VARCHAR(45),
    server_location VARCHAR(100),
    server_type ENUM('Primary', 'Secondary', 'Backup', 'Load Balancer'),
    maintenance_window VARCHAR(50),
    server_status ENUM('Active', 'Maintenance', 'Offline') DEFAULT 'Active',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_server_hostname (server_hostname),
    INDEX idx_server_type (server_type),
    INDEX idx_server_status (server_status)
) COMMENT='Media servers dimension for infrastructure monitoring';

-- Network paths dimension
CREATE TABLE IF NOT EXISTS dim_network_paths (
    path_key INT AUTO_INCREMENT PRIMARY KEY,
    switch_uri VARCHAR(255),
    switch_hostname VARCHAR(255),
    network_segment VARCHAR(100),
    path_quality ENUM('Excellent', 'Good', 'Fair', 'Poor'),
    
    latency_ms INT,
    packet_loss_percent DECIMAL(5,2),
    jitter_ms DECIMAL(8,2),
    bandwidth_mbps DECIMAL(10,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_switch_hostname (switch_hostname),
    INDEX idx_network_segment (network_segment),
    INDEX idx_path_quality (path_quality)
) COMMENT='Network paths dimension for network monitoring';

-- SIP details dimension
CREATE TABLE IF NOT EXISTS dim_sip_details (
    sip_key INT AUTO_INCREMENT PRIMARY KEY,
    channel_name VARCHAR(255),
    sip_protocol_version VARCHAR(20),
    codec_used VARCHAR(20),
    
    encryption_enabled BOOLEAN DEFAULT FALSE,
    nat_traversal BOOLEAN DEFAULT FALSE,
    rtp_proxy_used BOOLEAN DEFAULT FALSE,
    user_agent VARCHAR(255),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_channel_name (channel_name),
    INDEX idx_codec_used (codec_used),
    INDEX idx_encryption_enabled (encryption_enabled)
) COMMENT='SIP details dimension for protocol monitoring';

-- Recordings dimension
CREATE TABLE IF NOT EXISTS dim_recordings (
    recording_key INT AUTO_INCREMENT PRIMARY KEY,
    recording_file_name VARCHAR(255) UNIQUE NOT NULL,
    recording_url VARCHAR(1000),
    file_size_bytes BIGINT,
    duration_seconds INT,
    file_format VARCHAR(20),
    bitrate_kbps INT,
    storage_location VARCHAR(255),
    
    encryption_enabled BOOLEAN DEFAULT FALSE,
    retention_period_days INT,
    scheduled_deletion_date DATE,
    file_status ENUM('Active', 'Archived', 'Deleted', 'Corrupted') DEFAULT 'Active',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_recording_file_name (recording_file_name),
    INDEX idx_file_status (file_status),
    INDEX idx_scheduled_deletion_date (scheduled_deletion_date)
) COMMENT='Recordings dimension for recording management';

-- Privacy settings dimension
CREATE TABLE IF NOT EXISTS dim_privacy_settings (
    privacy_key INT AUTO_INCREMENT PRIMARY KEY,
    caller_screen_bit BOOLEAN DEFAULT FALSE,
    privacy_hide_name BOOLEAN DEFAULT FALSE,
    privacy_hide_number BOOLEAN DEFAULT FALSE,
    
    recording_consent BOOLEAN DEFAULT FALSE,
    data_retention_class ENUM('Standard', 'Extended', 'Permanent') DEFAULT 'Standard',
    gdpr_compliant BOOLEAN DEFAULT FALSE,
    pci_compliant BOOLEAN DEFAULT FALSE,
    hipaa_compliant BOOLEAN DEFAULT FALSE,
    
    consent_timestamp DATETIME,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_recording_consent (recording_consent),
    INDEX idx_gdpr_compliant (gdpr_compliant),
    INDEX idx_pci_compliant (pci_compliant)
) COMMENT='Privacy settings dimension for compliance tracking';

-- ===================================================================
-- SECTION 3: FACT TABLES
-- ===================================================================

-- Main fact table with Phase 3 enhancements
CREATE TABLE IF NOT EXISTS fact_calls (
    call_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    msg_id VARCHAR(100) NOT NULL,
    call_id VARCHAR(100) UNIQUE NOT NULL,
    
    -- Time dimensions
    date_key INT NOT NULL,
    time_key INT NOT NULL,
    
    -- User dimensions
    caller_user_key INT,
    callee_user_key INT,
    
    -- Call outcome dimensions
    disposition_key INT NOT NULL,
    system_key INT,
    
    -- Business context dimensions
    campaign_key INT,
    queue_key INT,
    
    -- Phase 3 enhanced dimensions
    erp_case_key INT,
    opportunity_key INT,
    customer_key INT,
    quality_key INT,
    media_server_key INT,
    network_path_key INT,
    sip_key INT,
    recording_key INT,
    privacy_key INT,
    
    -- Call metrics
    duration_seconds INT,
    billing_seconds INT,
    call_recording_url VARCHAR(1000),
    is_conference BOOLEAN DEFAULT FALSE,
    follow_up_notes TEXT,
    
    -- Phase 3 enhanced fields
    subdisposition_raw JSON COMMENT 'Raw subdisposition data for audit',
    campaign_interaction_key INT,
    p_cid VARCHAR(100) COMMENT 'P-CID from custom_sip_headers JSON',
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (time_key) REFERENCES dim_time_of_day(time_key),
    FOREIGN KEY (caller_user_key) REFERENCES dim_users(user_key),
    FOREIGN KEY (callee_user_key) REFERENCES dim_users(user_key),
    FOREIGN KEY (disposition_key) REFERENCES dim_call_disposition(disposition_key),
    FOREIGN KEY (system_key) REFERENCES dim_system(system_key),
    FOREIGN KEY (campaign_key) REFERENCES dim_campaigns(campaign_key),
    FOREIGN KEY (queue_key) REFERENCES dim_queues(queue_key),
    FOREIGN KEY (erp_case_key) REFERENCES dim_erp_cases(erp_case_key),
    FOREIGN KEY (opportunity_key) REFERENCES dim_opportunities(opportunity_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key),
    FOREIGN KEY (quality_key) REFERENCES dim_call_quality(quality_key),
    FOREIGN KEY (media_server_key) REFERENCES dim_media_servers(media_server_key),
    FOREIGN KEY (network_path_key) REFERENCES dim_network_paths(path_key),
    FOREIGN KEY (sip_key) REFERENCES dim_sip_details(sip_key),
    FOREIGN KEY (recording_key) REFERENCES dim_recordings(recording_key),
    FOREIGN KEY (privacy_key) REFERENCES dim_privacy_settings(privacy_key),
    
    -- Indexes for performance
    INDEX idx_date_key (date_key),
    INDEX idx_time_key (time_key),
    INDEX idx_call_id (call_id),
    INDEX idx_msg_id (msg_id),
    INDEX idx_caller_user_key (caller_user_key),
    INDEX idx_callee_user_key (callee_user_key),
    INDEX idx_disposition_key (disposition_key),
    INDEX idx_campaign_key (campaign_key),
    INDEX idx_queue_key (queue_key),
    INDEX idx_erp_case_key (erp_case_key),
    INDEX idx_opportunity_key (opportunity_key),
    INDEX idx_customer_key (customer_key),
    INDEX idx_quality_key (quality_key),
    INDEX idx_campaign_interaction_key (campaign_interaction_key),
    INDEX idx_p_cid (p_cid)
) COMMENT='Main fact table with comprehensive CDR data';

-- Campaign interactions fact table
CREATE TABLE IF NOT EXISTS fact_campaign_interactions (
    interaction_key INT AUTO_INCREMENT PRIMARY KEY,
    call_key BIGINT NOT NULL,
    
    campaign_id VARCHAR(100),
    campaign_name VARCHAR(200),
    campaign_type ENUM('Inbound', 'Outbound', 'Preview', 'Predictive', 'Blended'),
    
    queue_id VARCHAR(100),
    queue_name VARCHAR(200),
    
    lead_id VARCHAR(100),
    customer_name VARCHAR(200),
    customer_phone VARCHAR(50),
    agent_extension VARCHAR(50),
    customer_type VARCHAR(100),
    
    recording_filename VARCHAR(255),
    conference_id VARCHAR(100),
    
    caller_id_masked BOOLEAN DEFAULT FALSE,
    preview_transfer BOOLEAN DEFAULT FALSE,
    transfer_cid_number VARCHAR(50),
    transfer_cid_name VARCHAR(200),
    gateway VARCHAR(100),
    
    disposition_mandatory BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (call_key) REFERENCES fact_calls(call_key),
    
    INDEX idx_call_key (call_key),
    INDEX idx_campaign_id (campaign_id),
    INDEX idx_queue_id (queue_id),
    INDEX idx_lead_id (lead_id),
    INDEX idx_agent_extension (agent_extension),
    INDEX idx_customer_phone (customer_phone)
) COMMENT='Campaign interactions fact table for detailed campaign analytics';

-- Agent legs fact table for agent performance
CREATE TABLE IF NOT EXISTS fact_agent_legs (
    leg_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    call_key BIGINT NOT NULL,
    agent_key INT NOT NULL,
    disposition_key INT,
    
    wait_seconds INT DEFAULT 0,
    talk_seconds INT DEFAULT 0,
    wrap_up_seconds INT DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (call_key) REFERENCES fact_calls(call_key),
    FOREIGN KEY (agent_key) REFERENCES dim_users(user_key),
    FOREIGN KEY (disposition_key) REFERENCES dim_call_disposition(disposition_key),
    
    INDEX idx_call_key (call_key),
    INDEX idx_agent_key (agent_key),
    INDEX idx_disposition_key (disposition_key)
) COMMENT='Agent legs fact table for agent performance tracking';

-- Call legs fact table for complex call tracking
CREATE TABLE IF NOT EXISTS fact_call_legs (
    leg_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    call_key BIGINT NOT NULL,
    interaction_key INT,
    
    bridge_id VARCHAR(255),
    other_leg_call_id VARCHAR(255),
    leg_sequence TINYINT,
    leg_type ENUM('original', 'transfer', 'conference', 'outbound', 'callback') NOT NULL,
    
    leg_start_time DATETIME,
    leg_end_time DATETIME,
    leg_duration_seconds INT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (call_key) REFERENCES fact_calls(call_key),
    
    INDEX idx_call_key (call_key),
    INDEX idx_bridge_id (bridge_id),
    INDEX idx_other_leg_call_id (other_leg_call_id),
    INDEX idx_leg_type (leg_type)
) COMMENT='Call legs fact table for complex call tracking';

-- Call transfers fact table
CREATE TABLE IF NOT EXISTS fact_call_transfers (
    transfer_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_call_key BIGINT NOT NULL,
    target_call_key BIGINT NOT NULL,
    
    transfer_type ENUM('Blind', 'Attended', 'Conference', 'Warm', 'Cold'),
    transfer_initiated_by INT,
    transfer_timestamp DATETIME,
    transfer_reason VARCHAR(200),
    transfer_success BOOLEAN DEFAULT TRUE,
    transfer_duration_seconds INT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (source_call_key) REFERENCES fact_calls(call_key),
    FOREIGN KEY (target_call_key) REFERENCES fact_calls(call_key),
    FOREIGN KEY (transfer_initiated_by) REFERENCES dim_users(user_key),
    
    INDEX idx_source_call_key (source_call_key),
    INDEX idx_target_call_key (target_call_key),
    INDEX idx_transfer_initiated_by (transfer_initiated_by),
    INDEX idx_transfer_type (transfer_type)
) COMMENT='Call transfers fact table for transfer analytics';

-- System performance fact table
CREATE TABLE IF NOT EXISTS fact_system_performance (
    performance_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    measurement_date DATE NOT NULL,
    measurement_hour TINYINT NOT NULL,
    media_server_key INT NOT NULL,
    
    concurrent_calls INT DEFAULT 0,
    cpu_usage_percent DECIMAL(5,2),
    memory_usage_percent DECIMAL(5,2),
    disk_usage_percent DECIMAL(5,2),
    network_throughput_mbps DECIMAL(10,2),
    
    calls_per_hour INT DEFAULT 0,
    failed_calls_per_hour INT DEFAULT 0,
    average_call_duration DECIMAL(8,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (media_server_key) REFERENCES dim_media_servers(media_server_key),
    
    UNIQUE KEY uk_measurement (measurement_date, measurement_hour, media_server_key),
    INDEX idx_measurement_date (measurement_date),
    INDEX idx_media_server_key (media_server_key)
) COMMENT='System performance fact table for infrastructure monitoring';

-- ===================================================================
-- SECTION 4: STAGING TABLES
-- ===================================================================

-- Raw CDR data staging table
CREATE TABLE IF NOT EXISTS cdr_raw_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    msg_id VARCHAR(100) NOT NULL,
    record_data JSON NOT NULL,
    
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_processed_at TIMESTAMP NULL,
    
    INDEX idx_msg_id (msg_id),
    INDEX idx_etl_processed (etl_processed_at),
    INDEX idx_ingested_at (ingested_at)
) COMMENT='Raw CDR data staging table';

-- ===================================================================
-- SECTION 5: UTILITY FUNCTIONS AND PROCEDURES
-- ===================================================================

-- Procedure to populate date dimension
DELIMITER //
CREATE PROCEDURE IF NOT EXISTS PopulateDateDimension(
    IN start_date DATE,
    IN end_date DATE
)
BEGIN
    DECLARE current_date DATE DEFAULT start_date;
    
    WHILE current_date <= end_date DO
        INSERT IGNORE INTO dim_date (
            date_key, full_date, year, quarter, month, day_of_week,
            is_weekend, fiscal_year, fiscal_quarter
        ) VALUES (
            DATE_FORMAT(current_date, '%Y%m%d'),
            current_date,
            YEAR(current_date),
            QUARTER(current_date),
            MONTH(current_date),
            DAYNAME(current_date),
            CASE WHEN DAYOFWEEK(current_date) IN (1, 7) THEN TRUE ELSE FALSE END,
            YEAR(current_date),
            QUARTER(current_date)
        );
        
        SET current_date = DATE_ADD(current_date, INTERVAL 1 DAY);
    END WHILE;
END //
DELIMITER ;

-- Procedure to populate time dimension
DELIMITER //
CREATE PROCEDURE IF NOT EXISTS PopulateTimeDimension()
BEGIN
    DECLARE current_time TIME DEFAULT '00:00:00';
    DECLARE hour_val INT;
    DECLARE minute_val INT;
    DECLARE second_val INT;
    
    WHILE current_time < '24:00:00' DO
        SET hour_val = HOUR(current_time);
        SET minute_val = MINUTE(current_time);
        SET second_val = SECOND(current_time);
        
        INSERT IGNORE INTO dim_time_of_day (
            time_key, full_time, hour, minute, second,
            time_period, business_hours
        ) VALUES (
            hour_val * 10000 + minute_val * 100 + second_val,
            current_time,
            hour_val,
            minute_val,
            second_val,
            CASE 
                WHEN hour_val BETWEEN 0 AND 5 THEN 'Night'
                WHEN hour_val BETWEEN 6 AND 11 THEN 'Morning'
                WHEN hour_val BETWEEN 12 AND 17 THEN 'Afternoon'
                WHEN hour_val BETWEEN 18 AND 21 THEN 'Evening'
                ELSE 'Night'
            END,
            CASE WHEN hour_val BETWEEN 9 AND 17 THEN TRUE ELSE FALSE END
        );
        
        SET current_time = ADDTIME(current_time, '00:00:01');
    END WHILE;
END //
DELIMITER ;

-- ===================================================================
-- SECTION 6: INITIAL DATA POPULATION
-- ===================================================================

-- Populate date dimension for the next 2 years
CALL PopulateDateDimension(CURDATE(), DATE_ADD(CURDATE(), INTERVAL 2 YEAR));

-- Populate time dimension
CALL PopulateTimeDimension();

-- ===================================================================
-- SECTION 7: VERIFICATION QUERIES
-- ===================================================================

-- Verification query to check table creation
SELECT 
    TABLE_NAME,
    TABLE_ROWS,
    CREATE_TIME
FROM information_schema.TABLES 
WHERE TABLE_SCHEMA = DATABASE() 
    AND TABLE_NAME LIKE 'dim_%' OR TABLE_NAME LIKE 'fact_%'
ORDER BY TABLE_NAME;

-- Verification query to check foreign key constraints
SELECT 
    CONSTRAINT_NAME,
    TABLE_NAME,
    COLUMN_NAME,
    REFERENCED_TABLE_NAME,
    REFERENCED_COLUMN_NAME
FROM information_schema.KEY_COLUMN_USAGE 
WHERE TABLE_SCHEMA = DATABASE() 
    AND REFERENCED_TABLE_NAME IS NOT NULL
ORDER BY TABLE_NAME, CONSTRAINT_NAME;

-- ===================================================================
-- COMPLETION MESSAGE
-- ===================================================================

SELECT 'Phase 3 Complete Unified Schema deployed successfully!' as Status,
       COUNT(*) as Tables_Created
FROM information_schema.TABLES 
WHERE TABLE_SCHEMA = DATABASE() 
    AND (TABLE_NAME LIKE 'dim_%' OR TABLE_NAME LIKE 'fact_%');
