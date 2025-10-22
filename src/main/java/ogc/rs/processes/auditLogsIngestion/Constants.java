package ogc.rs.processes.auditLogsIngestion;

public class Constants {
    public static final String STARTING_AUDIT_LOGS_INGESTION_PROCESS_MESSAGE =
            "Starting Audit Logs Ingestion process.";
    public static final String LOG_ENTRY_EMPTY_MESSAGE =
            "One of the logs entries is empty.";
    public static final String INVALID_LOG_FORMAT_MESSAGE =
            "Invalid log format in one of the log entries : Expected 6 fields (userId|collectionId|itemId|timestamp|respSize|storageBackend)";
    public static final String MISSING_REQUIRED_FIELDS_MESSAGE =
            "Missing required fields in log entry.";
    public static final String INVALID_UUID_FORMAT_MESSAGE =
            "Invalid UUID format in one of the log entries.";
    public static final String INVALID_RESPONSE_SIZE_FORMAT_MESSAGE =
            "Invalid respSize format in one of the log entries.";
    public static final String INVALID_TIME_STAMP_FORMAT_MESSAGE =
            "Invalid timestamp format in one of the log entries.";
    public static final String AUDIT_LOGS_INSERTION_FAILURE_MESSAGE =
            "Audit logs Insertion process failed.";
    public static final String AUDIT_LOG_INSERTION_QUERY = "INSERT INTO metering (user_id, collection_id, api_path, timestamp, resp_size) " +
            "VALUES ($1, $2, $3, $4, $5)";
}
