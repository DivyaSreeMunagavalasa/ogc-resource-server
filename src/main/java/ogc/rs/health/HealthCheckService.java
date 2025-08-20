package ogc.rs.health;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import ogc.rs.apiserver.handlers.DxTokenAuthenticationHandler;

import java.io.File;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Health Check Service for OGC Resource Server
 * Implements MicroProfile Health Check specification
 * Self-contained service that initializes its own dependencies
 */
public class HealthCheckService {

    private static final String STATUS_UP = "UP";
    private static final String STATUS_DOWN = "DOWN";
    private static final Pattern FLYWAY_VERSION_PATTERN = Pattern.compile("V(\\d+(?:\\.\\d+)*)__.*\\.sql");

    private final Vertx vertx;
    private final JsonObject config;

    // Database-related fields
    private String databaseIp;
    private int databasePort;
    private String databaseName;
    private String databaseUserName;
    private String databasePassword;
    private int poolSize;

    // Client fields
    private WebClient webClient;
    private S3Client s3Client;

    public HealthCheckService(Vertx vertx, JsonObject config) {
        this.vertx = vertx;
        this.config = config;
        initializeDependencies();
    }

    /**
     * Initialize all dependencies (web client, S3 client)
     * Database pool is created per check
     */
    private void initializeDependencies() {
        initializeDatabaseConfig();
        initializeWebClient();
        initializeS3Client();
    }

    /**
     * Initialize database configuration parameters
     * Database pool will be created when needed in individual checks
     */
    private void initializeDatabaseConfig() {
        databaseIp = config.getString("databaseHost");
        databasePort = config.getInteger("databasePort");
        databaseName = config.getString("databaseName");
        databaseUserName = config.getString("databaseUser");
        databasePassword = config.getString("databasePassword");
        poolSize = config.getInteger("poolSize");
    }

    /**
     * Create a new database connection pool for health checks
     * This is called per check and pool is closed after use
     */
    private PgPool createDatabasePool() {
        PgConnectOptions connectOptions = new PgConnectOptions()
                .setPort(databasePort)
                .setHost(databaseIp)
                .setDatabase(databaseName)
                .setUser(databaseUserName)
                .setPassword(databasePassword)
                .setReconnectAttempts(2)
                .setReconnectInterval(1000L);

        PoolOptions poolOptions = new PoolOptions().setMaxSize(poolSize);
        return PgPool.pool(vertx, connectOptions, poolOptions);
    }

    /**
     * Initialize web client for external service checks
     */
    private void initializeWebClient() {
        WebClientOptions webClientOptions = new WebClientOptions()
                .setSsl(true)
                .setTrustAll(false)
                .setVerifyHost(true)
                .setConnectTimeout(5000)
                .setIdleTimeout(10000);

        this.webClient = WebClient.create(vertx, webClientOptions);
    }

    /**
     * Initialize S3 client
     */
    private void initializeS3Client() {
        try {
            JsonObject s3ConfigsBlock = config.getJsonObject("s3BucketsConfig");
            if (s3ConfigsBlock == null) {
                System.err.println("S3 configs block not found in configuration");
                this.s3Client = null;
                return;
            }

            this.s3Client = createS3Client(s3ConfigsBlock);
        } catch (Exception e) {
            System.err.println("Failed to initialize S3 client: " + e.getMessage());
            this.s3Client = null;
        }
    }

    /**
     * Helper method to create S3 Client based on configuration
     */
    private S3Client createS3Client(JsonObject s3ConfigsBlock) {
        try {
            JsonObject defaultS3Config = s3ConfigsBlock.getJsonObject("default");
            if (defaultS3Config == null) {
                System.err.println("Default S3 config not found");
                return null;
            }

            String endpoint = defaultS3Config.getString("endpoint");
            String region = defaultS3Config.getString("region");
            String accessKey = defaultS3Config.getString("accessKey");
            String secretKey = defaultS3Config.getString("secretKey");
            Boolean pathBasedAccess = defaultS3Config.getBoolean("pathBasedAccess", false);

            if (endpoint == null || region == null || accessKey == null || secretKey == null) {
                System.err.println("Incomplete S3 config values");
                return null;
            }

            return S3Client.builder()
                    .region(Region.of(region))
                    .endpointOverride(URI.create(endpoint))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(accessKey, secretKey)))
                    .serviceConfiguration(S3Configuration.builder()
                            .pathStyleAccessEnabled(pathBasedAccess)
                            .build())
                    .build();

        } catch (Exception e) {
            System.err.println("Failed to create S3 client: " + e.getMessage());
            return null;
        }
    }

    /**
     * Overall health check - AND operation of liveness and readiness
     */
    public Future<JsonObject> performHealthCheck() {
        Promise<JsonObject> promise = Promise.promise();

        Future<JsonObject> livenessFuture = performLivenessCheck();
        Future<JsonObject> readinessFuture = performReadinessCheck();

        Future.all(livenessFuture, readinessFuture).onComplete(ar -> {
            if (ar.succeeded()) {
                JsonObject livenessResult = livenessFuture.result();
                JsonObject readinessResult = readinessFuture.result();

                boolean isLive = STATUS_UP.equals(livenessResult.getString("status"));
                boolean isReady = STATUS_UP.equals(readinessResult.getString("status"));
                boolean overallStatus = isLive && isReady;

                JsonArray checks = new JsonArray();
                checks.addAll(livenessResult.getJsonArray("checks", new JsonArray()));
                checks.addAll(readinessResult.getJsonArray("checks", new JsonArray()));

                JsonObject response = new JsonObject()
                        .put("status", overallStatus ? STATUS_UP : STATUS_DOWN)
                        .put("checks", checks);

                promise.complete(response);
            } else {
                promise.fail(ar.cause());
            }
        });

        return promise.future();
    }

    /**
     * Liveness check - determines if the application is running
     */
    public Future<JsonObject> performLivenessCheck() {
        Promise<JsonObject> promise = Promise.promise();
        List<Future<JsonObject>> checks = new ArrayList<>();

        // Server alive check
        checks.add(checkServerAlive());

        Future.all(checks).onComplete(ar -> {
            JsonArray checkResults = new JsonArray();
            boolean allUp = true;

            if (ar.succeeded()) {
                for (Future<JsonObject> checkFuture : checks) {
                    JsonObject checkResult = checkFuture.result();
                    checkResults.add(checkResult);
                    if (!STATUS_UP.equals(checkResult.getString("status"))) {
                        allUp = false;
                    }
                }
            } else {
                allUp = false;
            }

            JsonObject response = new JsonObject()
                    .put("status", allUp ? STATUS_UP : STATUS_DOWN)
                    .put("checks", checkResults);

            promise.complete(response);
        });

        return promise.future();
    }

    /**
     * Readiness check - determines if the application is ready to serve requests
     */
    public Future<JsonObject> performReadinessCheck() {
        Promise<JsonObject> promise = Promise.promise();
        List<Future<JsonObject>> checks = new ArrayList<>();

        // Database readiness check
        checks.add(checkDatabaseReadiness());

        // Flyway migration check
        checks.add(checkFlywayMigrationStatus());

        // Catalogue service check
        checks.add(checkCatalogueService());

        // AAA service check
        checks.add(checkAAAService());

        // S3 service check
        checks.add(checkS3Service());

        Future.all(checks).onComplete(ar -> {
            JsonArray checkResults = new JsonArray();
            boolean allUp = true;

            if (ar.succeeded()) {
                for (Future<JsonObject> checkFuture : checks) {
                    JsonObject checkResult = checkFuture.result();
                    checkResults.add(checkResult);
                    if (!STATUS_UP.equals(checkResult.getString("status"))) {
                        allUp = false;
                    }
                }
            } else {
                allUp = false;
            }

            JsonObject response = new JsonObject()
                    .put("status", allUp ? STATUS_UP : STATUS_DOWN)
                    .put("checks", checkResults);

            promise.complete(response);
        });

        return promise.future();
    }

    /**
     * Performs a basic server alive check by executing this method.
     * Returns UP status with current timestamp and uptime information.
     */
    private Future<JsonObject> checkServerAlive() {
        Promise<JsonObject> promise = Promise.promise();

        // Simple check - if we can execute this code, the server is alive
        JsonObject result = new JsonObject()
                .put("name", "server-alive")
                .put("status", STATUS_UP)
                .put("data", new JsonObject()
                        .put("timestamp", Instant.now().toString())
                        .put("uptime", System.currentTimeMillis()));

        promise.complete(result);
        return promise.future();
    }

    /**
     * Checks database connection readiness by executing a simple SELECT query.
     * Creates a new connection pool for this check and closes it after use.
     * Returns connection details on success or error message on failure.
     */
    private Future<JsonObject> checkDatabaseReadiness() {
        Promise<JsonObject> promise = Promise.promise();

        // Create database pool for this specific check
        PgPool pgPool = createDatabasePool();

        pgPool.query("SELECT 1").execute(ar -> {
            JsonObject result = new JsonObject().put("name", "database-connection");

            if (ar.succeeded()) {
                result.put("status", STATUS_UP)
                        .put("data", new JsonObject()
                                .put("host", config.getString("databaseHost"))
                                .put("port", config.getInteger("databasePort"))
                                .put("database", config.getString("databaseName"))
                                .put("connectionPool", "active"));
            } else {
                result.put("status", STATUS_DOWN)
                        .put("data", new JsonObject()
                                .put("error", ar.cause().getMessage()));
            }

            // Close the pool after the check is complete
            pgPool.close();
            promise.complete(result);
        });

        return promise.future();
    }

    /**
     * Validates Flyway migration status by comparing filesystem and database versions.
     * Ensures all migrations are successfully applied and up-to-date.
     * Creates a new connection pool for this check and closes it after use.
     */
    private Future<JsonObject> checkFlywayMigrationStatus() {
        Promise<JsonObject> promise = Promise.promise();

        // Get latest migration version from filesystem
        String latestFileVersion = getLatestMigrationVersionFromFiles();

        // Create database pool for this specific check
        PgPool pgPool = createDatabasePool();

        // Check database migration status
        String query = "SELECT version, success FROM flyway_schema_history ORDER BY installed_rank DESC LIMIT 1";

        pgPool.query(query).execute(ar -> {
            JsonObject result = new JsonObject().put("name", "flyway-migration-status");

            if (ar.succeeded()) {
                RowSet<Row> rows = ar.result();
                if (rows.size() > 0) {
                    Row row = rows.iterator().next();
                    String dbVersion = row.getString("version");
                    Boolean success = row.getBoolean("success");

                    boolean isUpToDate = latestFileVersion != null && latestFileVersion.equals(dbVersion);
                    boolean isSuccessful = success != null && success;

                    if (isUpToDate && isSuccessful) {
                        result.put("status", STATUS_UP)
                                .put("data", new JsonObject()
                                        .put("latestFileVersion", latestFileVersion)
                                        .put("databaseVersion", dbVersion)
                                        .put("migrationSuccess", success)
                                        .put("upToDate", true));
                    } else {
                        result.put("status", STATUS_DOWN)
                                .put("data", new JsonObject()
                                        .put("latestFileVersion", latestFileVersion)
                                        .put("databaseVersion", dbVersion)
                                        .put("migrationSuccess", success)
                                        .put("upToDate", false));
                    }
                } else {
                    result.put("status", STATUS_DOWN)
                            .put("data", new JsonObject()
                                    .put("error", "No migration history found"));
                }
            } else {
                result.put("status", STATUS_DOWN)
                        .put("data", new JsonObject()
                                .put("error", ar.cause().getMessage()));
            }

            // Close the pool after the check is complete
            pgPool.close();
            promise.complete(result);
        });

        return promise.future();
    }

    /**
     * Checks catalogue service availability by making HTTP request to the root endpoint.
     * Accepts 2xx and 4xx status codes as indication that service is running.
     */
    private Future<JsonObject> checkCatalogueService() {
        Promise<JsonObject> promise = Promise.promise();

        String catServerHost = config.getString("catServerHost", "api.cat-test.iudx.io");
        Integer catServerPort = config.getInteger("catServerPort", 443);
        String catRequestInstanceUri = "/"; // Use root endpoint

        long startTime = System.currentTimeMillis();

        webClient.get(catServerPort, catServerHost, catRequestInstanceUri)
                .ssl(catServerPort == 443) // Enable SSL for port 443
                .putHeader("Host", catServerHost)
                .timeout(5000)
                .send(ar -> {
                    long responseTime = System.currentTimeMillis() - startTime;
                    JsonObject result = new JsonObject().put("name", "catalogue-service");

                    if (ar.succeeded()) {
                        int statusCode = ar.result().statusCode();
                        if (statusCode >= 200 && statusCode < 500) { // Accept 4xx as service is running
                            result.put("status", STATUS_UP)
                                    .put("data", new JsonObject()
                                            .put("host", catServerHost)
                                            .put("port", catServerPort)
                                            .put("endpoint", catRequestInstanceUri)
                                            .put("responseTime", responseTime + "ms")
                                            .put("statusCode", statusCode)
                                            .put("protocol", catServerPort == 443 ? "https" : "http"));
                        } else {
                            result.put("status", STATUS_DOWN)
                                    .put("data", new JsonObject()
                                            .put("error", "Unexpected status code: " + statusCode)
                                            .put("responseTime", responseTime + "ms"));
                        }
                    } else {
                        result.put("status", STATUS_DOWN)
                                .put("data", new JsonObject()
                                        .put("error", ar.cause().getMessage())
                                        .put("responseTime", responseTime + "ms"));
                    }
                    promise.complete(result);
                });

        return promise.future();
    }

    /**
     * Validates AAA service readiness by checking JWT authentication initialization.
     * Handles cases where authentication is disabled via system properties.
     */
    private Future<JsonObject> checkAAAService() {
        Promise<JsonObject> promise = Promise.promise();

        String authServerHost = config.getString("authServerHost");

        // Check if AAA service configuration is present
        if (authServerHost == null) {
            JsonObject result = new JsonObject()
                    .put("name", "aaa-service")
                    .put("status", STATUS_DOWN)
                    .put("data", new JsonObject().put("error", "AAA service configuration not found"));
            promise.complete(result);
            return promise.future();
        }

        // Check if authentication is disabled via system property
        if (System.getProperty("disable.auth") != null) {
            JsonObject result = new JsonObject()
                    .put("name", "aaa-service")
                    .put("status", STATUS_UP)
                    .put("data", new JsonObject()
                            .put("authenticationDisabled", true)
                            .put("note", "Authentication is disabled via system property"));
            promise.complete(result);
            return promise.future();
        }

        // Verify if JWT authentication is properly initialized
        // by checking the actual jwtAuth instance from DxTokenAuthenticationHandler
        try {
            boolean isJwtInitialized = DxTokenAuthenticationHandler.isJwtAuthInitialized();
            String initError = DxTokenAuthenticationHandler.getInitializationError();

            JsonObject result = new JsonObject().put("name", "aaa-service");

            if (isJwtInitialized) {
                result.put("status", STATUS_UP)
                        .put("data", new JsonObject()
                                .put("host", authServerHost)
                                .put("jwtAuthInitialized", true)
                                .put("note", "JWT Authentication is properly initialized and ready"));
            } else {
                result.put("status", STATUS_DOWN)
                        .put("data", new JsonObject()
                                .put("host", authServerHost)
                                .put("jwtAuthInitialized", false)
                                .put("error", initError != null ? initError : "JWT Authentication initialization failed")
                                .put("note", "Authentication service is not ready - server cannot authenticate requests"));
            }

            promise.complete(result);

        } catch (Exception e) {
            JsonObject result = new JsonObject()
                    .put("name", "aaa-service")
                    .put("status", STATUS_DOWN)
                    .put("data", new JsonObject()
                            .put("error", "Unexpected error checking JWT auth status: " + e.getMessage()));
            promise.complete(result);
        }

        return promise.future();
    }

    /**
     * Verifies S3 service accessibility by performing headBucket operation on configured bucket.
     * Checks bucket configuration and measures response time for connectivity validation.
     * Uses executeBlocking to handle the synchronous S3 SDK calls within Vert.x's event loop
     * without blocking the event loop thread, as S3 SDK operations are blocking by nature.
     */
    private Future<JsonObject> checkS3Service() {
        Promise<JsonObject> promise = Promise.promise();

        if (s3Client == null) {
            JsonObject result = new JsonObject()
                    .put("name", "s3-service")
                    .put("status", STATUS_DOWN)
                    .put("data", new JsonObject().put("error", "S3 client not configured"));
            promise.complete(result);
            return promise.future();
        }

        // Using executeBlocking because S3 SDK operations are synchronous/blocking by nature
        // and would block the Vert.x event loop if called directly. executeBlocking ensures
        // these blocking operations are handled on a worker thread pool instead.
        vertx.executeBlocking(blockingPromise -> {
            try {
                long startTime = System.currentTimeMillis();

                // Extract the bucket name from s3BucketsConfig.default.bucket
                JsonObject s3BucketsConfig = config.getJsonObject("s3BucketsConfig");
                if (s3BucketsConfig == null || s3BucketsConfig.getJsonObject("default") == null) {
                    JsonObject result = new JsonObject()
                            .put("name", "s3-service")
                            .put("status", STATUS_DOWN)
                            .put("data", new JsonObject()
                                    .put("error", "s3BucketsConfig.default not present in config"));
                    blockingPromise.complete(result);
                    return;
                }

                JsonObject defaultBucketConfig = s3BucketsConfig.getJsonObject("default");
                String bucketName = defaultBucketConfig.getString("bucket");

                if (bucketName == null || bucketName.isEmpty()) {
                    JsonObject result = new JsonObject()
                            .put("name", "s3-service")
                            .put("status", STATUS_DOWN)
                            .put("data", new JsonObject()
                                    .put("error", "Bucket name not configured in s3BucketsConfig.default"));
                    blockingPromise.complete(result);
                    return;
                }

                // Check if bucket is accessible
                HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                        .bucket(bucketName)
                        .build();

                s3Client.headBucket(headBucketRequest); // throws exception if inaccessible
                long responseTime = System.currentTimeMillis() - startTime;

                JsonObject result = new JsonObject()
                        .put("name", "s3-service")
                        .put("status", STATUS_UP)
                        .put("data", new JsonObject()
                                .put("bucketName", bucketName)
                                .put("bucketAccessible", true)
                                .put("responseTime", responseTime + "ms")
                                .put("endpoint", defaultBucketConfig.getString("endpoint")));
                blockingPromise.complete(result);

            } catch (SdkException e) {
                JsonObject result = new JsonObject()
                        .put("name", "s3-service")
                        .put("status", STATUS_DOWN)
                        .put("data", new JsonObject()
                                .put("error", e.getMessage())
                                .put("bucketName", "unknown"));
                blockingPromise.complete(result);
            } catch (Exception e) {
                JsonObject result = new JsonObject()
                        .put("name", "s3-service")
                        .put("status", STATUS_DOWN)
                        .put("data", new JsonObject()
                                .put("error", "Unexpected error: " + e.getMessage()));
                blockingPromise.complete(result);
            }
        }, promise);

        return promise.future();
    }

    /**
     * Scans migration directory to find the latest Flyway migration version from filenames.
     * Uses regex pattern matching to extract version numbers from migration files.
     */
    private String getLatestMigrationVersionFromFiles() {
        try {
            File migrationDir = new File("src/main/resources/db/migration");
            if (!migrationDir.exists() || !migrationDir.isDirectory()) {
                return null;
            }

            String latestVersion = null;
            File[] files = migrationDir.listFiles();

            if (files != null) {
                for (File file : files) {
                    if (file.isFile() && file.getName().endsWith(".sql")) {
                        Matcher matcher = FLYWAY_VERSION_PATTERN.matcher(file.getName());
                        if (matcher.matches()) {
                            String version = matcher.group(1);
                            if (latestVersion == null || compareVersions(version, latestVersion) > 0) {
                                latestVersion = version;
                            }
                        }
                    }
                }
            }

            return latestVersion;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Compares two version strings using semantic versioning rules.
     * Returns positive if v1 > v2, negative if v1 < v2, zero if equal.
     */
    private int compareVersions(String v1, String v2) {
        String[] parts1 = v1.split("\\.");
        String[] parts2 = v2.split("\\.");
        int maxLength = Math.max(parts1.length, parts2.length);

        for (int i = 0; i < maxLength; i++) {
            int part1 = i < parts1.length ? Integer.parseInt(parts1[i]) : 0;
            int part2 = i < parts2.length ? Integer.parseInt(parts2[i]) : 0;

            if (part1 != part2) {
                return Integer.compare(part1, part2);
            }
        }

        return 0;
    }

    /**
     * Clean up resources when the service is no longer needed
     * Database pools are created per check and closed immediately after use
     */
    public void close() {
        if (webClient != null) {
            webClient.close();
        }
        if (s3Client != null) {
            s3Client.close();
        }
    }
}