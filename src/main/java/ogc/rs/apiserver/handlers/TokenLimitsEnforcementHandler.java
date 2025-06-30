package ogc.rs.apiserver.handlers;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import ogc.rs.apiserver.util.AuthInfo;
import ogc.rs.apiserver.util.Limits;
import ogc.rs.apiserver.util.OgcException;
import ogc.rs.database.DatabaseService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

import static ogc.rs.apiserver.handlers.DxTokenAuthenticationHandler.USER_KEY;
import static ogc.rs.apiserver.util.Constants.*;
import static ogc.rs.common.Constants.DATABASE_SERVICE_ADDRESS;

/**
 * This HTTP request handler enforces data, API usage, feature and bbox limits per user and collection.
 * <p>
 * Limits are extracted from the user's JWT token and can include:
 * - Data usage limit (e.g., "100:mb")
 * - API hit limit (e.g., 100)
 * - Bounding box limits (bbox constraints)
 * - Feature access limits (collectionId -> featureIds mapping)
 * <p>
 * The limits are applied from the policy issued-at timestamp (iat) defined in the token.
 */
public class TokenLimitsEnforcementHandler implements Handler<RoutingContext> {
    private static final Logger LOGGER = LogManager.getLogger(TokenLimitsEnforcementHandler.class);
    private static final String LIMITS_CONTEXT_KEY = "VALIDATED_LIMITS";

    Vertx vertx;
    private final DatabaseService databaseService;

    /**
     * Constructs the handler with a Vert.x instance and initializes a proxy for DatabaseService.
     *
     * @param vertx Vertx instance used to create service proxies.
     */
    public TokenLimitsEnforcementHandler(Vertx vertx) {
        this.vertx = vertx;
        this.databaseService = DatabaseService.createProxy(vertx, DATABASE_SERVICE_ADDRESS);
    }

    /**
     * Intercepts each request and checks usage limits based on the user's token.
     * If the user has exceeded their allowed quota, the request is denied with 429 status.
     * Also validates and stores the limits for downstream use.
     *
     * @param routingContext Context of the current HTTP request.
     */
    @Override
    public void handle(RoutingContext routingContext) {

        if(System.getProperty("disable.auth") != null) {
            routingContext.next();
            return;
        }

        LOGGER.debug("Token Limits Enforcement Handler invoked");

        AuthInfo user = routingContext.get(USER_KEY);
        String userId = user.getRole() == AuthInfo.RoleEnum.delegate
                ? user.getDelegatorUserId().toString()
                : user.getUserId().toString();
        String collectionId = user.getResourceId().toString();
        String apiPath = routingContext.normalizedPath();

        // Parse limits
        JsonObject limitsJson = user.getConstraints().getJsonObject("limits");
        Limits limits = Limits.fromJson(limitsJson);
        if (limits == null) {
            routingContext.next();
            return;
        }

        // Store validated limits in context for downstream handlers
        routingContext.put(LIMITS_CONTEXT_KEY, limits);

        long policyIssuedAt = limits.getPolicyIssuedAt();
        LOGGER.info("policyIssuedAt: {}", policyIssuedAt);

        boolean constraintHandled = false;

        for (String key : limitsJson.fieldNames()) {
            switch (key) {
                case "dataUsage":
                    long limitInBytes = limits.getDataUsageLimitInBytes();
                    databaseService.getTotalDataUsage(userId, apiPath, collectionId, policyIssuedAt)
                            .onSuccess(dataUsage -> {
                                if (dataUsage > limitInBytes) {
                                    routingContext.fail(new OgcException(429, TOO_MANY_REQUESTS, DATA_USAGE_LIMIT_EXCEEDED));
                                } else {
                                    routingContext.next();
                                }
                            }).onFailure(fail -> {
                                LOGGER.error("Error checking data usage: {}", fail.getMessage());
                                routingContext.fail(fail);
                            });

                    constraintHandled = true;
                    break;

                case "apiHits":
                    long apiHitsLimit = limits.getApiHitsLimit();
                    databaseService.getTotalApiHits(userId, apiPath, collectionId, policyIssuedAt)
                            .onSuccess(apiHits -> {
                                if (apiHits > apiHitsLimit) {
                                    routingContext.fail(new OgcException(429, TOO_MANY_REQUESTS, API_CALLS_LIMIT_EXCEEDED));
                                } else {
                                    routingContext.next();
                                }
                            }).onFailure(fail -> {
                                LOGGER.error("Error checking API hits: {}", fail.getMessage());
                                routingContext.fail(fail);
                            });

                    constraintHandled = true;
                    break;

                case "feat":
                    Map<String, List<String>> featLimits = limits.getFeatLimitAsMap();

                    if (featLimits != null && !featLimits.isEmpty()) {
                        // Extract the collection ID from the feat limits
                        String collectionIdFromToken = featLimits.keySet().iterator().next();
                        LOGGER.debug("The collection Id from the token is: {}", collectionIdFromToken);

                        List<String> allowedFeatureIds = featLimits.get(collectionIdFromToken);
                        LOGGER.debug("The feature IDs in the token are: {}", allowedFeatureIds);

                        databaseService.checkTokenCollectionAndFeatureIdsExist(collectionIdFromToken, allowedFeatureIds)
                                .onSuccess(exists -> {
                                    if (!exists) {
                                        routingContext.fail(new OgcException(403, "Forbidden", "One or more features in the token do not exist"));
                                    } else {
                                        LOGGER.debug("Feature limits validated successfully");
                                        routingContext.next();
                                    }
                                }).onFailure(fail -> {
                                    if (fail instanceof OgcException) {
                                        OgcException ogcEx = (OgcException) fail;
                                        LOGGER.error("Failure: {} - {}", ogcEx.getStatusCode(), ogcEx.getMessage());
                                        routingContext.fail(ogcEx);
                                    } else {
                                        LOGGER.error("Unexpected error checking feature existence: {}", fail.getMessage());
                                        routingContext.fail(new OgcException(500, "Internal Server Error", "Unexpected error during feature existence check"));
                                    }
                                });
                    } else {
                        // feat key exists but no limits defined
                        LOGGER.warn("No feature limits defined");
                        routingContext.next();
                    }

                    constraintHandled = true;
                    break;

                case "bbox":
                    List<Double> bboxLimits = limits.getBboxLimitAsList();

                    if (bboxLimits != null && !bboxLimits.isEmpty()) {
                        // Bbox limits are valid (already validated in Limits.fromJson())
                        LOGGER.debug("Bbox limits validated successfully: {}", bboxLimits);
                        routingContext.next();
                    } else {
                        // bbox key exists but no limits defined
                        LOGGER.warn("No bbox limits defined");
                        routingContext.next();
                    }

                    constraintHandled = true;
                    break;

                default:
                    LOGGER.warn("Unknown usage constraint key: {}", key);
                    break;
            }

            if (constraintHandled) {
                return;
            }
        }

        // If no known constraint keys handled, proceed
        routingContext.next();
    }

    /**
     * Utility method to retrieve validated limits from the routing context.
     * This should be called by downstream handlers that need access to the limits.
     *
     * @param routingContext The routing context
     * @return Validated Limits object, or null if no limits were set
     */
    public static Limits getLimitsFromContext(RoutingContext routingContext) {
        return routingContext.get(LIMITS_CONTEXT_KEY);
    }
}