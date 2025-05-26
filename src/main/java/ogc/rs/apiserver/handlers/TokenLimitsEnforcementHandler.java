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
import static ogc.rs.apiserver.handlers.DxTokenAuthenticationHandler.USER_KEY;
import static ogc.rs.apiserver.util.Constants.*;
import static ogc.rs.common.Constants.DATABASE_SERVICE_ADDRESS;

/**
 * This HTTP request handler enforces data and API usage limits per user and collection.
 * <p>
 * Limits are extracted from the user's JWT token and can include either:
 * - Data usage limit (e.g., "100:mb")
 * - API hit limit (e.g., 100)
 * <p>
 * The limits are applied from the policy issued-at timestamp (iat) defined in the token.
 */
public class TokenLimitsEnforcementHandler implements Handler<RoutingContext> {
    private static final Logger LOGGER = LogManager.getLogger(TokenLimitsEnforcementHandler.class);
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

                case "bbox":
                    // Known constraint, not enforced here.
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
}
