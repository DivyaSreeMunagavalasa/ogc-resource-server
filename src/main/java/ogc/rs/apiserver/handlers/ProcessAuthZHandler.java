package ogc.rs.apiserver.handlers;

import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import ogc.rs.apiserver.util.OgcException;
import ogc.rs.apiserver.util.AuthInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

import static ogc.rs.apiserver.handlers.DxTokenAuthenticationHandler.USER_KEY;
import static ogc.rs.apiserver.util.Constants.ECHO_PROCESS_EXECUTION_ENDPOINT;

public class ProcessAuthZHandler implements Handler<RoutingContext> {
  private static final Logger LOGGER = LogManager.getLogger(ProcessAuthZHandler.class);
    // Check if fake token is enabled (set via JVM option -Dfake-token=true)
    private static final boolean FAKE_TOKEN_ENABLED =
            Boolean.parseBoolean(System.getProperty("fake-token", "false"));

    /**
   * Handles the routing context to authorize access to process APIs.
   *
   * @param routingContext the routing context of the request
   */
  @Override
  public void handle(RoutingContext routingContext) {
    LOGGER.debug("Process Authorization");
    AuthInfo user = routingContext.get(USER_KEY);
    UUID iid = user.getResourceId();
      // Block echo process unless fake-token=true
      String requestPath = routingContext.normalizedPath();
      if (requestPath.contains(ECHO_PROCESS_EXECUTION_ENDPOINT)) {
          if (!FAKE_TOKEN_ENABLED) {
              LOGGER.error("Attempted to run Echo process without fake-token=true");
              routingContext.fail(
                      new OgcException(
                              403,
                              "ForbiddenProcess",
                              "The echo process is only available for testing purposes with fake-token enabled"
                      )
              );
              return;
          }
      }
      LOGGER.debug("Validating access for execution ");
    if (!user.isRsToken()) {
      LOGGER.error("Resource Ids don't match!");
      routingContext.fail(
          new OgcException(
              401, "Not Authorized", "User is not authorised. Please contact DX AAA "));
      return;
    } else if ((user.getRole() == AuthInfo.RoleEnum.provider)) {
      JsonObject results = new JsonObject();
      results.put("iid", iid);
      results.put("userId", user.getUserId());
      results.put("role", user.getRole());
      routingContext.data().put("authInfo", results);
      routingContext.next();
    }
    else if ((user.getRole() == AuthInfo.RoleEnum.delegate && user.getDelegatorRole() == AuthInfo.RoleEnum.provider)) {
      JsonObject results = new JsonObject();
      results.put("iid", iid);
      results.put("userId", user.getDelegatorUserId());
      results.put("role", user.getDelegatorRole());
      routingContext.data().put("authInfo", results);
      routingContext.next();
    }
    else if (user.getRole() == AuthInfo.RoleEnum.admin) {
        JsonObject results = new JsonObject();
        results.put("userId", user.getUserId());
        results.put("role", user.getRole());
        routingContext.data().put("authInfo", results);
        routingContext.next();
    }
    else {
      LOGGER.debug("Not a provider or a provider delegate token or an admin role. It is of role {} ", user.getRole());
      routingContext.fail(
          new OgcException(
              401, "Not Authorized", "User is not authorised. Please contact DX AAA "));
    }
  }
}
