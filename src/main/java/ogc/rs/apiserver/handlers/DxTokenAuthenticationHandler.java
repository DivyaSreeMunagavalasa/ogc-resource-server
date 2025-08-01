package ogc.rs.apiserver.handlers;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.handler.AuthenticationHandler;
import ogc.rs.apiserver.util.OgcException;
import ogc.rs.apiserver.util.AuthInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;

import static ogc.rs.apiserver.util.Constants.*;

public class DxTokenAuthenticationHandler implements AuthenticationHandler {
  private static final Logger LOGGER = LogManager.getLogger(DxTokenAuthenticationHandler.class);
  public static final String USER_KEY = "userKey";

  Vertx vertx;
  private WebClient webClient;
  private static JWTAuth jwtAuth;

    // Add static fields to track initialization state
    private static boolean jwtAuthInitialized = false;
    private static String initializationError = null;

    static WebClient createWebClient(Vertx vertx, JsonObject config) {
    return createWebClient(vertx, config, false);
  }

  static WebClient createWebClient(Vertx vertxObj, JsonObject config, boolean testing) {
    WebClientOptions webClientOptions = new WebClientOptions();
    if (testing) {
      webClientOptions.setTrustAll(true).setVerifyHost(false);
    }
    webClientOptions.setSsl(true);
    return WebClient.create(vertxObj, webClientOptions);
  }

  public DxTokenAuthenticationHandler(Vertx vertx, JsonObject config) {
    this.vertx = vertx;
    getJwtPublicKey(vertx, config)
        .onSuccess(
            handler -> {
              String cert = handler;

              JWTAuthOptions jwtAuthOptions = new JWTAuthOptions();
              jwtAuthOptions.addPubSecKey(
                  new PubSecKeyOptions().setAlgorithm("ES256").setBuffer(cert));
              /*
               * Default jwtIgnoreExpiry is false. If set through config, then that value is taken
               */
              boolean jwtIgnoreExpiry =
                  config.getBoolean("jwtIgnoreExpiry") != null
                      && config.getBoolean("jwtIgnoreExpiry");
              if (jwtIgnoreExpiry) {
                jwtAuthOptions.getJWTOptions().setIgnoreExpiration(true);
                LOGGER.warn(
                    "JWT ignore expiration set to true, "
                        + "do not set IgnoreExpiration in production!!");
              }
              jwtAuthOptions
                  .getJWTOptions()
                  .setAudience(Collections.singletonList(config.getString("audience")));
              jwtAuthOptions.getJWTOptions().setIssuer(config.getString("issuer"));
              jwtAuth = JWTAuth.create(vertx, jwtAuthOptions);

              // Mark as successfully initialized
                jwtAuthInitialized = true;
                initializationError = null;
                LOGGER.info("JWT Authentication successfully initialized");
            })
        .onFailure(
            handler -> {
              LOGGER.error("failed to get JWT public key from auth server");
              LOGGER.error("Authentication verticle deployment failed.");
                // Mark as failed initialization
                jwtAuthInitialized = false;
                initializationError = handler.getMessage();
                jwtAuth = null;
            });
  }

  private Future<String> getJwtPublicKey(Vertx vertx, JsonObject config) {
    Promise<String> promise = Promise.promise();
    if (System.getProperty("fake-token") != null) {
      LOGGER.fatal("processing fake token property");
      String authCert = "src/test/resources/public.pem";
      FileSystem fileSystem = vertx.fileSystem();
      Buffer buffer = fileSystem.readFileBlocking(authCert);
      promise.complete(buffer.toString());
    } else {
      webClient = createWebClient(vertx, config);
      String authCert = config.getString("dxAuthBasePath") + AUTH_CERTIFICATE_PATH;
      webClient
          .get(443, config.getString("authServerHost"), authCert)
          .send(
              handler -> {
                if (handler.succeeded()) {
                  JsonObject json = handler.result().bodyAsJsonObject();
                  promise.complete(json.getString("cert"));
                } else {
                  promise.fail("failed to get JWT public key: " + handler.cause().getMessage());
                }
              });
    }
    return promise.future();
  }

    /**
     * Static method to check if JWT authentication is properly initialized
     * This can be called from HealthCheckService
     */
    public static boolean isJwtAuthInitialized() {
        return jwtAuthInitialized && jwtAuth != null;
    }
    /**
     * Get the initialization error if any
     */
    public static String getInitializationError() {
        return initializationError;
    }

    @Override
  public void handle(RoutingContext routingContext) {
    
    if(System.getProperty("disable.auth") != null) {
      routingContext.next();
      return;
    }
    
    String authZHeader = routingContext.request().headers().get(HEADER_AUTHORIZATION);
    String path = routingContext.normalizedPath();
    boolean isStacItemEndpoint = path.matches(STAC_ASSETS_BY_ID_REGEX);

      if (authZHeader == null) {
          if (isStacItemEndpoint) {
              routingContext.next();
          } else {
              routingContext.fail(new OgcException(401, "Authorization header not found", "Authorization header not found"));
          }
          return;
      }

    String[] parts = authZHeader.split(" ");

    if (parts.length < 2 || !"Bearer".equals(parts[0])) {
      routingContext.fail(
          new OgcException(401, "Invalid Authorization header", "Invalid Authorization header"));
      return;
    }
    
    String token = parts[1];

    // Check if jwtAuth is null (initialization failed)
        if (jwtAuth == null) {
            LOGGER.error("JWT Authentication not initialized - cannot authenticate token");
            routingContext.fail(new OgcException(503, "Authentication service unavailable", "JWT Authentication not properly initialized"));
            return;
        }

        jwtAuth.authenticate(
              new JsonObject().put("token", token),
              res -> {
                  if (res.succeeded()) {
                      LOGGER.debug("Authentication Successful");
                      JsonObject tokenDetails = res.result().attributes().getJsonObject("accessToken");
                      String accessToken = res.result().principal().getString("access_token");
                      tokenDetails.put("access_token", accessToken);
                      AuthInfo user = AuthInfo.createUser(tokenDetails);
                      routingContext.put(USER_KEY, user);
                      LOGGER.debug("the user key: " + routingContext.get(USER_KEY).toString());
                      routingContext.next();
                  } else {
                      LOGGER.debug("Authentication not successful: " + res.cause());
                      routingContext.fail(new OgcException(401, "Invalid Token", res.cause().getMessage()));
                  }
              });

  }
}
