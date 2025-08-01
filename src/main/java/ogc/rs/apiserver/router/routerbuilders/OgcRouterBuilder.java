package ogc.rs.apiserver.router.routerbuilders;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.openapi.RouterBuilder;
import ogc.rs.apiserver.ApiServerVerticle;
import ogc.rs.apiserver.router.RouterManager;
import ogc.rs.apiserver.router.gisentities.GisEntityInterface;
import static ogc.rs.apiserver.util.Constants.*;

import java.io.File;
import java.util.ServiceLoader;

/**
 * Class to build router for OGC APIs.
 *
 */
public class OgcRouterBuilder extends EntityRouterBuilder {

  private static final String OAS_URI_PATH = new File(RouterManager.OGC_OAS_REAL_PATH).toURI().toString();
  private static final String OAS_API_PATH = "/api";

  private OgcRouterBuilder(ApiServerVerticle apiServerVerticle, Vertx vertx, JsonObject config,
      RouterBuilder routerBuilder) {
    super(apiServerVerticle, vertx, routerBuilder, config);
  }

  /**
   * Create an instance of {@link OgcRouterBuilder}. 
   * 
   * @param apiServerVerticle the {@link ApiServerVerticle} whose router is to be updated
   * @param vertx an instance of Vert.x
   * @param config the config JSON
   * @return a Future of {@link OgcRouterBuilder}
   */
  public static Future<OgcRouterBuilder> create(ApiServerVerticle apiServerVerticle, Vertx vertx,
      JsonObject config) {
    Promise<OgcRouterBuilder> promise = Promise.promise();

    Future<RouterBuilder> routerBuilderFut = RouterBuilder.create(vertx, OAS_URI_PATH);

    routerBuilderFut
        .onSuccess(routerBuilder -> promise
            .complete(new OgcRouterBuilder(apiServerVerticle, vertx, config, routerBuilder)))
        .onFailure(err -> promise.fail(err));

    return promise.future();
  }

  @Override
  String getOasApiPath() {
    return OAS_API_PATH;
  }

  @Override
  void addImplSpecificRoutes() {
    routerBuilder.operation(LANDING_PAGE).handler(apiServerVerticle::sendOgcLandingPage)
        .failureHandler(failureHandler);

    routerBuilder.operation(CONFORMANCE_CLASSES).handler(routingContext -> {
      HttpServerResponse response = routingContext.response();
      response.sendFile("docs/conformance.json");
    });
    
    routerBuilder.operation(COLLECTIONS_API).handler(apiServerVerticle::getCollections)
        .handler(apiServerVerticle::putCommonResponseHeaders)
        .handler(apiServerVerticle::buildResponse).failureHandler(failureHandler);
    
    routerBuilder.operation(LIST_CONFIGURED_S3_BUCKETS_API)
        .handler(apiServerVerticle::listConfiguredS3Buckets)
        .handler(apiServerVerticle::putCommonResponseHeaders)
        .handler(apiServerVerticle::buildResponse).failureHandler(failureHandler);

    // Health Check Routes
    routerBuilder.operation(HEALTH_CHECK_API).handler(apiServerVerticle::handleHealthCheck);
    routerBuilder.operation(LIVENESS_HEALTH_CHECK_API).handler(apiServerVerticle::handleLivenessCheck);
    routerBuilder.operation(READINESS_HEALTH_CHECK_API).handler(apiServerVerticle::handleReadinessCheck);


    /**
     * For all implementers of GisEntityInterface, add the OGC routes to the RouterBuilder.
     */
    ServiceLoader<GisEntityInterface> loader = ServiceLoader.load(GisEntityInterface.class);
    loader.forEach(i -> i.giveOgcRoutes(this));

    return;
  }
}
