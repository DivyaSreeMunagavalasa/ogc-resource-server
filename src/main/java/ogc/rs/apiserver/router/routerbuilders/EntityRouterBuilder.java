package ogc.rs.apiserver.router.routerbuilders;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.openapi.RouterBuilderOptions;
import ogc.rs.apiserver.ApiServerVerticle;
import ogc.rs.apiserver.handlers.*;
import ogc.rs.apiserver.util.OgcException;
import static ogc.rs.apiserver.util.Constants.*;
import java.util.Set;

/**
 * Abstract class to aid in configuration and building of routers using {@link RouterBuilder}.
 *
 */
public abstract class EntityRouterBuilder {

  private static final Set<String> allowedHeaders =
      Set.of(HEADER_TOKEN, HEADER_CONTENT_LENGTH, HEADER_CONTENT_TYPE, HEADER_HOST, HEADER_ORIGIN,
          HEADER_REFERER, HEADER_ACCEPT, HEADER_ALLOW_ORIGIN);

  private static final Set<HttpMethod> allowedMethods = Set.of(HttpMethod.GET, HttpMethod.OPTIONS);
  public static final String API_DOC_FILE_PATH = "docs/apidoc.html";
  private static final String OPENAPI_V3_JSON_CONTENT_TYPE = "application/vnd.oai.openapi+json;version=3.0";
  private static final String HTML_CONTENT_TYPE = "text/html";

  /* this is used since all the API methods are implemented in the ApiServer verticle */
  public ApiServerVerticle apiServerVerticle;

  /* this is used to create handlers or anything that is initialized using a vert.x instance */
  public Vertx vertx;

  /* create all handlers here and make them public so that they can be accessed */
  public FailureHandler failureHandler = new FailureHandler();

  public RouterBuilder routerBuilder;
  private JsonObject config;
  public DxTokenAuthenticationHandler tokenAuthenticationHandler;
  public StacAssetsAuthZHandler stacAssetsAuthZHandler;
  public MeteringAuthZHandler meteringAuthZHandler = new MeteringAuthZHandler();
  public OgcFeaturesAuthZHandler ogcFeaturesAuthZHandler;
  public ProcessAuthZHandler processAuthZHandler = new ProcessAuthZHandler();

  EntityRouterBuilder(ApiServerVerticle apiServerVerticle, Vertx vertx, RouterBuilder routerBuilder,
      JsonObject config) {
    this.apiServerVerticle = apiServerVerticle;
    this.vertx = vertx;
    this.routerBuilder = routerBuilder;
    this.config = config;
    tokenAuthenticationHandler = new DxTokenAuthenticationHandler(vertx, config);
    stacAssetsAuthZHandler = new StacAssetsAuthZHandler(vertx);
    ogcFeaturesAuthZHandler = new OgcFeaturesAuthZHandler(vertx);
  }

  /**
   * Any common {@link RouterBuilder} configuration to be added to any router to be built.
   */
  final void setCommonRouterBuilderConfiguration() {

    RouterBuilderOptions factoryOptions =
        new RouterBuilderOptions().setMountResponseContentTypeHandler(true);

    routerBuilder.rootHandler(
        CorsHandler.create().allowedHeaders(allowedHeaders).allowedMethods(allowedMethods));
    routerBuilder.rootHandler(BodyHandler.create());
  }

  /**
   * Get a {@link Router} with all routes added to it.
   *
   * @return a fully built {@link Router}
   */
  public final Router getRouter() {

    setCommonRouterBuilderConfiguration();
    addImplSpecificRoutes();

    Router router = routerBuilder.createRouter();

    JsonObject oasJson = routerBuilder.getOpenAPI().getOpenAPI();

    /* Set the OpenAPI spec route */
    router
        .get(getOasApiPath())
        .produces(HTML_CONTENT_TYPE) // order of produces matters - so here priority is given to HTML 
        .produces(OPENAPI_V3_JSON_CONTENT_TYPE)
        .failureHandler(failureHandler)
        .handler(
            routingContext -> {
              HttpServerResponse response = routingContext.response();
              String queryParam = routingContext.request().getParam("f");
              
              String contentType;
              
              if ("html".equals(queryParam)) {
                contentType = HTML_CONTENT_TYPE;
              } else if ("json".equals(queryParam)) {
                contentType = OPENAPI_V3_JSON_CONTENT_TYPE;
              } else if (queryParam == null) {
                // use whatever comes in Accept header (controlled by produces block)
                contentType = routingContext.getAcceptableContentType();
              } else {
                routingContext
                    .fail(new OgcException(400, "Invalid query param for OpenAPI spec format",
                        "Invalid query param for OpenAPI spec format"));
                return;
              }
              
              response.putHeader("Content-type", contentType);

              if (HTML_CONTENT_TYPE.equals(contentType)) {
                try {
                  FileSystem fileSystem = vertx.fileSystem();
                  Buffer buffer = fileSystem.readFileBlocking(API_DOC_FILE_PATH);
                  String apiDocContent = buffer.toString();
                  String jsonSpecUrl = getOasApiPath() + "?f=json";
                  apiDocContent = apiDocContent.replace("$1", jsonSpecUrl);

                  response.end(apiDocContent);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              } else if(OPENAPI_V3_JSON_CONTENT_TYPE.equals(contentType)) {
                response.send(oasJson.toBuffer());
              }
            });

    return router;
  }

  /**
   * Return the API path where the OpenAPI spec JSON is served. The route is created in the
   * {@link #getRouter()} method.
   *
   * @return the endpoint
   */
  abstract String getOasApiPath();

  /**
   * Add all implementation specific routes to the {@link RouterBuilder}.
   */
  abstract void addImplSpecificRoutes();
}
