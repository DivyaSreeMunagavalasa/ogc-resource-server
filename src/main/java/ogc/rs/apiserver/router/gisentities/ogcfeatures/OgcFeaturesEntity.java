package ogc.rs.apiserver.router.gisentities.ogcfeatures;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import com.google.auto.service.AutoService;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ogc.rs.apiserver.ApiServerVerticle;
import ogc.rs.apiserver.handlers.FailureHandler;
import ogc.rs.apiserver.router.gisentities.GisEntityInterface;
import ogc.rs.apiserver.router.routerbuilders.OgcRouterBuilder;
import ogc.rs.apiserver.router.routerbuilders.StacRouterBuilder;
import ogc.rs.apiserver.router.util.OasFragments;
import ogc.rs.database.DatabaseService;

/**
 * Class to handle creation of routes for OGC Feature datasets.
 * 
 */
@AutoService(GisEntityInterface.class)
public class OgcFeaturesEntity implements GisEntityInterface {

  private static final String UUID_REGEX =
      "[0-9a-fA-F]{8}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{4}\\b-[0-9a-fA-F]{12}";
  
  private static final String OGC_FEATURE_COLLECTION_PATH_REGEX =
      "^/collections/" + UUID_REGEX + "/items";
  
  private static final Logger LOGGER = LogManager.getLogger(OgcFeaturesEntity.class);

  @Override
  public void giveOgcRoutes(OgcRouterBuilder ogcRouterBuilder) {

    RouterBuilder builder = ogcRouterBuilder.routerBuilder;
    ApiServerVerticle apiServerVerticle = ogcRouterBuilder.apiServerVerticle;
    FailureHandler failureHandler = ogcRouterBuilder.failureHandler;

    List<String> collectionSpecificOpIds = builder.operations().stream()
        .filter(op -> op.getOperationId().matches(OgcFeaturesMetadata.OGC_OP_ID_PREFIX_REGEX))
        .map(op -> op.getOperationId()).collect(Collectors.toList());

    collectionSpecificOpIds.forEach(opId -> {

      if (opId.matches(OgcFeaturesMetadata.OGC_GET_COLLECTION_ITEMS_OP_ID_REGEX)) {
        builder.operation(opId)
                .handler(ogcRouterBuilder.ogcFeaturesAuthZHandler)
                .handler(ogcRouterBuilder.tokenLimitsEnforcementHandler)
                .handler(apiServerVerticle::auditAfterApiEnded)
                .handler(apiServerVerticle::validateQueryParams)
                .handler(apiServerVerticle::getFeatures)
                .handler(apiServerVerticle::putCommonResponseHeaders)
                .handler(apiServerVerticle::buildResponse)
                .failureHandler(failureHandler);

      } else if (opId.matches(OgcFeaturesMetadata.OGC_GET_SPECIFIC_FEATURE_OP_ID_REGEX)) {
            builder.operation(opId)
                    .handler(ogcRouterBuilder.ogcFeaturesAuthZHandler)
                    .handler(ogcRouterBuilder.tokenLimitsEnforcementHandler)
                    .handler(apiServerVerticle::auditAfterApiEnded)
                    .handler(apiServerVerticle::validateQueryParams)
                    .handler(apiServerVerticle::getFeature)
                    .handler(apiServerVerticle::putCommonResponseHeaders)
                    .handler(apiServerVerticle::buildResponse)
                    .failureHandler(failureHandler);
          }
        });
  }

  @Override
  public void giveStacRoutes(StacRouterBuilder stacRouterBuilder) {
    // no STAC routes applicable for OGC Features
  }

  @Override
  public Future<OasFragments> generateNewSpecFragments(JsonObject existingOgcSpec,
      JsonObject existingStacSpec, DatabaseService dbService, JsonObject config) {

    Promise<OasFragments> promise = Promise.promise();
    
    Set<UUID> existingCollectionIds = getExistingFeatureCollectionsFromOgcSpec(existingOgcSpec);

    Future<List<JsonObject>> collectionMetadata =
        dbService.getOgcFeatureCollectionMetadataForOasSpec(
            existingCollectionIds.stream().map(i -> i.toString()).collect(Collectors.toList()));

    Future<List<OgcFeaturesMetadata>> metadataObjList = collectionMetadata.compose(res -> {
      List<OgcFeaturesMetadata> list =
          res.stream().map(i -> new OgcFeaturesMetadata(i)).collect(Collectors.toList());
      
      List<String> foundCollectionIds =
          list.stream().map(obj -> obj.getId().toString()).collect(Collectors.toList());

      if (foundCollectionIds.isEmpty()) {
        LOGGER.info("No new OGC Feature collections found!");
      } else {
        LOGGER.info("New OGC Feature collections found : {}", foundCollectionIds);
      }
      
      return Future.succeededFuture(list);
    });

    Future<OasFragments> result = metadataObjList.compose(list -> {
      List<JsonObject> ogcFrags = new ArrayList<JsonObject>();

      JsonObject geomMaxLimitConfig = config.getJsonObject("geomSpecificMaxLimits", new JsonObject());

      /*
       * if geomSpecificMaxLimits is not in config, then set max for all geometries to
       * OgcFeaturesMetadata.OGC_LIMIT_PARAM_MAX_DEFAULT)
       */
      Map<PostgisGeomTypes, Integer> geomSpecificMaxLimits = Arrays.stream(PostgisGeomTypes.values())
          .collect(Collectors.toMap(type -> type, type -> geomMaxLimitConfig
              .getInteger(type.toString(), OgcFeaturesMetadata.OGC_LIMIT_PARAM_MAX_DEFAULT)));
      
      list.forEach(obj -> {
        ogcFrags.add(obj.generateOgcOasBlock(geomSpecificMaxLimits));
      });

      OasFragments fragments = new OasFragments();
      fragments.setOgc(ogcFrags);

      return Future.succeededFuture(fragments);
    });

    result.onSuccess(i -> promise.complete(i)).onFailure(err -> promise.fail(err));

    return promise.future();
  }

  /**
   * Get existing OGC feature collections from the OGC spec. These IDs don't need to be fetched from the DB
   * and have spec generated for them. We only check the OGC spec and not the STAC spec since all
   * IDs that are there in the STAC spec will be there in the OGC spec as well.
   * 
   * @param ogcSpec the OGC OpenAPI spec JSON
   * @return
   */
  private Set<UUID> getExistingFeatureCollectionsFromOgcSpec(JsonObject ogcSpec) {
    Set<UUID> existingIds = new HashSet<UUID>();

    ogcSpec.getJsonObject("paths").forEach(k -> {
      String key = k.getKey();
      if (key.matches(OGC_FEATURE_COLLECTION_PATH_REGEX)) {
        String collectionId = key.split("/")[2];
        existingIds.add(UUID.fromString(collectionId));
      }
    });

    return existingIds;
  }
}
