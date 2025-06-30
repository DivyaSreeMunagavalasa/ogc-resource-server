package ogc.rs.database;

import static ogc.rs.apiserver.util.Constants.BBOX_VIOLATES_CONSTRAINTS;
import static ogc.rs.database.util.Constants.PROCESSES_TABLE_NAME;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.*;
import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import ogc.rs.apiserver.router.RouterManager;
import ogc.rs.apiserver.util.Limits;
import ogc.rs.apiserver.util.OgcException;
import ogc.rs.apiserver.util.StacItemSearchParams;
import ogc.rs.database.util.FeatureQueryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static ogc.rs.common.Constants.*;
import static ogc.rs.database.util.Constants.*;
import static ogc.rs.database.util.Constants.UPDATE_COLLECTIONS_DETAILS;


public class DatabaseServiceImpl implements DatabaseService{
    private static final Logger LOGGER = LogManager.getLogger(DatabaseServiceImpl.class);

    private final PgPool client;
    private final JsonObject config;
    public DatabaseServiceImpl(final PgPool pgClient,JsonObject config) {
        this.client = pgClient;this.config=config;
    }

    @Override
    public Future<List<JsonObject>> getCollection(String collectionId) {
        LOGGER.info("getCollection");
        Promise<List<JsonObject>> result = Promise.promise();
        Collector<Row, ? , List<JsonObject>> collector = Collectors.mapping(Row::toJson, Collectors.toList());
        Collector<Row, ? , List<JsonObject>> enclosureCollector = Collectors.mapping(Row::toJson, Collectors.toList());
        client.withConnection(conn ->
           conn.preparedQuery("select collections_details.id, title, array_agg(distinct crs_to_srid.crs) as crs" +
                   ", collections_details.crs as \"storageCrs\", description, datetime_key, bbox, temporal," +
                   " array_agg(distinct collection_type.type) as type" +
                   " from collections_details join collection_supported_crs" +
                   " on collections_details.id = collection_supported_crs.collection_id join crs_to_srid" +
                   " on crs_to_srid.id = collection_supported_crs.crs_id join collection_type" +
                   " on collections_details.id=collection_type.collection_id  where collection_type.type != 'STAC' group by collections_details.id" +
                   " having collections_details.id = $1::uuid")
               .collecting(collector)
               .execute(Tuple.of(UUID.fromString( collectionId))).map(SqlResult::value)
            .onSuccess(success ->  {
                String query =
                        "SELECT * from collections_enclosure where collections_id = $1::uuid";
                conn.preparedQuery(query)
                        .collecting(enclosureCollector)
                        .execute(Tuple.of(UUID.fromString(collectionId)))
                        .map(SqlResult::value)
                        .onSuccess(
                                enclosureResult-> {
                                    if (!enclosureResult.isEmpty()) {
                                        success.get(0).put("enclosure", enclosureResult);
                                    }
                                    result.complete(success);
                                })
                        .onFailure(
                                failed -> {
                                    LOGGER.error("Failed at getFeature- {}", failed.getMessage());
                                    result.fail("Error!");
                                });

            })
            .onFailure(fail -> {
                LOGGER.error("Failed at getCollection- {}",fail.getMessage());
                result.fail("Error!");
            }));
        return result.future();
    }

    @Override
    public Future<List<JsonObject>> getCollections() {
        Promise<List<JsonObject>> result = Promise.promise();
        Collector<Row, ?, List<JsonObject>> collector = Collectors.mapping(Row::toJson, Collectors.toList());
        client.withConnection(conn ->
                conn.preparedQuery("select collections_details.id, collections_details.title, array_agg(DISTINCT crs_to_srid.crs) as crs" +
                        " , collections_details.crs as \"storageCrs\", collections_details.description, collections_details.datetime_key, " +
                        "   collections_details.bbox, collections_details.temporal, jsonb_agg(distinct(row_to_json(collections_enclosure.*)::jsonb " +
                        "   - 'collections_id')) AS enclosure, ARRAY_AGG(DISTINCT collection_type.type) AS type FROM collections_details LEFT " +
                        "   JOIN collections_enclosure ON collections_details.id = collections_enclosure.collections_id JOIN collection_supported_crs ON " +
                        "   collections_details.id = collection_supported_crs.collection_id JOIN crs_to_srid ON crs_to_srid.id = " +
                        "   collection_supported_crs.crs_id JOIN collection_type ON collections_details.id = collection_type.collection_id " +
                        "   WHERE collection_type.type != 'STAC' GROUP BY collections_details.id;")
                    .collecting(collector)
                    .execute()
                    .map(SqlResult::value))
            .onSuccess(success -> {
                LOGGER.debug("Collections Result: {}", success.toString());
                result.complete(success);
            })
            .onFailure(fail -> {
                LOGGER.error("Failed to getCollections! - {}", fail.getMessage());
                result.fail("Error!");
            });
        return result.future();
    }
    @Override
    public Future<JsonObject> getFeatures(String collectionId, Map<String, String> queryParams,
                                          Limits limits, Map<String, Integer> crs) {
        LOGGER.info("getFeatures");
        Promise<JsonObject> result = Promise.promise();

        Collector<Row, ? , Map<String, Integer>> collectorT = Collectors.toMap(row -> row.getColumnName(0),
                row -> row.getInteger("count"));
        Collector<Row, ? , List<JsonObject>> collector = Collectors.mapping(Row::toJson, Collectors.toList());

        String datetimeValue = queryParams.getOrDefault("datetime", null);

        FeatureQueryBuilder featureQuery = new FeatureQueryBuilder(collectionId);

        featureQuery.setLimit(Integer.parseInt(queryParams.get("limit")));
        featureQuery.setOffset(Integer.parseInt(queryParams.get("offset")));
        featureQuery.setCrs(String.valueOf(crs.get(queryParams.get("crs"))));
        featureQuery.setBboxCrsSrid(String.valueOf(crs.get(queryParams.get("bbox-crs"))));

        // Filter logic
        Map<String, String> filteredParams = new HashMap<>(queryParams);
        filteredParams.keySet().removeAll(WELL_KNOWN_QUERY_PARAMETERS);
        if (!filteredParams.isEmpty()) {
            String[] key = filteredParams.keySet().toArray(new String[filteredParams.size()]);
            featureQuery.setFilter(key[0], filteredParams.get(key[0]));
        }

        Future<String> sridOfStorageCrs = getSridOfStorageCrs(collectionId);

        Future<Void> bboxFuture = sridOfStorageCrs.compose(srid -> {
            LOGGER.debug("srid is: {}", srid);
            String queryBbox = queryParams.get("bbox");
            // Check for bbox limits from token
            List<Double> tokenBboxList = (limits != null && limits.getBboxLimit() != null) ? limits.getBboxLimitAsList() : null;
            String tokenBbox = null;
            if (tokenBboxList != null && !tokenBboxList.isEmpty()) {
                tokenBbox = tokenBboxList.stream()
                        .map(String::valueOf)
                        .collect(Collectors.joining(","));
                LOGGER.debug("Token bbox from limits: {}", tokenBbox);
            }

            if (queryBbox != null || tokenBbox != null) {
                LOGGER.debug("Entered into bbox check");
                LOGGER.debug("The token bbox is : {}", tokenBbox);

                if (queryBbox != null) {
                    queryBbox = queryBbox.replace("[", "").replace("]", "");
                    LOGGER.debug("The query param bbox is : {}", queryBbox);
                }

                Promise<Void> bboxPromise = Promise.promise();

                if (queryBbox != null && tokenBbox != null) {
                    // Use PostGIS to check bbox intersection
                    String sql = "SELECT ST_Intersects(" +
                            "ST_Transform(ST_MakeEnvelope(" + queryBbox + ", " + srid + "), 4326), " +
                            "ST_MakeEnvelope(" + tokenBbox + ", 4326)" +
                            ")";
                    String finalQueryBbox = queryBbox;
                    String finalTokenBbox = tokenBbox;
                    client.query(sql).execute()
                            .onSuccess(rows -> {
                                if (rows.iterator().hasNext()) {
                                    Row row = rows.iterator().next();
                                    boolean intersects = row.getBoolean(0);
                                    if (intersects) {
                                        LOGGER.debug("Both token bbox and query param bbox are getting intersected...");
                                        featureQuery.setBboxWhenTokenBboxExists(finalQueryBbox, finalTokenBbox, srid);
                                        bboxPromise.complete();
                                    } else {
                                        LOGGER.debug(BBOX_VIOLATES_CONSTRAINTS);
                                        bboxPromise.fail(new OgcException(403, "Forbidden", BBOX_VIOLATES_CONSTRAINTS));
                                    }
                                } else {
                                    bboxPromise.fail("No result from ST_Intersects check.");
                                }
                            })
                            .onFailure(err -> {
                                LOGGER.debug("PostGIS intersection check failed: {}", err.getMessage());
                                bboxPromise.fail(err);
                            });
                } else {
                    if (queryBbox != null) {
                        featureQuery.setBbox(queryBbox, srid);
                    } else if (tokenBbox != null) {
                        featureQuery.setBbox(tokenBbox, srid);
                    }
                    bboxPromise.complete();
                }

                return bboxPromise.future();
            } else {
                return Future.succeededFuture();
            }
        });

        // Enhanced feature limits handling with intersection check
        Future<Void> featLimitsFuture = bboxFuture.compose(v -> {
            // Check for feature limits from token
            if (limits != null && limits.getFeatLimit() != null && !limits.getFeatLimit().isEmpty()) {
                Map<String, List<String>> featLimits = limits.getFeatLimitAsMap();
                String tokenFeatCollectionId = featLimits.keySet().iterator().next();
                List<String> tokenFeatureIds = featLimits.get(tokenFeatCollectionId);
                String tokenFeatIds = String.join(",", tokenFeatureIds);

                LOGGER.debug("Processing feature limits from token - Collection: {}, Feature IDs: {}",
                        tokenFeatCollectionId, tokenFeatIds);

                Promise<Void> featLimitsPromise = Promise.promise();

                // Check if there's any intersection between request collection and token feature geometries
                String intersectionCheckSql =
                        "SELECT EXISTS(" +
                                "SELECT 1 FROM \"" + collectionId + "\" request_feature " +
                                "JOIN \"" + tokenFeatCollectionId + "\" token_feature " +
                                "ON ST_Intersects(request_feature.geom, token_feature.geom) " +
                                "WHERE token_feature.id IN (" + tokenFeatIds + ")" +
                                ")";

                LOGGER.debug("Feature intersection check SQL: {}", intersectionCheckSql);

                client.query(intersectionCheckSql).execute()
                        .onSuccess(rows -> {
                            if (rows.iterator().hasNext()) {
                                Row row = rows.iterator().next();
                                boolean hasIntersection = row.getBoolean(0);
                                if (hasIntersection) {
                                    LOGGER.debug("Feature intersection found, proceeding with feature limits enforcement");
                                    // Set feature limits in the query builder
                                    featureQuery.setFeatLimits(tokenFeatCollectionId, tokenFeatIds);
                                    featLimitsPromise.complete();
                                } else {
                                    LOGGER.debug("No intersection found between request collection and token feature boundaries");
                                    featLimitsPromise.fail(new OgcException(403, "Forbidden",
                                            "Feature not found within the allowed feature boundaries"));
                                }
                            } else {
                                featLimitsPromise.fail("No result from feature intersection check.");
                            }
                        })
                        .onFailure(err -> {
                            LOGGER.error("Feature intersection check failed: {}", err.getMessage());
                            featLimitsPromise.fail(err);
                        });

                return featLimitsPromise.future();
            } else {
                return Future.succeededFuture();
            }
        });
        // Continue only after both bboxFuture and featLimitsFuture complete
        return featLimitsFuture.compose(v -> sridOfStorageCrs.compose(srid ->
                client.withConnection(conn ->
                        conn.preparedQuery("select datetime_key from collections_details where id = $1::uuid")
                                .collecting(collector)
                                .execute(Tuple.of(UUID.fromString(collectionId)))
                                .compose(conn1 -> {
                                    if (conn1.value().get(0).getString("datetime_key") != null && datetimeValue != null ){
                                        String datetimeKey = conn1.value().get(0).getString("datetime_key");
                                        featureQuery.setDatetimeKey(datetimeKey);
                                        featureQuery.setDatetime(datetimeValue);
                                    }
                                            LOGGER.debug("datetime_key: {}",conn1.value().get(0).getString("datetime_key"));
                                            LOGGER.debug("<DBService> Sql query- {} ",  featureQuery.buildSqlString());
                                            LOGGER.debug("Count Query- {}", featureQuery.buildSqlString("count"));

                                            JsonObject resultJson = new JsonObject();
                                            return conn.preparedQuery(featureQuery.buildSqlString("count"))
                                                    .collecting(collectorT).execute()
                                                    .compose(count -> {
                                                        LOGGER.debug("Feature Count- {}", count.value().get("count"));
                                                        int totalCount = count.value().get("count");
                                                        resultJson.put("numberMatched", totalCount);

                                                        return conn.preparedQuery(featureQuery.buildSqlString())
                                                                .collecting(collector).execute()
                                                                .map(SqlResult::value)
                                                                .compose(success -> {
                                                                    if (!success.isEmpty())
                                                                        resultJson
                                                                                .put("features", new JsonArray(success))
                                                                                .put("numberReturned", success.size());
                                                                    else
                                                                        resultJson
                                                                                .put("features", new JsonArray())
                                                                                .put("numberReturned", 0);
                                                                    resultJson.put("type", "FeatureCollection");
                                                                    return Future.succeededFuture(resultJson);
                                                                });
                                                    });
                                        })))
                .onFailure(err -> {
                    LOGGER.error("Failed at getFeatures - {}", err.getMessage());
                    result.fail(err);
                }));
    }

    private Future<String> getSridOfStorageCrs(String collectionId) {
    LOGGER.info("getSridOfStorageCrs");
    Promise<String> result = Promise.promise();
    Collector<Row, ? , Map<String, Integer>> collector = Collectors.toMap(row -> row.getColumnName(0),
        row -> row.getInteger("srid"));
    client.withConnection(conn ->
        conn.preparedQuery("select srid from collections_details join crs_to_srid on collections_details.crs = " +
                " crs_to_srid.crs and collections_details.id = $1::uuid")
            .collecting(collector)
            .execute(Tuple.of(UUID.fromString(collectionId)))
            .onSuccess(success -> {
              LOGGER.debug("Srid of Storage Crs- {}",success.value().get("srid"));
              int srid = success.value().get("srid");
              result.complete(String.valueOf(srid));
            })
            .onFailure(fail -> {
              LOGGER.error("Something went wrong, {}", fail.getMessage());
              result.fail(new OgcException(500, "Internal Server Error", "Internal Server Error"));
            }));
    return result.future();
  }

  @Override
  public Future<Map<String, Integer>> isCrsValid(String collectionId, Map<String, String > queryParams) {
    Promise<Map<String, Integer>> result = Promise.promise();
    //check for both crs and bbox-crs
    String requestCrs = queryParams.get("crs");
    String bboxCrs = queryParams.getOrDefault("bbox-crs", DEFAULT_SERVER_CRS);

    if (requestCrs.equalsIgnoreCase(DEFAULT_SERVER_CRS) && bboxCrs.equalsIgnoreCase(DEFAULT_SERVER_CRS)) {
      result.complete(Map.of(DEFAULT_SERVER_CRS, DEFAULT_CRS_SRID));
      return result.future();
    }

    Collector<Row, ?, Map<String, Integer>> crsCollector = Collectors.toMap(row -> row.getString("crs"),
        row -> row.getInteger("srid"));
    client.withConnection(conn ->
      conn.preparedQuery("Select crs, srid from collection_supported_crs as colcrs join crs_to_srid as crsrid on " +
              "colcrs.crs_id = crsrid.id and colcrs.collection_id = $1::uuid")
          .collecting(crsCollector)
          .execute(Tuple.of(UUID.fromString(collectionId)))
          .onSuccess(success -> {
            LOGGER.debug("CRS:SRID-\n{}",success.value() );
            if (!success.value().containsKey(requestCrs)) {
              result.fail(new OgcException(400, "Bad Request", "Collection does not support this crs"));
            }
            if (!success.value().containsKey(bboxCrs)) {
              result.fail(new OgcException(400, "Bad Request", "Collection does not support this bbox-crs"));
            }
            success.value().put(DEFAULT_SERVER_CRS, DEFAULT_CRS_SRID);
            result.complete(success.value());
          })
          .onFailure(failed -> {
            LOGGER.error("Error: {}", failed.getMessage());
            result.fail(new OgcException(500, "Internal Server Error", "Internal Server Error"));
          })
    );
    return result.future();
  }

    @Override
    public Future<JsonObject> getFeature(String collectionId, Integer featureId, Map<String, String> queryParams,
                                         Limits limits, Map<String, Integer> crs) {
        LOGGER.info("getFeature");
        Promise<JsonObject> result = Promise.promise();

        Collector<Row, ?, List<JsonObject>> collector = Collectors.mapping(Row::toJson, Collectors.toList());
        String srid = String.valueOf(crs.get(queryParams.get("crs")));
        String geoColumn = "cast(st_asgeojson(st_transform(geom," + srid + "),9,0) as json)";

        // Step 1: Check if feature exists without any filters
        String checkExistSql = "SELECT 1 FROM \"" + collectionId + "\" WHERE id = $1::int";

        client.withConnection(conn ->
                conn.preparedQuery(checkExistSql).execute(Tuple.of(featureId))
                        .compose(existsResult -> {
                            if (existsResult.rowCount() == 0) {
                                // Feature does not exist at all
                                return Future.failedFuture(new OgcException(404, "Not found", "Feature not found"));
                            }

                            // Feature exists, now apply filters
                            return applySpatialFiltersAndGetFeature(conn, collectionId, featureId, limits, geoColumn, collector);
                        })
        ).onComplete(ar -> {
            if (ar.succeeded()) {
                result.complete(ar.result());
            } else {
                Throwable cause = ar.cause();
                if (cause instanceof OgcException) {
                    result.fail(cause);
                } else {
                    LOGGER.error("Failed at getFeature - {}", cause.getMessage(), cause);
                    result.fail(new OgcException(500, "Internal Server Error", cause.getMessage()));
                }
            }
        });

        return result.future();
    }

    private Future<JsonObject> applySpatialFiltersAndGetFeature(SqlConnection conn, String collectionId, Integer featureId,
                                                                Limits limits, String geoColumn,
                                                                Collector<Row, ?, List<JsonObject>> collector) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT request_feature.id, 'Feature' AS type, ")
                .append("cast(st_asgeojson(st_transform(request_feature.geom,")
                .append(geoColumn.substring(geoColumn.indexOf(",") + 1))
                .append(" as geometry, ")
                .append("(row_to_json(request_feature)::jsonb - 'id' - 'geom') as properties ");

        // Check if we have feature limits
        boolean hasFeatLimit = limits != null && limits.getFeatLimitAsMap() != null && !limits.getFeatLimitAsMap().isEmpty();

        // Check if we have bbox limits
        boolean hasBboxLimit = limits != null && limits.getBboxLimitAsList() != null && !limits.getBboxLimitAsList().isEmpty();

        if (hasFeatLimit) {
            // Use spatial intersection with boundary collection
            Map<String, List<String>> featLimits = limits.getFeatLimitAsMap();
            String boundaryCollectionId = featLimits.keySet().iterator().next();
            List<String> allowedFeatureIds = featLimits.get(boundaryCollectionId);
            String featLimit = String.join(",", allowedFeatureIds);

            sqlBuilder.append("FROM \"").append(collectionId).append("\" AS request_feature ")
                    .append("JOIN \"").append(boundaryCollectionId).append("\" AS token_feature ")
                    .append("ON ST_Intersects(request_feature.geom, token_feature.geom) ")
                    .append("WHERE request_feature.id = $1::int ")
                    .append("AND token_feature.id IN (").append(featLimit).append(")");
        } else if (hasBboxLimit) {
            // Only bbox filter
            List<Double> bboxList = limits.getBboxLimitAsList();
            String tokenBbox = bboxList.stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining(","));

            sqlBuilder.append("FROM \"").append(collectionId).append("\" AS request_feature ")
                    .append("WHERE request_feature.id = $1::int ")
                    .append("AND ST_Intersects(ST_Transform(request_feature.geom, 4326), ST_MakeEnvelope(")
                    .append(tokenBbox).append(", 4326))");
        } else {
            // No filters, just get the feature
            sqlBuilder.append("FROM \"").append(collectionId).append("\" AS request_feature ")
                    .append("WHERE request_feature.id = $1::int");
        }

        String finalSql = sqlBuilder.toString();
        LOGGER.debug("Executing query: {}", finalSql);

        return conn.preparedQuery(finalSql)
                .collecting(collector)
                .execute(Tuple.of(featureId))
                .map(SqlResult::value)
                .compose(features -> {
                    if (features.isEmpty()) {
                        if (hasFeatLimit) {
                            return Future.failedFuture(new OgcException(403, "Forbidden",
                                    "Feature not found within the allowed feature boundaries"));
                        } else if (hasBboxLimit) {
                            return Future.failedFuture(new OgcException(403, "Forbidden",
                                    "Feature not found within the bbox limit"));
                        } else {
                            return Future.failedFuture(new OgcException(404, "Not found", "Feature not found"));
                        }
                    } else {
                        return Future.succeededFuture(features.get(0));
                    }
                });
    }

    @Override
    public Future<Boolean> checkTokenCollectionAndFeatureIdsExist(String collectionId, List<String> featureIds) {
        LOGGER.debug("Checking if the Collection and Feature Ids in the token exist in the DB...");
        Promise<Boolean> result = Promise.promise();

        if (featureIds == null || featureIds.isEmpty()) {
            result.complete(false);
            return result.future();
        }

        // Step 1: Check if table (collection) exists
        String tableCheckSql = "SELECT to_regclass($1) IS NOT NULL AS exists";
        client.preparedQuery(tableCheckSql)
                .execute(Tuple.of(collectionId))
                .onSuccess(tableRows -> {
                    boolean tableExists = tableRows.iterator().next().getBoolean("exists");
                    if (!tableExists) {
                        LOGGER.warn("Collection table {} does not exist", collectionId);
                        result.fail(new OgcException(404, "Not Found", "Collection not found"));
                        return;
                    }

                    // Step 2: Check if all feature IDs exist using array parameter
                    String sql = "SELECT COUNT(id) as count FROM \"" + collectionId + "\" WHERE id = ANY($2::int[])";

                        // Convert feature IDs from String list to integer array
                        int[] featureIdArray = featureIds.stream()
                                .mapToInt(Integer::parseInt)
                                .toArray();

                        // Create tuple with collection ID and feature ID array
                        Tuple tuple = Tuple.of(UUID.fromString(collectionId), featureIdArray);

                        client.withConnection(conn ->
                                conn.preparedQuery(sql)
                                        .execute(tuple)
                                        .onSuccess(rows -> {
                                            int count = rows.iterator().next().getInteger("count");
                                            boolean allExist = count == featureIds.size();
                                            LOGGER.debug("Feature existence check for collection {}: {} out of {} features exist",
                                                    collectionId, count, featureIds.size());
                                            result.complete(allExist);
                                        })
                                        .onFailure(throwable -> {
                                            LOGGER.error("Error checking feature existence: {}", throwable.getMessage());
                                            result.fail(new OgcException(500, "Internal Server Error", "Error validating feature existence"));
                                        })
                        );
                })
                .onFailure(err -> {
                    LOGGER.error("Failed to check collection existence: {}", err.getMessage());
                    result.fail(new OgcException(500, "Internal Server Error", "Error checking collection existence"));
                });

        return result.future();
    }

    @Override
  public Future<List<JsonObject>> getStacCollections() {
    Promise<List<JsonObject>> result = Promise.promise();
    Collector<Row, ?, List<JsonObject>> collector =
        Collectors.mapping(Row::toJson, Collectors.toList());
    client.withConnection(
        conn ->
            conn.preparedQuery(
                    "Select collections_details.id, title, description,"
                 + " bbox, temporal, license FROM collections_details JOIN collection_type "
                 + "ON collections_details.id = collection_type.collection_id WHERE "
                 + "collection_type.type = 'STAC' ")
                .collecting(collector)
                .execute()
                .map(SqlResult::value)
                .onSuccess(
                    success -> {
                      if (success.isEmpty()) {
                        LOGGER.error("Collections table is empty!");
                        result.fail(
                            new OgcException(404, "Not found", "Collection table is Empty!"));
                      } else {
                        conn.preparedQuery("SELECT * FROM STAC_COLLECTIONS_ASSETS")
                            .collecting(collector)
                            .execute()
                            .map(SqlResult::value)
                            .onSuccess(
                                assets -> {
                                  if (assets.isEmpty()) {
                                    LOGGER.warn("Assets table is empty!");
                                    result.complete(success);
                                  } else {
                                    for (JsonObject asset : assets) {
                                      for (JsonObject successItem : success) {
                                        if (successItem
                                            .getString("id")
                                            .equals(asset.getString("stac_collections_id"))) {
                                          if (successItem.containsKey("assets")) {
                                            successItem.getJsonArray("assets").add(asset);
                                          } else {
                                            successItem.put("assets", new JsonArray().add(asset));
                                          }
                                        }
                                      }
                                    }
                                    result.complete(success);
                                  }
                                })
                            .onFailure(
                                fail -> {
                                  LOGGER.error("Failed to get Assets! - {}", fail.getMessage());
                                  result.fail("Error!");
                                });
                      }
                    })
                .onFailure(
                    fail -> {
                      LOGGER.error("Failed to getCollections! - {}", fail.getMessage());
                      result.fail("Error!");
                    }));
    return result.future();
  }

  @Override
  public Future<List<JsonObject>> getTileMatrixSets() {
    LOGGER.info("getTileMatrixSets");
    Promise<List<JsonObject>> result = Promise.promise();
    Collector<Row, ?, List<JsonObject>> collector = Collectors.mapping(Row::toJson, Collectors.toList());
    client.withConnection(conn ->
            conn.preparedQuery("Select title as id, title, uri from tms_metadata")
                .collecting(collector)
                .execute()
                .map(SqlResult::value))
        .onSuccess(success -> {
          if (success.isEmpty()) {
            LOGGER.error("TileMatrixSet_relation table is empty!");
            result.fail(new OgcException(404, "Not found", "TileMatrixSets (tiling scheme) not found"));
          } else {
            LOGGER.debug("TileMatrixSets Result: {}", success.toString());
            result.complete(success);
          }
        })
        .onFailure(fail -> {
          LOGGER.error("Failed to getTileMatrixSets(tiling scheme)! - {}", fail.getMessage());
          result.fail("Error!");
        });
    return result.future();
  }

  @Override
  public Future<JsonObject> getStacCollection(String collectionId) {
    LOGGER.info("getFeature");
    Promise<JsonObject> result = Promise.promise();

    Collector<Row, ?, List<JsonObject>> collector =
        Collectors.mapping(Row::toJson, Collectors.toList());
    Collector<Row, ?, List<JsonObject>> assetCollector =
        Collectors.mapping(Row::toJson, Collectors.toList());
    client.withConnection(
        conn ->
            conn.preparedQuery(
                            "SELECT collections_details.id, title, "
                + "description, bbox, temporal, license FROM collections_details "
                + "JOIN collection_type ON collections_details.id = collection_type.collection_id "
                + "WHERE collection_type.type = 'STAC' AND collections_details.id = $1::uuid ")
                .collecting(collector)
                .execute(Tuple.of(UUID.fromString(collectionId)))
                .map(SqlResult::value)
                .onSuccess(
                    success -> {
                      LOGGER.debug("DB result - {}", success);
                      if (success.isEmpty()) {
                        LOGGER.debug("Stac Collection of id {} Not Found!", collectionId);
                        result.fail(new OgcException(404, "Not found", "Collection not found"));
                      }
                      JsonObject collection = success.get(0);
                      String query =
                          "SELECT * from stac_collections_assets where stac_collections_id = $1::uuid";
                      conn.preparedQuery(query)
                          .collecting(assetCollector)
                          .execute(Tuple.of(UUID.fromString(collectionId)))
                          .map(SqlResult::value)
                          .onSuccess(
                              assetResult -> {
                                if (!assetResult.isEmpty()) {
                                  collection.put("assets", assetResult);
                                }
                                result.complete(collection);
                              })
                          .onFailure(
                              failed -> {
                                LOGGER.error("Failed at getFeature- {}", failed.getMessage());
                                result.fail("Error!");
                              });
                    })
                .onFailure(
                    fail -> {
                      LOGGER.error("Failed at getCollection- {}", fail.getMessage());
                      result.fail("Error!");
                    }));
    return result.future();
  }

  @Override
  public Future<List<JsonObject>> getStacItems(String collectionId, int limit, int offset) {
    Promise<List<JsonObject>> result = Promise.promise();
    Collector<Row, ?, List<JsonObject>> collector =
        Collectors.mapping(Row::toJson, Collectors.toList());
    // pagination
    String getItemsQuery = String.format("select item_table.id, cast(st_asgeojson(item_table.geom) as json) as" +
            " geometry, item_table.bbox, item_table.properties, item_table.p_id" +
            ", jsonb_agg((row_to_json(stac_items_assets.*)::jsonb - 'item_id')) as assetobjects" +
            ", 'Feature' as type, '%1$s' as collection from \"%1$s\" as item_table left join stac_items_assets" +
            " on item_table.id=stac_items_assets.item_id" +
            " group by item_table.id, item_table.geom, item_table.bbox, item_table.p_id, item_table.collection_id" +
            ", item_table.properties having p_id >= %2$d and item_table.collection_id = $1::uuid order by p_id limit " +
            "%3$d"
        , collectionId, offset, limit);

    checkIfCollectionExist(collectionId)
        .compose(collectionExist ->
            client.withConnection(
                conn -> conn.preparedQuery(getItemsQuery)
                    .collecting(collector)
                    .execute(Tuple.of(UUID.fromString(collectionId)))
                    .map(SqlResult::value))
        )
        .onSuccess(success -> {
          if(success.isEmpty()) {
            LOGGER.debug("No STAC items found!");
            result.complete(new ArrayList<>());
          } else {
            LOGGER.debug("STAC Items query successful.");
            LOGGER.debug(success);
            result.complete(success);
          }
        })
        .onFailure(failed -> {
          LOGGER.error("Failed to retrieve STAC items- {}", failed.getMessage());
          if (failed instanceof OgcException)
            result.fail(failed);
          else
            result.fail("Error!");
        });
    return result.future();
  }

  @Override
  public Future<JsonObject> getStacItemById(String collectionId, String stacItemId) {
    Promise<JsonObject> result = Promise.promise();
    Collector<Row, ?, List<JsonObject>> collector =
        Collectors.mapping(Row::toJson, Collectors.toList());
    String getItemQuery = String.format("select item_table.id, cast(st_asgeojson(item_table.geom) as json) as" +
        " geometry, item_table.bbox, item_table.properties" +
        ", jsonb_agg((row_to_json(stac_items_assets.*)::jsonb-'item_id')) as assetobjects, 'Feature' as type" +
        ", '%1$s' as collection from \"%1$s\" as item_table left join stac_items_assets" +
        " on item_table.id=stac_items_assets.item_id" +
        " group by item_table.id, item_table.geom, item_table.bbox, item_table.properties, item_table.collection_id" +
        " having item_table.id = $1::text and item_table.collection_id = $2::uuid", collectionId);
    checkIfCollectionExist(collectionId)
        .compose(collectionExist -> client.withConnection(
            conn -> conn.preparedQuery(getItemQuery)
                    .collecting(collector)
                    .execute(Tuple.of(stacItemId, UUID.fromString(collectionId)))
                    .map(SqlResult::value)))
        .onSuccess(success -> {
          if (success.isEmpty())
            result.fail(new OgcException(404, "NotFoundError", "Item " + stacItemId + " not found in " +
                "collection " + collectionId));
          else
            result.complete(success.get(0));
        })
        .onFailure(failed -> {
          LOGGER.error("Failed at stac_item_retrieval- {}", failed.getMessage());
          if (failed instanceof OgcException)
            result.fail(failed);
          else
            result.fail("Error!");
        });
    return result.future();
  }

  @Override
  public Future<JsonObject> stacItemSearch(StacItemSearchParams params) {
    LOGGER.debug("stacItemSearch");

    Promise<JsonObject> result = Promise.promise();

    Collector<Row, ?, List<JsonObject>> collector =
        Collectors.mapping(Row::toJson, Collectors.toList());

    FeatureQueryBuilder featureQuery = new FeatureQueryBuilder();

    featureQuery.setLimit(params.getLimit());
    featureQuery.setOffset(params.getOffset());

    if (!params.getBbox().isEmpty()) {
      featureQuery.setBboxCrsSrid(String.valueOf(DEFAULT_CRS_SRID));
      featureQuery.setBbox(
          params.getBbox().stream().map(i -> i.toString()).collect(Collectors.joining(",")),
          String.valueOf(DEFAULT_CRS_SRID));
    }

    if (params.getDatetime() != null) {
      featureQuery.setDatetimeKey(STAC_ITEMS_DATETIME_KEY);
      featureQuery.setDatetime(params.getDatetime());
    }

    if (!params.getCollections().isEmpty()) {
      featureQuery.setStacCollectionIds(params.getCollections().toArray(String[]::new));
    }

    if (!params.getIds().isEmpty()) {
      featureQuery.setStacItemIds(params.getIds().toArray(String[]::new));
    }

    if (params.getIntersects() != null) {
      featureQuery.setStacIntersectsGeom(params.getIntersects());
    }

    Tuple tuple = Tuple.tuple();
    String builtQuery = featureQuery.buildItemSearchSqlString(tuple);

    JsonObject resultJson = new JsonObject();

    client.withConnection(conn ->
        conn.preparedQuery(builtQuery)
          .collecting(collector).execute(tuple).map(SqlResult::value)
          .onSuccess(success -> {
            if (!success.isEmpty())
              resultJson
                  .put("features", new JsonArray(success))
                  .put("numberReturned", success.size());
            else
              resultJson
                  .put("features", new JsonArray())
                  .put("numberReturned", 0);
            resultJson.put("type", "FeatureCollection");
            result.complete(resultJson);
          })
          .onFailure(failed -> {
            LOGGER.error("Failed at getFeatures- {}",failed.getMessage());
            result.fail("Error!");
          }));
    return result.future();
    }

    @Override
    public Future<JsonObject> postStacCollection(JsonObject jsonObject) {
        LOGGER.debug("Inserting a new collection");
        Promise<JsonObject> result = Promise.promise();

        // Extract values
        String id = jsonObject.getString("id");
        String title = jsonObject.getString("title");
        String description = jsonObject.getString("description");
        String datetimeKey = jsonObject.getString("datetimeKey");
        String crs = jsonObject
                .getJsonObject("extent")
                .getJsonObject("spatial")
                .getString("crs", "http://www.opengis.net/def/crs/OGC/1.3/CRS84");
        String license = jsonObject.getString("license");
        String accessPolicy = jsonObject.getString("accessPolicy");
        String ownerUserId = jsonObject.getString("ownerUserId");
        String role = jsonObject.getString("role").toUpperCase();

        JsonArray bboxJsonArray = jsonObject.getJsonObject("extent").getJsonObject("spatial").getJsonArray("bbox").getJsonArray(0);
        Number[] bboxArray = bboxJsonArray.stream().map(num -> ((Number) num)).toArray(Number[]::new);

        JsonArray temporalJsonArray = jsonObject.getJsonObject("extent").getJsonObject("temporal").getJsonArray("interval").getJsonArray(0);
        String[] temporalArray = temporalJsonArray.stream().map(Object::toString).toArray(String[]::new);

        LOGGER.debug("id: {}, title: {}, description: {}, datetimeKey: {}, crs: {}, bboxArray: {}, temporalArray: {}, license: {}",
                id, title, description, datetimeKey, crs, bboxArray, temporalArray, license);

        client.withTransaction(
                conn ->
                        conn.preparedQuery(INSERT_COLLECTIONS_DETAILS)
                                .execute(
                                        Tuple.of(
                                                id,
                                                title,
                                                description,
                                                datetimeKey,
                                                crs,
                                                bboxArray,
                                                temporalArray,
                                                license))
                                .compose(
                                        res -> {
                                            LOGGER.debug("Item inserted in collections_details Table");
                                            return conn.preparedQuery(INSERT_COLLECTION_TYPE).execute(Tuple.of(id));
                                        })
                                .compose(
                                        insertRoles -> {
                                            LOGGER.debug("Item inserted in collection_type table");
                                            return conn.preparedQuery(INSERT_ROLES).execute(Tuple.of(ownerUserId, role));
                                        })
                                .compose(
                                        insertRiDetails -> {
                                            LOGGER.debug("Item inserted in roles table");
                                            return conn.preparedQuery(INSERT_RI_DETAILS)
                                                    .execute(Tuple.of(id, ownerUserId, accessPolicy));
                                        })
                                .compose(
                                        res -> {
                                            LOGGER.debug("Item inserted in ri_details table");
                                            return conn.query(CREATE_TABLE_BY_ID.replace("$1", id)).execute();
                                        })
                                .compose(
                                        res -> {
                                            LOGGER.debug("Table created by id name");
                                            return conn.query(ATTACH_PARTITION.replace("$1", id)).execute();
                                        })
                                .compose(
                                        res -> {
                                            LOGGER.debug("Partition attached, granting permissions...");
                                            return conn.query(
                                                            GRANT_PRIVILEGES.replace("$1", id).replace("$2", config.getString(DATABASE_USER)))
                                                    .execute();
                                        })
                                .compose(res -> {
                                    LOGGER.debug("All queries executed successfully! Notifying listeners...");
                                    return conn.preparedQuery(
                                                    RouterManager
                                                            .TRIGGER_SPEC_UPDATE_AND_ROUTER_REGEN_SQL
                                                            .apply("demo")).execute();
                                })
                                .onSuccess(
                                        res -> {
                                            LOGGER.debug("All queries executed successfully!");
                                            JsonObject response = new JsonObject().put("id", id);
                                            result.complete(response);
                                        })
                                .onFailure(
                                        failRes -> {
                                            LOGGER.error("Failed to execute queries: {}", failRes.getMessage());
                                            if(failRes.getMessage().contains("duplicate key value"))
                                            {
                                                OgcException ogcException =
                                                        new OgcException(
                                                                409, "Conflict", "STAC Collection Already Exists");
                                                result.fail(ogcException);
                                            } else {
                                                result.fail(failRes.getMessage());
                                            }
                                        }));

        return result.future();
    }

    @Override
    public Future<JsonObject> postStacCollections(JsonArray collectionDataList) {
        LOGGER.debug("posting the data in the db for multiple collections");
        List<UUID> failedIds = Collections.synchronizedList(new ArrayList<>()); // Thread-safe list

        return client.withTransaction(conn -> {
            List<Future<Object>> futures = collectionDataList.stream()
                    .map(obj -> {
                        JsonObject collectionData = (JsonObject) obj;
                        UUID id = UUID.fromString(collectionData.getString("id"));
                        String title = collectionData.getString("title");
                        String description =  collectionData.getString("description");
                        String dateTimeKey = collectionData.getString("datetimeKey");
                        String crs =   collectionData.getString("crs");
                        String license =   collectionData.getString("license");
                        String ownerUserId =  collectionData.getString("ownerUserId");
                        String role =  collectionData.getString("role").toUpperCase();
                        String accessPolicy = collectionData.getString("accessPolicy");

                        JsonArray bboxJsonArray = collectionData.getJsonObject("extent").getJsonObject("spatial").getJsonArray("bbox").getJsonArray(0);
                        Number[] bboxArray = bboxJsonArray.stream().map(num -> ((Number) num)).toArray(Number[]::new);

                        JsonArray temporalJsonArray = collectionData.getJsonObject("extent").getJsonObject("temporal").getJsonArray("interval").getJsonArray(0);
                        String[] temporalArray = temporalJsonArray.stream().map(Object::toString).toArray(String[]::new);

                        LOGGER.debug("Processing collection ID: {}", id);

                        return conn.preparedQuery(INSERT_COLLECTIONS_DETAILS)
                                .execute(Tuple.of(id,title, description,dateTimeKey, crs,bboxArray,temporalArray,license))
                                .mapEmpty()
                                .compose(res -> conn.preparedQuery(INSERT_COLLECTION_TYPE).execute(Tuple.of(id)).mapEmpty())
                                .compose(res -> conn.preparedQuery(INSERT_ROLES).execute(Tuple.of(ownerUserId, role)).mapEmpty())
                                .compose(res -> conn.preparedQuery(INSERT_RI_DETAILS).execute(Tuple.of(
                                        id,
                                      ownerUserId,accessPolicy
                                        )).mapEmpty())
                                .compose(res -> conn.query(CREATE_TABLE_BY_ID.replace("$1", id.toString())).execute().mapEmpty())
                                .compose(res -> conn.query(ATTACH_PARTITION.replace("$1", id.toString())).execute().mapEmpty())
                                .compose(res -> conn.query(GRANT_PRIVILEGES.replace("$1", id.toString())
                                        .replace("$2", config.getString(DATABASE_USER))).execute().mapEmpty())
                                .compose(res -> conn.preparedQuery(RouterManager.TRIGGER_SPEC_UPDATE_AND_ROUTER_REGEN_SQL.apply("demo")).execute().mapEmpty())
                                .onFailure(err -> {
                                    LOGGER.error("Insert failed for ID {}: {}", id, err.getMessage());
                                    failedIds.add(id);
                                });
                    })
                    .collect(Collectors.toList());

            return CompositeFuture.all(futures.stream().map(f -> (Future<?>) f).collect(Collectors.toList()))
                    .map(v -> {
                        LOGGER.info("All collections have been successfully onboarded!");
                        return new JsonObject().put("status", "success");
                    })
                    .recover(err -> {
                        JsonObject jsonError = new JsonObject()
                                .put("failedIds", new JsonArray(failedIds))
                                .put("description", err.getMessage());

                        LOGGER.error("Transaction failed, rolling back: {}", jsonError);
                        return Future.failedFuture(jsonError.encode());
                    });
        });
    }

    @Override
    public Future<JsonObject> updateStacCollection(JsonObject requestBody) {
        LOGGER.debug("Updating the Collection!");
        Promise<JsonObject> result = Promise.promise();
        String id = requestBody.getString("id");
        String title = requestBody.getString("title");
        String description = requestBody.getString("description");
        String datetimeKey = requestBody.getString("datetimeKey");
        String crs = requestBody.getString("crs");
        String license = requestBody.getString("license");

        JsonArray bboxJsonArray = requestBody.getJsonObject("extent").getJsonObject("spatial").getJsonArray("bbox").getJsonArray(0);
        Number[] bboxArray = bboxJsonArray.stream().map(num -> ((Number) num)).toArray(Number[]::new);

        JsonArray temporalJsonArray = requestBody.getJsonObject("extent").getJsonObject("temporal").getJsonArray("interval").getJsonArray(0);
        String[] temporalArray = temporalJsonArray.stream().map(Object::toString).toArray(String[]::new);

        LOGGER.debug("id: {}, title: {}, description: {}, datetimeKey: {}, crs: {}, bboxArray: {}, temporalArray: {}, license: {}",
                id, title, description, datetimeKey, crs, bboxArray, temporalArray, license);

        client.preparedQuery(UPDATE_COLLECTIONS_DETAILS)
                .execute(Tuple.of(id, title, description, datetimeKey, crs, bboxArray, temporalArray, license))
                .onSuccess(res ->
                {
                    LOGGER.debug("Update in collections_Details successful!");
                    result.complete();
                })
                .onFailure(err ->
                {
                    LOGGER.error("Failed to update collections_Details: {}", err.getMessage());
                    result.fail("Collection update failed");
                });


        return result.future();
    }

  @Override
  public Future<JsonObject> insertStacItems(JsonObject requestBody) {
      LOGGER.debug("Inside insertStacItems");
      Promise<JsonObject> result = Promise.promise();
      String collectionId = requestBody.getString("collectionId");
      String[] itemIds = requestBody.getJsonArray("features").stream()
          .map(obj -> {
            JsonObject feature = (JsonObject) obj;
            return feature.getString("id");
          }).toArray(String[]::new);
      List<Future<Void>> insertFut = new ArrayList<>();

      checkIfCollectionExist(collectionId)
          .compose(collection -> checkIfItemsExist(itemIds, collectionId))
          .compose(items -> {
            requestBody.getJsonArray("features")
                .forEach(feature -> {
                  JsonObject featureJson = (JsonObject) feature;
                  featureJson.put("collectionId", collectionId);
                  insertFut.add(insertItemIntoDb(featureJson));
                });
            return Future.join(insertFut);
          })
          .onSuccess(success -> {
            LOGGER.info("STAC items have been created.");
            result.complete(new JsonObject().put("code", "Items are created."));
          }).onFailure(failed -> {
            LOGGER.error("Something went wrong! Error: {}", failed.getMessage());
            result.fail(failed);
          });
      return result.future();
  }

  @Override
  public Future<JsonObject> insertStacItem(JsonObject feature) {
      LOGGER.debug("Inside insertStacItem");
      Promise<JsonObject> result = Promise.promise();
      String itemId = feature.getString("id");
      String collectionId = feature.getString("collectionId");
      checkIfCollectionExist(collectionId)
        .compose(collection -> checkIfItemExist(itemId,collectionId))
        .compose(item -> insertItemIntoDb(feature))
        .compose(insert -> getStacItemById(collectionId,itemId))
        .onSuccess(result::complete)
        .onFailure(failed -> {
          LOGGER.error("Failed at getting stac item- {}",failed.getMessage());
          result.fail(failed);
        });
    return result.future();
  }

  @Override
  public Future<JsonObject> getAccessDetails(String collectionId) {
    Promise<JsonObject> result = Promise.promise();
    Collector<Row, ?, List<JsonObject>> collector =
        Collectors.mapping(Row::toJson, Collectors.toList());
    String getAccessDetailsQuery = "select id, role_id from ri_details where id = $1::uuid";
    client.withConnection(conn ->
        conn.preparedQuery(getAccessDetailsQuery)
            .collecting(collector)
            .execute(Tuple.of(UUID.fromString(collectionId)))
            .map(SqlResult::value)
            .onSuccess(success -> {
              if(success.isEmpty()) {
                LOGGER.error("Collection not found in ri_details!");
                result.fail(new OgcException(404, "Not Found", "Collection not found"));
                return;
              }
              result.complete(success.get(0));
            })
            .onFailure(failed -> {
              LOGGER.error("Failed to get access details for collectionId {}, E- {}",collectionId, failed.getMessage());
              result.fail("Error!");
            }));
    return result.future();
  }

  @Override
  public Future<JsonObject> updateStacItem(JsonObject stacItem) {
    Promise<JsonObject> result = Promise.promise();
    String itemId = stacItem.getString("itemId");
    String collectionId = stacItem.getString("collectionId");
    Double[] bboxArray = stacItem.containsKey("bbox") ? stacItem.getJsonArray("bbox").stream()
        .map(obj -> obj instanceof Number ? ((Number) obj).doubleValue() : 0.0)
        .toArray(Double[]::new) : null;
    String geometry = stacItem.containsKey("geometry") ? stacItem.getJsonObject("geometry").toString() : null;
    JsonObject properties = stacItem.containsKey("properties") ? stacItem.getJsonObject("properties") : null;
    String updateItemQuery = "UPDATE stac_collections_part SET bbox = COALESCE($3, bbox)," +
        " geom = COALESCE(st_geomfromgeojson($4), geom), properties = COALESCE($5::jsonb, properties)" +
        " WHERE id = $1 and collection_id = $2";

    checkIfCollectionExist(collectionId)
        .compose(collection -> checkIfItemExistForUpdate(itemId, collectionId))
        .compose(item -> client.withTransaction(conn ->
            conn.preparedQuery(updateItemQuery)
                .execute(Tuple.of(itemId,collectionId,bboxArray, geometry, properties))
                .compose(updateItem -> {
                  if (!stacItem.containsKey("assets"))
                    return Future.succeededFuture();
                  else {
                    if (stacItem.getJsonObject("assets").isEmpty())
                      return Future.succeededFuture();
                    JsonObject assets = stacItem.getJsonObject("assets");
                    LOGGER.debug("Updated Item in stac_collection_part");
                    List<Tuple> batchInserts = new ArrayList<>();
                    assets.stream().forEach(asset -> {
                      String assetId = asset.getKey();
                      JsonObject assetJsonObj = (JsonObject) asset.getValue();
                      String title = assetJsonObj.containsKey("title")
                          ? assetJsonObj.getString("title") : null;
                      String description = assetJsonObj.containsKey("description") ?
                          assetJsonObj.getString("description") : null;
                      String href = assetJsonObj.containsKey("href") ? assetJsonObj.getString("href") : null;
                      String type = assetJsonObj.containsKey("type")
                          ? assetJsonObj.getString("type") : null;
                      Long size = assetJsonObj.containsKey("size") ? assetJsonObj.getLong("size") : null;
                      JsonArray rolesJsonArr = assetJsonObj.containsKey("roles")
                          ? assetJsonObj.getJsonArray("roles") : null;
                      String[] roles = (rolesJsonArr != null) ? rolesJsonArr.stream()
                          .map(Object::toString).toArray(String[]::new) : null;
                      JsonObject assetProperties =
                          assetJsonObj.containsKey("properties") ? assetJsonObj.getJsonObject("properties") : null;
                      String s3BucketId = assetJsonObj.getString("s3BucketId");
                      batchInserts.add(
                          Tuple.of(UUID.fromString(assetId), collectionId, itemId, title, description, href, type, size,
                          roles, assetProperties, s3BucketId));
                    });
                    return conn.preparedQuery("INSERT INTO stac_items_assets as asset_table" +
                            " (id, collection_id, item_id, title, description, href, type, size, roles, properties, s3_bucket_id)" +
                            " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11) " +
                            " ON CONFLICT (id) DO UPDATE SET title = COALESCE (EXCLUDED.title, asset_table.title)" +
                            ", description = COALESCE(EXCLUDED.description, asset_table.description)" +
                            ", href = COALESCE(EXCLUDED.href, asset_table.href)" +
                            ", type= COALESCE(EXCLUDED.type, asset_table.type)" +
                            ", size = COALESCE(EXCLUDED.size, asset_table.size)" +
                            ", roles = COALESCE(EXCLUDED.roles, asset_table.roles)" +
                            ", properties = COALESCE(EXCLUDED.properties, asset_table.properties)" +
                            ", s3_bucket_id = COALESCE(EXCLUDED.s3_bucket_id, asset_table.s3_bucket_id)")
                        .executeBatch(batchInserts);
                  }
                })
      ))
        .compose(assetInsert -> getStacItemById(collectionId,itemId))
        .onSuccess(success -> {
            LOGGER.info("Stac Item updated.");
            result.complete(success);
          })
          .onFailure(failed -> {
            LOGGER.error("Failed to update Stac Item. \nError: {}", failed.getMessage());
            failed.printStackTrace();
            if (failed.getMessage().contains("violates not-null constraint")) {
              result.fail(new OgcException(400, "Bad Request", "either [id, collection_id, item_id, size, roles, " +
                  "href, type is empty/null on updating]"));
            } else
                result.fail(failed);
          });
    return result.future();
  }


    @Override
    public Future<List<JsonObject>> getOgcRecords(String catalogId) {
        Promise <List<JsonObject>> result = Promise.promise();
        Collector<Row, ?, List<JsonObject>> collector = Collectors.mapping(Row::toJson, Collectors.toList());
        String tableName = "public.\"" + catalogId + "\"";
        String query = String.format(
                "SELECT id, ST_AsGeoJSON(geometry)::json AS geometry, created, title, description, keywords, bbox, temporal, collection_id, provider_name, provider_contacts FROM %s",
                tableName
        );
        client.withConnection(conn ->
                        conn.query(query)
                                .collecting(collector)
                                .execute()
                                .map(SqlResult::value))
                .onSuccess(success -> {
                    LOGGER.debug("Record Items Successfully fetched!");
                    if (success.isEmpty()) {
                        LOGGER.error("Records table is empty!");
                        result.fail(
                                new OgcException(404, "Not found", "Record table is Empty!"));
                        return;
                    }
                    result.complete(success);
                })
                .onFailure(fail -> {
                    LOGGER.error("Failed to getOgcRecords! - {}", fail.getMessage());
                    result.fail("Error!");
                });

        return result.future();
    }

    @Override
    public Future<JsonObject> getOgcRecordItem(String catalogId, String recordId) {

        Promise <JsonObject> result = Promise.promise();
        Collector<Row, ?, List<JsonObject>> collector = Collectors.mapping(Row::toJson, Collectors.toList());
        String tableName = "public.\"" + catalogId + "\"";
        String query = String.format(
                "SELECT id, ST_AsGeoJSON(geometry)::json AS geometry, created, title, description, keywords, bbox, temporal, collection_id, provider_name, provider_contacts FROM %s WHERE id = $1::int",
                tableName
        );
        client.withConnection(conn ->
                        conn.preparedQuery(query)
                                .collecting(collector)
                                .execute(Tuple.of(Integer.parseInt(recordId)))
                                .map(SqlResult::value))
                .onSuccess(success -> {
                    LOGGER.debug("Record Item Successfully fetched");
                    if(success.isEmpty())
                    {
                        LOGGER.debug("handleeee");
                        LOGGER.error("Record is not present!");
                        result.fail(
                                new OgcException(404, "Not found", "Record Not Found!"));
                        return;

                    }
                    result.complete(success.get(0));
                })
                .onFailure(fail -> {
                    LOGGER.error("Failed to getOgcRecords! - {}", fail.getMessage());
                    result.fail("Error!");
                });

        return result.future();
    }

    private  Future<JsonObject> getStacItemWithoutAssets(String collectionId, String itemId) {
    Promise<JsonObject> result = Promise.promise();
    Collector<Row, ?, List<JsonObject>> collector =
        Collectors.mapping(Row::toJson, Collectors.toList());
    String getItemQuery = String.format("select id, cast(st_asgeojson(geom) as json) as geometry, bbox, " +
        " properties, 'Feature' as type,  '%1$s' as collection from \"%1$s\" where id = $1::text", collectionId);
    client.withConnection(
        conn ->
            conn.preparedQuery(getItemQuery)
                .collecting(collector)
                .execute(Tuple.of(itemId))
                .map(SqlResult::value)
                .onSuccess(success -> {
                  if (success.isEmpty())
                    result.fail(new OgcException(404, "Not Found", "Item " + itemId + " not found in " +
                        "collection " + collectionId));
                  else
                    result.complete(success.get(0));
                })
                .onFailure(failed -> {
                  LOGGER.error("Failed at STAC Item retrieval- {}", failed.getMessage());
                  result.fail("Error!");
                }));
    return result.future();
  }


  private Future<Void> checkIfCollectionExist(String collectionId) {
    Promise<Void> promise = Promise.promise();
    client.withConnection(conn -> conn.preparedQuery("SELECT 1 FROM collections_details WHERE id = $1")
          .execute(Tuple.of(collectionId))
          .onSuccess(collection -> {
            if (collection.rowCount() > 0)
              promise.complete();
            else {
              LOGGER.error("Error: Collection not found in db!");
              promise.fail(new OgcException(404, "Not Found", "Collection not found."));
            }
          })
          .onFailure(failed -> {
            LOGGER.error("Something went wrong. {}", failed.getMessage());
            promise.fail(failed);
          }));
      return promise.future();
  }

  private Future<Void> checkIfItemExist(String itemId, String collectionId) {
    Promise<Void> promise = Promise.promise();
      client.withConnection(conn ->
          conn.preparedQuery("SELECT 1 FROM stac_collections_part WHERE id = $1 and collection_id = $2::uuid")
          .execute(Tuple.of(itemId, UUID.fromString(collectionId)))
              .onSuccess(item -> {
                if (item.rowCount() > 0) {
                  LOGGER.error("One or more STAC item(s) exist!");
                  promise.fail(new OgcException(409, "Conflict", "One ore more STAC item(s) exist!"));
                }
                else
                  promise.complete();
              })
              .onFailure(failed -> {
                  LOGGER.error("Something went wrong. {}", failed.getMessage());
                  promise.fail("Error!");
              }));
      return promise.future();
  }

  private Future<Void> checkIfItemExistForUpdate(String itemId, String collectionId) {
    Promise<Void> promise = Promise.promise();
    client.withConnection(conn ->
        conn.preparedQuery("SELECT 1 FROM stac_collections_part WHERE id = $1 and collection_id = $2::uuid")
            .execute(Tuple.of(itemId, UUID.fromString(collectionId)))
            .onSuccess(item -> {
              if (item.rowCount() == 0) {
                LOGGER.error("No STAC Item exists to update!");
                promise.fail(new OgcException(404, "Not found", "No STAC Item exists to update"));
              }
              else
                promise.complete();
            })
            .onFailure(failed -> {
              LOGGER.error("Something went wrong. {}", failed.getMessage());
              promise.fail("Error!");
            }));
    return promise.future();
  }

  private Future<Void> checkIfItemsExist(String[] itemIds, String collectionId) {
    Promise<Void> promise = Promise.promise();
    Collector<Row, ?, List<String>> collector = Collectors.mapping(row -> row.getString("id"), Collectors.toList());
    client.withConnection(conn ->
        conn.preparedQuery("SELECT id FROM stac_collections_part WHERE id = ANY($1::text[]) and collection_id = " +
                "$2::uuid")
            .collecting(collector)
            .execute(Tuple.of(itemIds, UUID.fromString(collectionId)))
            .map(SqlResult::value)
            .onSuccess(ids -> {
              if (!ids.isEmpty()) {
                LOGGER.error("One or more STAC item(s) exist!");
                promise.fail(new OgcException(409, "Conflict", "The following items are already present - "+ ids +
                    ". Please remove them and try again "));
              }
              else
                promise.complete();
            })
            .onFailure(failed -> {
              LOGGER.error("Something went wrong. {}", failed.getMessage());
              promise.fail(failed.getMessage());
            }));
    return promise.future();
  }
  private Future<Void> insertItemIntoDb (JsonObject stacItem) {
      Promise<Void> result = Promise.promise();
      String itemId = stacItem.getString("id");
      String collectionId = stacItem.getString("collectionId");
      JsonArray bbox = stacItem.containsKey("bbox") ? stacItem.getJsonArray("bbox") : new JsonArray();
      Double[] bboxArray = (bbox != null) ? bbox.stream()
        .map(obj -> obj instanceof Number ? ((Number) obj).doubleValue() : 0.0)
        .toArray(Double[]::new) : new Double[0];
      JsonObject geometry = stacItem.containsKey("geometry") ? stacItem.getJsonObject("geometry") : new JsonObject();
      JsonObject properties = stacItem.containsKey("properties") ? stacItem.getJsonObject("properties") : new JsonObject();
      client.withTransaction(conn ->
          conn.preparedQuery("INSERT INTO stac_collections_part(id, collection_id, bbox, geom, properties)" +
                  " VALUES ($1, $2::uuid, $3, st_geomfromgeojson($4), $5::jsonb)")
              .execute(Tuple.of(itemId, UUID.fromString(collectionId), bboxArray, geometry.toString(), properties))
              .compose(sql -> {
                if (!stacItem.containsKey("assets"))
                  return Future.succeededFuture();
                else {
                  if (stacItem.getJsonObject("assets").isEmpty())
                    return Future.succeededFuture();
                  JsonObject assets = stacItem.getJsonObject("assets");
                  LOGGER.debug("Inserted into stac_collection_part");
                  List<Tuple> batchInserts = new ArrayList<>();
                  assets.stream().forEach(asset -> {
                    String assetId = asset.getKey();
                    JsonObject assetJsonObj = (JsonObject) asset.getValue();
                    String title = assetJsonObj.containsKey("title")
                        ? assetJsonObj.getString("title") : "";
                    String description = assetJsonObj.containsKey("description") ?
                        assetJsonObj.getString("description") : "";
                    String href = assetJsonObj.getString("href");
                    String type = assetJsonObj.containsKey("type")
                        ? assetJsonObj.getString("type") : "";
                    long size = assetJsonObj.containsKey("size") ? assetJsonObj.getLong("size") : 0;
                    JsonArray rolesJsonArr = assetJsonObj.containsKey("roles")
                        ? assetJsonObj.getJsonArray("roles") : new JsonArray();
                    String[] roles = (rolesJsonArr != null) ? rolesJsonArr.stream()
                        .map(Object::toString).toArray(String[]::new) : new String[0];
                    JsonObject assetProperties = assetJsonObj.containsKey("properties")
                        ? assetJsonObj.getJsonObject("properties") : new JsonObject();
                    String s3BucketId = assetJsonObj.getString("s3BucketId");
                    batchInserts.add(Tuple.of(UUID.fromString(assetId), collectionId, itemId, title, description, href,
                        type, size, roles, assetProperties, s3BucketId));
                  });
                  return conn.preparedQuery("INSERT INTO stac_items_assets" +
                          " (id, collection_id, item_id, title, description, href, type, size, roles, properties, s3_bucket_id)" +
                          " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)")
                      .executeBatch(batchInserts);
                }
              }))
          .onSuccess(success -> {
            LOGGER.info("Stac Item created.");
            result.complete();
          })
          .onFailure(failed -> {
            LOGGER.error("Failed to create Stac Item. \nError: {}", failed.getMessage());
            failed.printStackTrace();
            result.fail(failed);
          });
      return result.future();
  }
  @Override
  public Future<List<JsonObject>> getTileMatrixSetMetaData(String tileMatrixSet) {
    LOGGER.info("getTileMatrixSetMetaData");
    Promise<List<JsonObject>> result = Promise.promise();
    Collector<Row, ?, List<JsonObject>> collector = Collectors.mapping(Row::toJson, Collectors.toList());
    client.withConnection(conn ->
            conn.preparedQuery("Select tmsr.id as id, tmsr.title as title, tmsr.uri as uri, pointoforigin, tilewidth," +
                    " tileheight, crs, tmsm.tilematrix_id, tmsm.title as tilematrixmeta_title, tmsm.description, " +
                    "scaledenominator, cellsize, corneroforigin, matrixwidth, matrixheight" +
                    " from tilematrixsets_relation as tmsr join tilematrixset_metadata tmsm on" +
                    " tmsr.id = tmsm.tilematrixset_id where tmsr.id = $1::text")
                .collecting(collector)
                .execute(Tuple.of(tileMatrixSet))
                .map(SqlResult::value))
        .onSuccess(success -> {
          if (success.isEmpty()) {
            LOGGER.error("TileMatrixSet_relation or TileMatrixset_metadata table is empty!");
            result.fail(new OgcException(404, "Not found", "TileMatrixSets (tiling scheme) not found"));
          } else {
            LOGGER.debug("TileMatrices Result: {}", success.toString());
            result.complete(success);
          }
        })
        .onFailure(fail -> {
          LOGGER.error("Failed to getTileMatrixSetsRelation(tiling scheme)! - {}", fail.getMessage());
          result.fail("Error!");
        });
    return result.future();
  }

  @Override
  public Future<List<JsonObject>> getTileMatrixSetRelation(String collectionId) {
    LOGGER.info("getTileMatrixSetRelation");
    Promise<List<JsonObject>> result = Promise.promise();
    Collector<Row, ?, List<JsonObject>> collector = Collectors.mapping(Row::toJson, Collectors.toList());
    client.withConnection(conn ->
              conn.preparedQuery("select tmsr.collection_id as collection_id, tms_meta.crs" +
                      " , tms_meta.title as tilematrixset, tms_meta.uri, ctype.type as datatype" +
                      " from tilematrixsets_relation as tmsr join tms_metadata as tms_meta" +
                      " on tmsr.tms_id = tms_meta.id join collection_type as ctype " +
                      " on ctype.collection_id=tmsr.collection_id where ctype.collection_id = $1::uuid" +
                      " and (ctype.type = 'VECTOR' or ctype.type = 'MAP')")
                .collecting(collector)
                .execute(Tuple.of(collectionId))
                .map(SqlResult::value))
        .onSuccess(success -> {
          if (success.isEmpty()) {
            LOGGER.error("TileMatrixSet_relation or collections_details table is empty!");
            result.fail(new OgcException(404, "Not found", "TileSetList not found"));
          } else {
            LOGGER.debug("TileSets Result: {}", success.toString());
            result.complete(success);
          }
        })
        .onFailure(fail -> {
          LOGGER.error("Failed to getTileSetList! - {}", fail.getMessage());
          result.fail("Error!");
        });
    return result.future();

  }

  @Override
  public Future<List<JsonObject>> getTileMatrixSetRelationOverload(String collectionId, String tileMatrixSetId) {
    LOGGER.info("getTileMatrixSetRelation<collId,tileMatrixSetId>");
    Promise<List<JsonObject>> result = Promise.promise();
    Collector<Row, ?, List<JsonObject>> collector = Collectors.mapping(Row::toJson, Collectors.toList());
    client.withConnection(conn ->
              conn.preparedQuery("select tmsr.collection_id as collection_id, tms_meta.crs," +
                      " tms_meta.title as tilematrixset, tms_meta.title as tilematrixset_title, tms_meta.uri," +
                      " ctype.type as datatype from tilematrixsets_relation as tmsr join tms_metadata as tms_meta" +
                      " on tmsr.tms_id = tms_meta.id join collection_type as ctype" +
                      " on ctype.collection_id=tmsr.collection_id" +
                      " where ctype.collection_id = $1::uuid and (ctype.type = 'VECTOR' or ctype.type = 'MAP')" +
                      " and tms_meta.title = $2::text")
                .collecting(collector)
                .execute(Tuple.of(collectionId, tileMatrixSetId))
                .map(SqlResult::value))
        .onSuccess(success -> {
          if (success.isEmpty()) {
            LOGGER.error("TileMatrixSet_relation or collections_details table is empty!");
            result.fail(new OgcException(404, "Not found", "TileSetList not found"));
          } else {
            LOGGER.debug("TileSets Result: {}", success.toString());
            result.complete(success);
          }
        })
        .onFailure(fail -> {
          LOGGER.error("Failed to getTileSetList! - {}", fail.getMessage());
          result.fail("Error!");
        });
    return result.future();
  }

  @Override
  public Future<String> getTileS3BucketId(String collectionId, String tileMatrixSetId) {
    Promise<String> result = Promise.promise();
    client.withConnection(conn ->
              conn.preparedQuery("SELECT s3_bucket_id FROM tilematrixsets_relation AS tmsr join tms_metadata AS tms_meta" +
                      " ON tmsr.tms_id = tms_meta.id WHERE collection_id = $1::uuid AND tms_meta.title = $2::text")
                .execute(Tuple.of(collectionId, tileMatrixSetId))
                .map(SqlResult::value))
        .onSuccess(success -> {
          if (success.rowCount() == 0) {
            result.fail(new OgcException(404, "Failed to get tile", "Could not get S3 bucket id for collection + TMS"));
          } else {
            result.complete(success.iterator().next().getString("s3_bucket_id"));
          }
        })
        .onFailure(fail -> {
          LOGGER.error("Failed S3 bucket id for collection + TMS! - {}", fail.getMessage());
          result.fail("Error!");
        });
    return result.future();
  }

  @Override
  public Future<JsonObject> getAssets(String assetId) {
    LOGGER.info("get details of assets");
    Promise<JsonObject> result = Promise.promise();
    Collector<Row, ?, List<JsonObject>> collector =
        Collectors.mapping(Row::toJson, Collectors.toList());
    client
        .withConnection(
            conn ->
                conn.preparedQuery("select * from stac_collections_assets where id = $1::uuid")
                    .collecting(collector)
                    .execute(Tuple.of(UUID.fromString(assetId)))
                    .map(SqlResult::value))
        .onSuccess(
            success -> {
              if (success.isEmpty()) {
                LOGGER.info("Given asset is not present in stac_collections_assets table. Trying stac_items_assets " +
                    "table...");
                client.withConnection(conn1 ->
                  conn1.preparedQuery("select * from stac_items_assets where id = $1::uuid")
                      .collecting(collector)
                      .execute(Tuple.of(UUID.fromString(assetId)))
                      .map(SqlResult::value)
                      .onSuccess(successAsset -> {
                        if (successAsset.isEmpty()) {
                          LOGGER.error("Given asset is not present in either stac_collections_assets or " +
                              "stac_items_assets table. Trying collections_enclosure table.");
                          client.withConnection(conn2 ->
                            conn2.preparedQuery("select * from collections_enclosure where id = $1::uuid")
                                    .collecting(collector)
                                    .execute(Tuple.of(UUID.fromString(assetId)))
                                    .map(SqlResult::value)
                                    .onSuccess(successAssetEnclosure -> {
                                      if (successAssetEnclosure.isEmpty()) {
                                        LOGGER.error(" Given asset is not present in either stac_collection_assets, " +
                                                        "stac_items_assets table or collections_enclosure");
                                        result.fail(new OgcException(404, "Not found", "Asset not found"));
                                      } else {
                                        LOGGER.debug("Asset Result: {}", successAssetEnclosure.get(0));
                                        result.complete(successAssetEnclosure.get(0));
                                      }
                                    })
                                    .onFailure(failedAssetEnclosure -> {
                                      LOGGER.error("Failed to get assets! - {}", failedAssetEnclosure.getMessage());
                                      result.fail("Error!");
                                    }));
                                                  }
                        else {
                          LOGGER.debug("Asset Result: {}", successAsset.get(0));
                          result.complete(successAsset.get(0));
                        }
                      })
                      .onFailure(failedOp -> {
                        LOGGER.error("Failed to get assets! - {}", failedOp.getMessage());
                        result.fail("Error!");
                      }));
              } else {
                LOGGER.debug("Asset Result: {}", success.get(0));
                result.complete(success.get(0));
              }
            })
        .onFailure(
            fail -> {
              LOGGER.error("Failed to get assets! - {}", fail.getMessage());
              result.fail("Error!");
            });
    return result.future();
  }
  public Future<JsonObject> getProcesses(int limit) {
    Promise<JsonObject> promise = Promise.promise();
    String sqlQuery =
      "SELECT version, id, title, description, mode AS \"jobControlOptions\", keywords, response AS \"outputTransmission\" FROM " +
        PROCESSES_TABLE_NAME + " LIMIT $1;";
    executeQueryAndHandleResult(limit, promise, sqlQuery);
    return promise.future();
  }
  @Override
  public Future<JsonObject> getProcess(String processId) {
    Promise<JsonObject> promise = Promise.promise();
    String sqlQuery =
      "SELECT version, id, title, description, mode AS \"jobControlOptions\", keywords, response AS \"outputTransmission\"," +
        " input as inputs, output as outputs FROM " + PROCESSES_TABLE_NAME + " WHERE id=$1::UUID;";
    executeQueryAndHandleResult(UUID.fromString(processId), promise, sqlQuery);
    return promise.future();
  }

  private void executeQueryAndHandleResult(Object parameter, Promise<JsonObject> promise,
                                           String sqlQuery) {
    client.withConnection(
      conn -> conn.preparedQuery(sqlQuery).execute(Tuple.of(parameter)).onSuccess(rowSet -> {
        if (rowSet.size() == 0) {
          promise.fail(processException404);
        } else {
          JsonObject result = handleRowSet(rowSet);
          if (parameter instanceof Integer) {
            promise.complete(result);
          } else {
            result.remove("links");
            promise.complete(result);
          }
        }
      }).onFailure(fail -> handleFailure(fail, promise)));
  }

  private JsonObject handleRowSet(RowSet<Row> rowSet) {
    List<JsonObject> jsonObjects = new ArrayList<>();
    String baseUrl = config.getString("hostName");

    for (Row row : rowSet) {
      JsonObject tempProcessObj = row.toJson();
      JsonArray tempLinkArray = createLinkArray(baseUrl, row);
      tempProcessObj.put("links", tempLinkArray);
      jsonObjects.add(tempProcessObj);
    }

    JsonObject result = new JsonObject().put("processes", jsonObjects);
    JsonArray linkArray = createLinkArray(baseUrl, null);
    result.put("links", linkArray);
    return result;
  }

  private JsonArray createLinkArray(String baseUrl, Row row) {
    JsonArray linkArray = new JsonArray();
    JsonObject linkObject = new JsonObject().put("type", "application/json").put("rel", "self");

    if (row != null) {
      linkObject.put("title", "Process description as JSON");
      linkObject.put("href",
        baseUrl.concat("/processes/").concat(String.valueOf(row.getUUID("id"))));
    } else {
      linkObject.put("href", baseUrl.concat("/processes"));
    }

    linkArray.add(linkObject.copy());
    return linkArray;
  }

  private void handleFailure(Throwable fail, Promise<JsonObject> promise) {
    LOGGER.error("Failed to get processes- {}", fail.getMessage());
    promise.fail(processException500);
  }

    /**
     * Retrieves the total number of API hits made by a specific user to a given API path and collection
     * since the specified policyIssuedAt timestamp.
     *
     * @param userId         The UUID of the user as a string.
     * @param apiPath        The path of the API being accessed.
     * @param collectionId   The UUID of the collection as a string.
     * @param policyIssuedAt The epoch time (in seconds) representing when the policy started.
     * @return A {@link Future} containing the count of API hits.
     */
    @Override
    public Future<Long> getTotalApiHits(String userId, String apiPath, String collectionId, long policyIssuedAt) {
        Promise<Long> result = Promise.promise();

        // Query to count API hits for the given user, api_path, and collection_id within the time window
        String query = "SELECT COUNT(*) FROM metering WHERE user_id = $1 AND api_path = $2 AND collection_id = $3 " +
                "AND timestamp > to_timestamp($4) AND resp_size > 0";

        client.preparedQuery(query)
                .execute(Tuple.of(userId, apiPath, collectionId, policyIssuedAt))
                .onSuccess(success -> {
                    Row row = success.iterator().next();
                    Long apiHits = row.getLong(0);
                    result.complete(apiHits != null ? apiHits : 0L);
                })
                .onFailure(fail -> {
                    LOGGER.error("Failed to check API hits: {} - {}", fail.getMessage(), fail);
                    result.fail(new OgcException(500, "Internal Server Error", "Failed to check API hits: " + fail.getMessage()));
                });

        return result.future();
    }

    /**
     * Retrieves the total data usage (sum of response sizes in bytes) made by a specific user to a given
     * API path and collection since the specified policyIssuedAt timestamp.
     *
     * @param userId         The UUID of the user as a string.
     * @param apiPath        The path of the API being accessed.
     * @param collectionId   The UUID of the collection as a string.
     * @param policyIssuedAt The epoch time (in seconds) representing when the policy started.
     * @return A {@link Future} containing the total data usage in bytes.
     */
    @Override
    public Future<Long> getTotalDataUsage(String userId, String apiPath, String collectionId, long policyIssuedAt) {
        Promise<Long> result = Promise.promise();

        // Query to sum the data usage (resp_size) for the given user, api_path, and collection_id within the time window
        String query = "SELECT SUM(resp_size) FROM metering WHERE user_id = $1 AND api_path = $2 AND collection_id = $3 " +
                "AND timestamp > to_timestamp($4)";

        client.preparedQuery(query)
                .execute(Tuple.of(userId, apiPath, collectionId, policyIssuedAt))
                .onSuccess(success -> {
                    Row row = success.iterator().next();
                    Long dataUsage = row.getLong(0);
                    result.complete(dataUsage != null ? dataUsage : 0L);
                })
                .onFailure(fail -> {
                    LOGGER.error("Failed to check data usage: {} - {}", fail.getMessage(), fail);
                    result.fail(new OgcException(500, "Internal Server Error", "Failed to check data usage: " + fail.getMessage()));
                });

        return result.future();
    }

  @Override
  public Future<List<JsonObject>> getOgcFeatureCollectionMetadataForOasSpec(
      List<String> existingCollectionUuidIds) {

    Promise<List<JsonObject>> result = Promise.promise();

    UUID[] existingCollectionIdsArr =
        existingCollectionUuidIds.stream().map(i -> UUID.fromString(i)).toArray(UUID[]::new);

    Collector<Row, ?, List<JsonObject>> collector =
        Collectors.mapping(Row::toJson, Collectors.toList());

    Collector<Row, ?, Map<String, JsonObject>> collectionIdToJsonCollector =
        Collectors.toMap(row -> row.getString("collection_id"), obj -> obj.toJson());

    final String GET_COLLECTION_INFO =
        "WITH crs_info AS (SELECT collection_id, array_agg(crs) as supported_crs"
            + " FROM collection_supported_crs"
            + " JOIN crs_to_srid ON collection_supported_crs.crs_id = crs_to_srid.id"
            + " WHERE collection_id != ALL($1::UUID[]) GROUP BY collection_id)"
            + ", geom_info AS (SELECT type AS geometry_type, f_table_name::text AS collection_id"
            + " FROM geometry_columns WHERE f_table_name != ALL($2::text[]))"
            + " SELECT collections_details.id, title, description, datetime_key, crs, bbox, temporal, supported_crs"
            + ", geometry_type FROM collections_details"
            + " JOIN crs_info ON crs_info.collection_id = collections_details.id"
            + " JOIN geom_info ON geom_info.collection_id::text = collections_details.id::text"
            + " JOIN collection_type ON collection_type.collection_id = collections_details.id"
            + " WHERE collections_details.id != ALL($1::UUID[]) AND collection_type.type = 'FEATURE'";

    final String GET_COLLECTION_ATTRIBUTE_INFO =
        "SELECT table_name AS collection_id, json_object_agg(column_name, data_type) AS attributes"
            + " FROM information_schema.columns WHERE table_name = ANY($1::text[])"
            + " AND column_name != ALL('{\"id\",\"geom\"}') GROUP BY table_name";

    Future<List<JsonObject>> newCollectionsJson =
        client.withConnection(conn -> conn.preparedQuery(GET_COLLECTION_INFO).collecting(collector)
            .execute(Tuple.of(existingCollectionIdsArr)
                .addArrayOfString(existingCollectionUuidIds.toArray(String[]::new)))
            .map(res -> res.value()));

    Future<Map<String, JsonObject>> collectionToAttribFut = newCollectionsJson.compose(list -> {
      if (list.isEmpty()) {
        return Future.succeededFuture(Map.of());
      }

      List<String> newCollectionIds =
          list.stream().map(obj -> obj.getString("id")).collect(Collectors.toList());

      return client.withConnection(conn -> conn.preparedQuery(GET_COLLECTION_ATTRIBUTE_INFO)
          .collecting(collectionIdToJsonCollector).execute(Tuple.of(newCollectionIds.toArray()))
          .map(res -> res.value()));
    });

    Future<List<JsonObject>> collectionInfoMerged = collectionToAttribFut.compose(res -> {
      if (newCollectionsJson.result().isEmpty()) {
        return Future.succeededFuture(List.of());
      }

      Map<String, JsonObject> collectionToAttribMap = collectionToAttribFut.result();

      // if a newly found collection ID is absent in collectionToAttribMap, means that it does not have
      // any attributes. So using getOrDefault to handle those collection IDs by returning a JSON object
      // with empty 'attributes' JSON object.

      JsonObject defaultValIfCollectionHasNoAttribs = new JsonObject().put("attributes", new JsonObject());

      List<JsonObject> newListWithAllInfo = newCollectionsJson.result().stream().map(obj -> {
        String collectionId = obj.getString("id");

        obj.put("attributes",
            collectionToAttribMap.getOrDefault(collectionId, defaultValIfCollectionHasNoAttribs)
                .getJsonObject("attributes"));

        return obj;
      }).collect(Collectors.toList());

      return Future.succeededFuture(newListWithAllInfo);
    });

    collectionInfoMerged.onSuccess(succ -> result.complete(succ)).onFailure(fail -> {
      LOGGER.error("Something went wrong when querying DB for new OGC feature collections {}",
          fail.getMessage());
      result.fail(fail);
    });

    return result.future();
  }

    @Override
    public Future<List<JsonObject>> getOgcRecordMetadataForOasSpec(List<String> existingCollectionUuidIds) {
        Promise<List<JsonObject>> result = Promise.promise();

        UUID[] existingCollectionIdsArr =
                existingCollectionUuidIds.stream().map(i-> UUID.fromString(i)).toArray(UUID[]::new);

        Collector<Row, ?, List<JsonObject>> collector =
                Collectors.mapping(Row::toJson, Collectors.toList());

        final String GET_RECORD_CATALOG_INFO =
                "SELECT collections_details.id, title, description FROM collections_details JOIN "
                        + "collection_type ON collections_details.id = collection_type.collection_id WHERE "
                        + "collections_details.id != ALL($1::UUID[]) AND collection_type.type = 'COLLECTION'";

        Future<List<JsonObject>> newCollectionJson =
                client.withConnection(
                        conn->
                                conn.preparedQuery(GET_RECORD_CATALOG_INFO)
                                        .collecting(collector)
                                        .execute(Tuple.of(existingCollectionIdsArr))
                                        .map(res-> res.value()));

        newCollectionJson
                .onSuccess(succ -> result.complete(succ))
                .onFailure(
                        fail-> {
                            LOGGER.error(
                                    "Something went wrong when querying DB for new OGC collections {}",
                                    fail.getMessage());
                            result.fail(fail);
                        });

        return  result.future();
    }


    /**
   * This method queries the database to determine if a resource identified by the given
   * ID is accessible as "open" or "secure". The access status is retrieved from the
   * "ri_details" table where the ID matches the provided UUID.
   * If the ID is not found in the database, the method fails with a
   * {@link OgcException} indicating a 404 Not Found error.
   *
   * @param id which is a UUID
   * @return a boolean result. If "secure" return false, if "open" returns true
   */
  @Override
  public Future<Boolean> getAccess(String id) {
        Promise<Boolean> promise = Promise.promise();
        String sqlString = "select access from ri_details where id = $1::uuid";
        Collector<Row, ? , List<JsonObject>> collector = Collectors.mapping(Row::toJson, Collectors.toList());

        client.withConnection(conn ->
                        conn.preparedQuery(sqlString)
                                .collecting(collector)
                                .execute(Tuple.of(UUID.fromString(id)))
                                .map(SqlResult::value))
                .onSuccess(success -> {
                    if (success.isEmpty()){
                        promise.fail(new OgcException(404,"Not found", "Collection not found"));
                    }
                    else {
                        String access = success.get(0).getString("access");
                        if (access.equalsIgnoreCase("secure")) {
                            promise.complete(false);
                        }
                        else if (access.equalsIgnoreCase("open")){
                            promise.complete(true);
                        }
                    }
                })
                .onFailure(fail -> {
                    LOGGER.error("Something went wrong at isOpenResource: {}", fail.getMessage() );
                    promise.fail(new OgcException(500, "Internal Server Error","Internal Server Error"));
                });
        return promise.future();
    }


  @Override
  public Future<JsonObject> getCoverageDetails(String id) {
    Promise<JsonObject> promise = Promise.promise();
    String sqlString =
        "select schema, href, s3_bucket_id from collection_coverage where collection_id = $1::uuid";
    Collector<Row, ?, List<JsonObject>> collector =
        Collectors.mapping(Row::toJson, Collectors.toList());

    client
        .withConnection(
            conn ->
                conn.preparedQuery(sqlString)
                    .collecting(collector)
                    .execute(Tuple.of(UUID.fromString(id)))
                    .map(SqlResult::value))
        .onSuccess(
            success -> {
              if (success.isEmpty()) {
                promise.fail(new OgcException(404, "Not found", "Collection not found"));
              } else {
                LOGGER.debug("response from database: " + success.get(0));
                JsonObject schema = success.get(0);
                promise.complete(schema);
              }
            })
        .onFailure(
            fail -> {
              LOGGER.error("Something went wrong at isOpenResource: {}", fail.getMessage());
              promise.fail(new OgcException(500, "Internal Server Error", "Internal Server Error"));
            });
    return promise.future();
  }

  @Override
  public Future<List<JsonObject>> getCollectionMetadataForOasSpec(
      List<String> existingCollectionUuidIds) {

    Promise<List<JsonObject>> result = Promise.promise();

    UUID[] existingCollectionIdsArr =
        existingCollectionUuidIds.stream().map(i -> UUID.fromString(i)).toArray(UUID[]::new);

    Collector<Row, ?, List<JsonObject>> collector =
        Collectors.mapping(Row::toJson, Collectors.toList());

    final String GET_COLLECTION_INFO =
        "SELECT collections_details.id, title, description FROM collections_details "
      + "JOIN collection_type ON collections_details.id = collection_type.collection_id "
      + "WHERE collections_details.id != ALL($1::UUID[]) AND collection_type.type NOT IN ('STAC', 'COLLECTION')";

    Future<List<JsonObject>> newCollectionsJson =
        client.withConnection(
            conn ->
                conn.preparedQuery(GET_COLLECTION_INFO)
                    .collecting(collector)
                    .execute(Tuple.of(existingCollectionIdsArr))
                    .map(res -> res.value()));

    newCollectionsJson
        .onSuccess(succ -> result.complete(succ))
        .onFailure(
            fail -> {
              LOGGER.error(
                  "Something went wrong when querying DB for new OGC collections {}",
                  fail.getMessage());
              result.fail(fail);
            });

    return result.future();
  }

    @Override
    public Future<List<JsonObject>> getStacCollectionMetadataForOasSpec(
            List<String> existingCollectionUuidIds) {

        Promise<List<JsonObject>> result = Promise.promise();

        UUID[] existingCollectionIdsArr =
                existingCollectionUuidIds.stream().map(i -> UUID.fromString(i)).toArray(UUID[]::new);

        Collector<Row, ?, List<JsonObject>> collector =
                Collectors.mapping(Row::toJson, Collectors.toList());

        final String GET_STAC_COLLECTION_INFO =
                "SELECT collections_details.id, title, description FROM collections_details JOIN "
              + "collection_type ON collections_details.id = collection_type.collection_id WHERE "
              + "collections_details.id != ALL($1::UUID[]) AND collection_type.type = 'STAC'";

        Future<List<JsonObject>> newCollectionsJson =
                client.withConnection(
                        conn ->
                                conn.preparedQuery(GET_STAC_COLLECTION_INFO)
                                        .collecting(collector)
                                        .execute(Tuple.of(existingCollectionIdsArr))
                                        .map(res -> res.value()));

        newCollectionsJson
                .onSuccess(succ -> result.complete(succ))
                .onFailure(
                        fail -> {
                            LOGGER.error(
                                    "Something went wrong when querying DB for new OGC collections {}",
                                    fail.getMessage());
                            result.fail(fail);
                        });

        return result.future();
    }


}
