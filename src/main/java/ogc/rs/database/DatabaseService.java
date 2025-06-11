package ogc.rs.database;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ogc.rs.apiserver.util.StacItemSearchParams;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@VertxGen
@ProxyGen
public interface DatabaseService {
    @GenIgnore
    static DatabaseService createProxy(Vertx vertx, String address) {
        return new DatabaseServiceVertxEBProxy(vertx, address);
    }
    // TODO:
    // Future<JsonObject> to Future<DO> class.

    Future<List<JsonObject>>  getCollections();

    Future<List<JsonObject>>  getCollection(final String collectionId);

    Future<JsonObject> getFeatures(String collectionId, Map<String, String> queryParams, Map<String, Integer> crs);

    Future<JsonObject> getFeature(String collectionId, Integer featureId, Map<String, String> queryParams, Map<String,
        Integer> crs);

    Future<Map<String, Integer>> isCrsValid(String collectionId, Map<String, String> queryParams);

    Future<List<JsonObject>>  getStacCollections();

    Future<JsonObject> getStacCollection(String collectionId);

    Future<List<JsonObject>> getTileMatrixSets();

    Future<List<JsonObject>> getStacItems(String collectionId, int limit, int offset);

    Future<JsonObject> getStacItemById(String collectionId, String stacItemId);

    Future<List<JsonObject>> getTileMatrixSetMetaData(String tileMatrixSet);

    Future<List<JsonObject>> getTileMatrixSetRelation(String collectionId);

    Future<List<JsonObject>> getTileMatrixSetRelationOverload(String collectionId, String tileMatrixSetId);

    Future<JsonObject> getAssets(String assetId);

    Future<JsonObject> getProcesses(int limit);
    Future<JsonObject> getProcess(String processId);

    // Get API hits count based on user_id, api_path, and collection_id within the time window
    Future<Long> getTotalApiHits(String userId, String apiPath, String collectionId, long policyIssuedAt);

    // Get data usage (sum of resp_size) based on user_id, api_path, and collection_id within the time window
    Future<Long> getTotalDataUsage(String userId, String apiPath, String collectionId, long policyIssuedAt);

    /**
     * Checks if all specified feature IDs exist in the given collection table.
     *
     * @param collectionId The collection (table) ID to check
     * @param featureIds List of feature IDs to validate
     * @return Future<Boolean> true if all features exist, false otherwise
     */
    Future<Boolean> checkFeatureExists(String collectionId, List<String> featureIds);
    /**
     * Get OGC Feature Collection metadata to be used for OpenAPI spec generation.
     *
     * @param existingCollectionUuidIds UUID IDs of collections that are already part of the spec.
     * @return list of {@link JsonObject}, which is cast to the required type by the caller.
     */
    Future<List<JsonObject>> getOgcFeatureCollectionMetadataForOasSpec(List<String> existingCollectionUuidIds);

    /**
     * GET OGC REcords metadata  to be used for OPENAPI spec generation.
     *
     * @param existingCollectionUuidIds UUID IDs of records catalog that are already part of the spec.
     * @return list of {@link JsonObject}, which is cast to the required type by the caller.
     */
    Future<List<JsonObject>> getOgcRecordMetadataForOasSpec(List<String> existingCollectionUuidIds);

    Future<Boolean> getAccess(String id);

    /**
     * Retrieves all the details of coverage for the collection whose id is provided.
     *
     * @param id The ID of the collection for which to retrieve the schema. Must be a valid UUID string.
     * @return The future will return with a JsonObject containing the coverageJson data if successful,
     * or it will fail with an OgcException if the collection is not found or if there is an internal server error.
     */
    Future<JsonObject> getCoverageDetails(String id);

    /**
     * Get all collections metadata to be used for OpenAPI spec generation.
     *
     * @param existingCollectionUuidIds UUID IDs of collections that are already part of the spec.
     * @return list of {@link JsonObject}, which is cast to the required type by the caller.
     */
    Future<List<JsonObject>> getCollectionMetadataForOasSpec(List<String> existingCollectionUuidIds);

    /**
     * Get all STAC collections metadata to be used for OpenAPI spec generation.
     *
     * @param existingCollectionUuidIds UUID IDs of collections that are already part of the spec.
     * @return list of {@link JsonObject}, which is cast to the required type by the caller.
     */
    Future<List<JsonObject>> getStacCollectionMetadataForOasSpec(List<String> existingCollectionUuidIds);


    /**
     * Run STAC Item Search query given query params in {@link StacItemSearchParams} object.
     *
     * @param params contains all the query params for STAC Item Search
     * @return JSON response data
     */
    Future<JsonObject> stacItemSearch(StacItemSearchParams params);


    /**
     * Create STAC collection by inserting the items in following order.
     * 1)insert into collection_details table
     * 2)insert into collections_type
     * 3)insert into roles table
     * 4)insert into ri_details table
     * 5)table cretaed by the id
     * 6)table partition created in stac_collections_part and newly created table attached
     * 7)Privileges granted to newly created table
     *
     * @param jsonObject is the request body
     * @return JSON response data
     */
    Future<JsonObject> postStacCollection(JsonObject jsonObject);

  /**
   * Onboards multiple STAC collection by inserting the items in following order.
   * 1)insert into collection_details table
   * 2)insert into collections_type
   * 3)insert into roles table
   * 4)insert into ri_details table
   * 5)table cretaed by the id
   * 6)table partition created in stac_collections_part and newly created table attached
   * 7)Privileges granted to newly created table
   *
   * If any of the collection onboarding fails then all the previously onboarded collections are
   * rolled back and the id which caused it is returned along with the error message.
   *
   * @param jsonArray is the request body
   * @return json Object with multiple collections inserted
   */
  Future<JsonObject> postStacCollections(JsonArray jsonArray);

    /**
     * Udation of the object by id. Fields present in the request body are updated.
     *
     * @param jsonObject contains the fields that need to be updated
     * @return JSON response data
     */
    Future<JsonObject> updateStacCollection(JsonObject jsonObject);

    Future<JsonObject> insertStacItems(JsonObject requestBody);

    Future<JsonObject> insertStacItem(JsonObject requestBody);

    Future<JsonObject> getAccessDetails(String collectionId);

   Future<JsonObject> updateStacItem(JsonObject requestBody);

    Future<List<JsonObject>>  getOgcRecords(String catalogId);

    Future<JsonObject> getOgcRecordItem (String catalogId, String recordId);

    /**
     * Get S3 bucket ID for tiles for a particular collection ID and TMS. The ID can be used to find
     * the S3 config for the particular bucket.
     * 
     * @param collectionId the collection ID
     * @param tileMatrixSetId the tile matrix set ID
     * @return String the S3 bucket ID
     */
    Future<String> getTileS3BucketId(String collectionId, String tileMatrixSetId);
  }

