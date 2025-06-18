package ogc.rs.apiserver.util;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ogc.rs.apiserver.util.Constants.*;

/**
 * Represents API usage, data usage, spatial (bbox), and feature limits extracted from a user's token.
 */
@DataObject
public class Limits {
    @GenIgnore
    private static final Logger LOGGER = LogManager.getLogger(Limits.class);

    private Long policyIssuedAt;
    private Long dataUsageLimitInBytes;
    private Long apiHitsLimit;
    private JsonArray bboxLimit;
    private JsonObject featLimit;

    // Default constructor required for @DataObject
    public Limits() {
    }

    // Constructor from JsonObject (required for @DataObject)
    public Limits(JsonObject json) {
        this.policyIssuedAt = json.getLong("policyIssuedAt");
        this.dataUsageLimitInBytes = json.getLong("dataUsageLimitInBytes");
        this.apiHitsLimit = json.getLong("apiHitsLimit");
        this.bboxLimit = json.getJsonArray("bboxLimit");
        this.featLimit = json.getJsonObject("featLimit");
    }

    // Convert to JsonObject (required for @DataObject)
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        if (policyIssuedAt != null) json.put("policyIssuedAt", policyIssuedAt);
        if (dataUsageLimitInBytes != null) json.put("dataUsageLimitInBytes", dataUsageLimitInBytes);
        if (apiHitsLimit != null) json.put("apiHitsLimit", apiHitsLimit);
        if (bboxLimit != null) json.put("bboxLimit", bboxLimit);
        if (featLimit != null) json.put("featLimit", featLimit);
        return json;
    }

    private Limits(Long policyIssuedAt, Long dataUsageLimitInBytes, Long apiHitsLimit,
                   JsonArray bboxLimit, JsonObject featLimit) {
        this.policyIssuedAt = policyIssuedAt;
        this.dataUsageLimitInBytes = dataUsageLimitInBytes;
        this.apiHitsLimit = apiHitsLimit;
        this.bboxLimit = bboxLimit;
        this.featLimit = featLimit;
    }

    /**
     * Parses the JsonObject representing limits and constructs a Limits instance.
     *
     * @param limitsJson JsonObject with fields like "iat", "dataUsage", "apiHits", "bbox", "feat"
     * @return Limits object, or null if limitsJson is null
     */
    @GenIgnore
    public static Limits fromJson(JsonObject limitsJson) {
        if (limitsJson == null) {
            return null;
        }

        Long iat = limitsJson.getLong("iat", 0L);

        Long dataUsageBytes = null;
        if (limitsJson.containsKey("dataUsage")) {
            String dataUsageString = limitsJson.getString("dataUsage");
            dataUsageBytes = parseDataUsageToBytes(dataUsageString);
        }

        Long apiHits = null;
        if (limitsJson.containsKey("apiHits")) {
            apiHits = limitsJson.getLong("apiHits");
        }

        JsonArray bbox = null;
        if (limitsJson.containsKey("bbox")) {
            bbox = parseBboxLimitToJsonArray(limitsJson.getJsonArray("bbox"));
        }

        JsonObject feat = null;
        if (limitsJson.containsKey("feat")) {
            feat = parseFeatLimitToJsonObject(limitsJson.getJsonObject("feat"));
        }

        return new Limits(iat, dataUsageBytes, apiHits, bbox, feat);
    }

    /**
     * Converts a human-readable data limit string (e.g., "100:mb") into bytes.
     *
     * @param limit A string in the format "<value>:<unit>", e.g., "500:mb"
     * @return Limit in bytes
     * @throws OgcException if the unit is invalid or unsupported
     */
    @GenIgnore
    private static Long parseDataUsageToBytes(String limit) {
        if (limit == null || limit.isEmpty()) {
            return null;
        }

        String[] parts = limit.split(":");
        if (parts.length != 2) {
            throw new OgcException(400, "Bad Request", "Invalid data usage format: " + limit);
        }

        long value = Long.parseLong(parts[0]);
        String unit = parts[1].toLowerCase();

        switch (unit) {
            case "kb":
                return value * 1024L;
            case "mb":
                return value * 1024L * 1024L;
            case "gb":
                return value * 1024L * 1024L * 1024L;
            case "tb":
                return value * 1024L * 1024L * 1024L * 1024L;
            default:
                throw new OgcException(400, "Bad Request", "Invalid data usage unit: " + unit);
        }
    }

    /**
     * Parses and validates bbox limit, returning as JsonArray for DataObject compatibility.
     */
    @GenIgnore
    private static JsonArray parseBboxLimitToJsonArray(JsonArray bboxArray) {
        if (bboxArray == null || bboxArray.isEmpty()) {
            return null;
        }

        if (bboxArray.size() != 4) {
            throw new OgcException(400, "Bad Request", INVALID_BBOX_FORMAT);
        }

        JsonArray validatedBbox = new JsonArray();
        for (int i = 0; i < bboxArray.size(); i++) {
            Object val = bboxArray.getValue(i);
            if (!(val instanceof Number)) {
                throw new OgcException(400, "Bad Request", ERR_BBOX_NON_NUMERIC);
            }

            double value = ((Number) val).doubleValue();
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                throw new OgcException(400, "Bad Request", ERR_BBOX_NON_FINITE);
            }

            validatedBbox.add(value);
        }

        double minLon = validatedBbox.getDouble(0);
        double minLat = validatedBbox.getDouble(1);
        double maxLon = validatedBbox.getDouble(2);
        double maxLat = validatedBbox.getDouble(3);

        LOGGER.debug("minLon, minLat, maxLon, maxLat in token bbox: {} {} {} {}", minLon, minLat, maxLon, maxLat);

        if (minLon < -180 || minLon > 180 || maxLon < -180 || maxLon > 180) {
            throw new OgcException(400, "Bad Request", ERR_BBOX_LONGITUDE_RANGE);
        }

        if (minLat < -90 || minLat > 90 || maxLat < -90 || maxLat > 90) {
            throw new OgcException(400, "Bad Request", ERR_BBOX_LATITUDE_RANGE);
        }

        if (minLon >= maxLon || minLat >= maxLat) {
            throw new OgcException(400, "Bad Request", ERR_BBOX_MIN_MAX_ORDER);
        }

        return validatedBbox;
    }

    /**
     * Parses and validates feature limits, returning as JsonObject for DataObject compatibility.
     */
    @GenIgnore
    private static JsonObject parseFeatLimitToJsonObject(JsonObject featObject) {
        if (featObject == null || featObject.isEmpty()) {
            return null;
        }

        JsonObject validatedFeat = new JsonObject();

        for (String collectionId : featObject.fieldNames()) {
            JsonArray featureIds = featObject.getJsonArray(collectionId);
            if (featureIds == null) {
                throw new OgcException(400, "Bad Request", "Invalid feature limit format for collection: " + collectionId);
            }

            JsonArray validatedFeatureIds = new JsonArray();
            for (int i = 0; i < featureIds.size(); i++) {
                Object featureId = featureIds.getValue(i);
                if (featureId != null) {
                    validatedFeatureIds.add(featureId.toString());
                }
            }

            if (validatedFeatureIds.size() > 10) {
                throw new OgcException(400, "Bad Request", "Maximum 10 feature IDs allowed per collection");
            }

            validatedFeat.put(collectionId, validatedFeatureIds);
        }

        return validatedFeat;
    }

    // Convenience methods to get typed data
    @GenIgnore
    public List<Double> getBboxLimitAsList() {
        if (bboxLimit == null) return null;
        List<Double> result = new ArrayList<>();
        for (int i = 0; i < bboxLimit.size(); i++) {
            result.add(bboxLimit.getDouble(i));
        }
        return result;
    }

    @GenIgnore
    public Map<String, List<String>> getFeatLimitAsMap() {
        if (featLimit == null) return null;
        Map<String, List<String>> result = new HashMap<>();
        for (String collectionId : featLimit.fieldNames()) {
            JsonArray featureIds = featLimit.getJsonArray(collectionId);
            List<String> featureIdList = new ArrayList<>();
            for (int i = 0; i < featureIds.size(); i++) {
                featureIdList.add(featureIds.getString(i));
            }
            result.put(collectionId, featureIdList);
        }
        return result;
    }

    // Getters
    public Long getPolicyIssuedAt() {
        return policyIssuedAt;
    }

    public Long getDataUsageLimitInBytes() {
        return dataUsageLimitInBytes;
    }

    public Long getApiHitsLimit() {
        return apiHitsLimit;
    }

    public JsonArray getBboxLimit() {
        return bboxLimit;
    }

    public JsonObject getFeatLimit() {
        return featLimit;
    }
}