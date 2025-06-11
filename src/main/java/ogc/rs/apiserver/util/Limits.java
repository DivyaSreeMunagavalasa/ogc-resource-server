package ogc.rs.apiserver.util;

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
public class Limits {
    private static final Logger LOGGER = LogManager.getLogger(Limits.class);
    private final Long policyIssuedAt;
    private final Long dataUsageLimitInBytes;
    private final Long apiHitsLimit;
    private final List<Double> bboxLimit;
    private final Map<String, List<String>> featLimit;

    private Limits(Long policyIssuedAt, Long dataUsageLimitInBytes, Long apiHitsLimit,
                   List<Double> bboxLimit, Map<String, List<String>> featLimit) {
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

        List<Double> bbox = null;
        if (limitsJson.containsKey("bbox")) {
            bbox = parseBboxLimit(limitsJson.getJsonArray("bbox"));
        }

        Map<String, List<String>> feat = null;
        if (limitsJson.containsKey("feat")) {
            feat = parseFeatLimit(limitsJson.getJsonObject("feat"));
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
     * Parses a JSON array representing a 2D bounding box into a List<Double>.
     * The expected format is:
     *   bbox = [minLon, minLat, maxLon, maxLat] or: [west, south, east, north]
     * where:
     *   - minLon (west) ∈ [-180, 180]
     *   - maxLon (east) ∈ [-180, 180]
     *   - minLat (south) ∈ [-90, 90]
     *   - maxLat (north) ∈ [-90, 90]
     *   - minLon < maxLon i,e west < east
     *   - minLat < maxLat i,e south < north
     *
     * @param bboxArray JsonArray containing exactly 4 numeric values
     * @return List<Double> representation of the bounding box
     * @throws OgcException if the format, type, range, or ordering is invalid
     */
    private static List<Double> parseBboxLimit(JsonArray bboxArray) {
        if (bboxArray == null || bboxArray.isEmpty()) {
            return null;
        }

        if (bboxArray.size() != 4) {
            throw new OgcException(400, "Bad Request", INVALID_BBOX_FORMAT);
        }

        List<Double> bbox = new ArrayList<>();
        for (int i = 0; i < bboxArray.size(); i++) {
            Object val = bboxArray.getValue(i);
            if (!(val instanceof Number)) {
                throw new OgcException(400, "Bad Request", ERR_BBOX_NON_NUMERIC);
            }

            double value = ((Number) val).doubleValue();
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                throw new OgcException(400, "Bad Request", ERR_BBOX_NON_FINITE);
            }

            bbox.add(value);
        }

        double minLon = bbox.get(0);
        double minLat = bbox.get(1);
        double maxLon = bbox.get(2);
        double maxLat = bbox.get(3);

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

        return bbox;
    }

    /**
     * Parses feature limits from the token.
     * Expected format: {"collectionId": ["featureId1", "featureId2", ...]}
     *
     * @param featObject JsonObject containing collection to feature ID mappings
     * @return Map of collection IDs to lists of allowed feature IDs
     * @throws OgcException if the format is invalid
     */
    private static Map<String, List<String>> parseFeatLimit(JsonObject featObject) {
        if (featObject == null || featObject.isEmpty()) {
            return null;
        }

        Map<String, List<String>> featMap = new HashMap<>();

        for (String collectionId : featObject.fieldNames()) {
            JsonArray featureIds = featObject.getJsonArray(collectionId);
            if (featureIds == null) {
                throw new OgcException(400, "Bad Request", "Invalid feature limit format for collection: " + collectionId);
            }

            List<String> featureIdList = new ArrayList<>();
            for (int i = 0; i < featureIds.size(); i++) {
                Object featureId = featureIds.getValue(i);
                if (featureId != null) {
                    featureIdList.add(featureId.toString());
                }
            }

            if (featureIdList.size() > 10) {
                throw new OgcException(400, "Bad Request", "Maximum 10 feature IDs allowed per collection");
            }

            featMap.put(collectionId, featureIdList);
        }

        return featMap;
    }

    public Long getPolicyIssuedAt() {
        return policyIssuedAt;
    }

    public Long getDataUsageLimitInBytes() {
        return dataUsageLimitInBytes;
    }

    public Long getApiHitsLimit() {
        return apiHitsLimit;
    }

    public List<Double> getBboxLimit() {
        return bboxLimit;
    }

    public Map<String, List<String>> getFeatLimit() {
        return featLimit;
    }
}