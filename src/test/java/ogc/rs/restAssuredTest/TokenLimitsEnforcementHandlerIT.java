package ogc.rs.restAssuredTest;

import io.restassured.response.Response;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import jdk.jfr.Description;
import ogc.rs.util.FakeTokenBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import java.util.UUID;
import java.time.Instant;
import static io.restassured.RestAssured.given;
import static ogc.rs.apiserver.util.Constants.*;
import static ogc.rs.common.Constants.CODE_KEY;
import static ogc.rs.common.Constants.DESCRIPTION_KEY;
import static org.hamcrest.Matchers.*;

@ExtendWith(RestAssuredConfigExtension.class)
public class TokenLimitsEnforcementHandlerIT {

    private static final Logger LOGGER = LogManager.getLogger(TokenLimitsEnforcementHandlerIT.class);
    String privateVectorDataEndpoint = "/collections/{collectionId}/items";
    String collectionId = "a5a6e26f-d252-446d-b7dd-4d50ea945102";
    Instant sixMonthsAgo = Instant.now().minusSeconds(6 * 30L * 24L * 60L * 60L); // 6 months ago

    private Response sendRequest(String collectionId, String token) {
        return given().pathParam("collectionId", collectionId)
                .auth().oauth2(token)
                .contentType("application/json")
                .when().get(privateVectorDataEndpoint);
    }

    private Response sendBBoxRequest(String collectionId, String token, String bbox) {
        return given().pathParam("collectionId", collectionId)
                .queryParam("bbox", bbox)
                .auth().oauth2(token)
                .contentType("application/json")
                .when().get(privateVectorDataEndpoint);
    }

    @Test
    @Description("Test if API hit limit is properly enforced for a token")
    public void testApiHitLimitEnforced() {
        LOGGER.info("Testing if API hit limit is enforced for a token with a valid limit.");
        String token = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceAndRg(UUID.fromString(collectionId), UUID.randomUUID())
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("apiHits", 5000)
                        .put("iat", sixMonthsAgo.getEpochSecond())))
                .build();
        Response response = sendRequest(collectionId, token);
        response.then().statusCode(200);
    }

    @Test
    @Description("Test if data usage limit is properly enforced for a token")
    public void testDataUsageLimitEnforced() {
        LOGGER.info("Testing if data usage limit is enforced for a token with a valid limit.");
        String token = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceAndRg(UUID.fromString(collectionId), UUID.randomUUID())
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("dataUsage", "100:gb")
                        .put("iat", sixMonthsAgo.getEpochSecond())))
                .build();
        Response response = sendRequest(collectionId, token);
        response.then().statusCode(200);
    }

    @Test
    @Description("Test if API hit limit is properly enforced for a provider delegate token")
    public void testApiHitLimitEnforcedTokenWithDelegateRole() {
        LOGGER.info("Testing if API hit limit is enforced for a provider delegate token with a valid limit.");
        String token = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceAndRg(UUID.fromString(collectionId), UUID.randomUUID())
                .withDelegate(UUID.fromString("9304cb99-7125-47f1-8686-a070bb6c3eaf"), "provider")
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("apiHits", 5000)
                        .put("iat", sixMonthsAgo.getEpochSecond())))
                .build();
        Response response = sendRequest(collectionId, token);
        response.then().statusCode(200);
    }

    @Test
    @Description("Test if data usage limit is properly enforced for a provider delegate token")
    public void testDataUsageLimitEnforcedWithDelegateRole() {
        LOGGER.info("Testing if data usage limit is enforced for a provider delegate token with a valid limit.");
        String token = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceAndRg(UUID.fromString(collectionId), UUID.randomUUID())
                .withDelegate(UUID.fromString("9304cb99-7125-47f1-8686-a070bb6c3eaf"), "provider")
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("dataUsage", "100:gb")
                        .put("iat", sixMonthsAgo.getEpochSecond())))
                .build();
        Response response = sendRequest(collectionId, token);
        response.then().statusCode(200);
    }

    @Test
    @Description("Test the flow when no limits are set for a token")
    public void testNoUsageLimitEnforced() {
        LOGGER.info("Testing the flow if no usage limit is enforced for a token");
        String token = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceAndRg(UUID.fromString(collectionId), UUID.randomUUID())
                .withRoleProvider()
                .withCons(new JsonObject()) // No usage limit
                .build();
        Response response = sendRequest(collectionId, token);
        response.then().statusCode(200);
    }

    @Test
    @Description("Test if API hit limit exceeded for a token")
    public void testApiHitLimitExceeded() {
        LOGGER.info("Testing if API hit limit is exceeded for a token.");
        String token = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceAndRg(UUID.fromString(collectionId), UUID.randomUUID())
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("apiHits", 0)
                        .put("iat", sixMonthsAgo.getEpochSecond())))
                .build();
        Response response = sendRequest(collectionId, token);
        response.then().statusCode(429).body(DESCRIPTION_KEY, is(API_CALLS_LIMIT_EXCEEDED));
    }

    @Test
    @Description("Test if data usage limit exceeded for a token")
    public void testDataUsageLimitExceeded() {
        LOGGER.info("Testing if data usage limit is exceeded for a token.");
        String token = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceAndRg(UUID.fromString(collectionId), UUID.randomUUID())
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("dataUsage", "0:kb")
                        .put("iat", sixMonthsAgo.getEpochSecond())))
                .build();
        Response response = sendRequest(collectionId, token);
        response.then().statusCode(429).body(DESCRIPTION_KEY, is(DATA_USAGE_LIMIT_EXCEEDED));
    }

    @Test
    @Description("Test handling of unsupported data usage unit in token constraints.")
    public void testInvalidDataUsageUnit() {
        LOGGER.info("Testing invalid data unit for a token.");
        String token = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceAndRg(UUID.fromString(collectionId), UUID.randomUUID())
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("dataUsage", "0:pb")
                        .put("iat", sixMonthsAgo.getEpochSecond())))
                .build();
        Response response = sendRequest(collectionId, token);
        response.then().statusCode(400).body(CODE_KEY, is("Bad Request"));
    }

    @Test
    @Description("Testing full intersection of query bbox with token bbox")
    public void testFullIntersectionOfQueryBboxWithTokenBbox() {
        LOGGER.info("Testing full intersection of query bbox with token bbox");
        String bbox = "-4.5,51.5,-2.5,52.5";
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("bbox", new JsonArray()
                                .add(-5.0)
                                .add(51.0)
                                .add(-2.0)
                                .add(53.0))))
                .build();
        Response response = sendBBoxRequest(collectionId, token, bbox);
        response.then().statusCode(200);
    }

    @Test
    @Description("Testing partial intersection of query bbox with token bbox")
    public void testPartialIntersectionOfQueryBboxWithTokenBbox() {
        LOGGER.info("Testing partial intersection of query bbox with token bbox");
        String bbox = "-6.0,52.0,-3.0,54.0";
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("bbox", new JsonArray()
                                .add(-5.0)
                                .add(51.0)
                                .add(-2.0)
                                .add(53.0))))
                .build();
        Response response = sendBBoxRequest(collectionId, token, bbox);
        response.then().statusCode(200);
    }

    @Test
    @Description("Testing no intersection of query bbox with token bbox")
    public void testNoIntersectionOfQueryBboxWithTokenBbox() {
        LOGGER.info("Testing no intersection of query bbox with token bbox");
        String bbox = "0.0,55.0,1.0,57.0";
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("bbox", new JsonArray()
                                .add(-5.0)
                                .add(51.0)
                                .add(-2.0)
                                .add(53.0))))
                .build();
        Response response = sendBBoxRequest(collectionId, token, bbox);
        response.then().statusCode(403).body(DESCRIPTION_KEY, is(BBOX_VIOLATES_CONSTRAINTS));
    }

    @Test
    @Description("Testing when the data usage limit is exceeded, even if the bbox is within the allowed spatial constraints")
    public void testDataUsageLimitExceededWithBboxLimitsInRange() {
        LOGGER.info("Testing when the data usage limit is exceeded but bounding box is within allowed spatial constraints");
        String bbox = "-4.5,51.5,-2.5,52.5";
        String token = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("dataUsage", "0:kb")
                        .put("iat",sixMonthsAgo.getEpochSecond())
                        .put("bbox", new JsonArray()
                                .add(-5.0)
                                .add(51.0)
                                .add(-2.0)
                                .add(53.0))))
                .build();
        Response response = sendBBoxRequest(collectionId, token, bbox);
        response.then().statusCode(429).body(DESCRIPTION_KEY, is(DATA_USAGE_LIMIT_EXCEEDED));
    }

    @Test
    @Description("Testing when the API hits limit is exceeded, even if the bbox is within the allowed spatial constraints")
    public void testApiHitsLimitExceededWithBboxLimitsInRange() {
        LOGGER.info("Testing when the API hits limit is exceeded but bounding box is within allowed spatial constraints");
        String bbox = "-4.5,51.5,-2.5,52.5";
        String token = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("apiHits", 0)
                        .put("iat",sixMonthsAgo.getEpochSecond())
                        .put("bbox", new JsonArray()
                                .add(-5.0)
                                .add(51.0)
                                .add(-2.0)
                                .add(53.0))))
                .build();
        Response response = sendBBoxRequest(collectionId, token, bbox);
        response.then().statusCode(429).body(DESCRIPTION_KEY, is(API_CALLS_LIMIT_EXCEEDED));
    }

    @Test
    @Description("Test handling of malformed bbox in token constraints")
    public void testMalformedTokenBbox() {
        LOGGER.info("Testing with malformed bbox in token constraints");
        JsonArray malformedBbox = new JsonArray().add("a").add("b").add("c").add("d");
        String queryParamBbox = "-4.5,51.5,-2.5,52.5";
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("bbox", malformedBbox)))
                .build();
        Response response = sendBBoxRequest(collectionId, token, queryParamBbox);
        response.then().statusCode(400).body(DESCRIPTION_KEY, is(ERR_BBOX_NON_NUMERIC));
    }

    @Test
    @Description("Test handling of invalid bbox in token constraints")
    public void testInvalidTokenBbox() {
        LOGGER.info("Testing with invalid bbox in token constraints");
        JsonArray malformedBbox = new JsonArray().add(-5.0).add(51.0).add(-2.0);
        String queryParamBbox = "-4.5,51.5,-2.5,52.5";
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("bbox", malformedBbox)))
                .build();
        Response response = sendBBoxRequest(collectionId, token, queryParamBbox);
        response.then().statusCode(400).body(DESCRIPTION_KEY, is(INVALID_BBOX_FORMAT));
    }

    @Test
    @Description("Test when west is greater than east in bbox inside token")
    public void testBboxWestGreaterThanEastInToken() {
        LOGGER.info("Testing bbox with west > east inside token");
        String queryParamBbox = "-4.5,51.5,-2.5,52.5";
        JsonArray bbox = new JsonArray().add(10.0).add(20.0).add(5.0).add(25.0); // west > east
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("bbox", bbox)))
                .build();
        Response response = sendBBoxRequest(collectionId, token, queryParamBbox);
        response.then().statusCode(400).body(DESCRIPTION_KEY, is(ERR_BBOX_MIN_MAX_ORDER));
    }

    @Test
    @Description("Test when south is greater than north in bbox inside token")
    public void testBboxSouthGreaterThanNorthInToken() {
        LOGGER.info("Testing bbox with south > north inside token");
        String queryParamBbox = "-4.5,51.5,-2.5,52.5";
        JsonArray bbox = new JsonArray().add(-10.0).add(30.0).add(-5.0).add(20.0); // south > north
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("bbox", bbox)))
                .build();
        Response response = sendBBoxRequest(collectionId, token, queryParamBbox);
        response.then().statusCode(400).body(DESCRIPTION_KEY, is(ERR_BBOX_MIN_MAX_ORDER));
    }

    @Test
    @Description("Test when minLon is less than -180 inside token")
    public void testBboxMinLonOutOfRangeInToken() {
        LOGGER.info("Testing bbox with minLon < -180 inside token");
        String queryParamBbox = "-4.5,51.5,-2.5,52.5";
        JsonArray bbox = new JsonArray().add(-181.0).add(10.0).add(50.0).add(20.0); // minLon < -180
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("bbox", bbox)))
                .build();
        Response response = sendBBoxRequest(collectionId, token, queryParamBbox);
        response.then().statusCode(400).body(DESCRIPTION_KEY, is(ERR_BBOX_LONGITUDE_RANGE));
    }

    @Test
    @Description("Test when maxLon is greater than 180 inside token")
    public void testBboxMaxLonOutOfRangeInToken() {
        LOGGER.info("Testing bbox with maxLon > 180 inside token");
        String queryParamBbox = "-4.5,51.5,-2.5,52.5";
        JsonArray bbox = new JsonArray().add(10.0).add(10.0).add(181.0).add(20.0); // maxLon > 180
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("bbox", bbox)))
                .build();
        Response response = sendBBoxRequest(collectionId, token, queryParamBbox);
        response.then().statusCode(400).body(DESCRIPTION_KEY, is(ERR_BBOX_LONGITUDE_RANGE));
    }

    @Test
    @Description("Test when minLat is less than -90 inside token")
    public void testBboxMinLatOutOfRangeInToken() {
        LOGGER.info("Testing bbox with minLat < -90 inside token");
        String queryParamBbox = "-4.5,51.5,-2.5,52.5";
        JsonArray bbox = new JsonArray().add(10.0).add(-91.0).add(50.0).add(20.0); // minLat < -90
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("bbox", bbox)))
                .build();
        Response response = sendBBoxRequest(collectionId, token, queryParamBbox);
        response.then().statusCode(400).body(DESCRIPTION_KEY, is(ERR_BBOX_LATITUDE_RANGE));
    }

    @Test
    @Description("Test when maxLat is greater than 90 inside token")
    public void testBboxMaxLatOutOfRangeInToken() {
        LOGGER.info("Testing bbox with maxLat > 90 inside token");
        String queryParamBbox = "-4.5,51.5,-2.5,52.5";
        JsonArray bbox = new JsonArray().add(10.0).add(10.0).add(50.0).add(91.0); // maxLat > 90
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("bbox", bbox)))
                .build();
        Response response = sendBBoxRequest(collectionId, token, queryParamBbox);
        response.then().statusCode(400).body(DESCRIPTION_KEY, is(ERR_BBOX_LATITUDE_RANGE));
    }

    @Test
    @Description("Test bbox with NaN value inside token")
    public void testBboxWithNaNInToken() {
        LOGGER.info("Testing bbox with NaN value inside token");
        String queryParamBbox = "-4.5,51.5,-2.5,52.5";
        JsonArray bbox = new JsonArray().add("NaN").add(10.0).add(50.0).add(20.0); // NaN value
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("bbox", bbox)))
                .build();
        Response response = sendBBoxRequest(collectionId, token, queryParamBbox);
        response.then().statusCode(400).body(CODE_KEY, is("Bad Request"));
    }

    @Test
    @Description("Test bbox with positive infinity inside token")
    public void testBboxWithPositiveInfinityInToken() {
        LOGGER.info("Testing bbox with positive infinity inside token");
        String queryParamBbox = "-4.5,51.5,-2.5,52.5";
        JsonArray bbox = new JsonArray().add(10.0).add(10.0).add("Infinity").add(20.0); // Positive Infinity
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("bbox", bbox)))
                .build();
        Response response = sendBBoxRequest(collectionId, token, queryParamBbox);
        response.then().statusCode(400).body(CODE_KEY, is("Bad Request"));
    }

    @Test
    @Description("Test bbox with negative infinity inside token")
    public void testBboxWithNegativeInfinityInToken() {
        LOGGER.info("Testing bbox with negative infinity inside token");
        String queryParamBbox = "-4.5,51.5,-2.5,52.5";
        JsonArray bbox = new JsonArray().add("-Infinity").add(10.0).add(50.0).add(20.0); // Negative Infinity
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("bbox", bbox)))
                .build();
        Response response = sendBBoxRequest(collectionId, token, queryParamBbox);
        response.then().statusCode(400).body(CODE_KEY, is("Bad Request"));
    }

    @Test
    @Description("Test bbox limit enforcement at /items/{featureId} endpoint")
    public void testBboxLimitsInFeatureIdEndpoint(){
        LOGGER.info("Testing bbox limit enforcement at /items/{featureId} endpoint");
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("bbox", new JsonArray()
                                .add(-5.0)
                                .add(51.0)
                                .add(-2.0)
                                .add(53.0))))
                .build();
        Response response =
                given().pathParam("collectionId", collectionId)
                        .pathParam("featureId", 1)
                        .auth().oauth2(token)
                        .contentType("application/json")
                        .when().get("/collections/{collectionId}/items/{featureId}");
        response.then().statusCode(403).body(DESCRIPTION_KEY, is("Feature not found within the bbox limit"));
    }

    @Test
    @Description("Success: Test feature limit enforcement at /items endpoint")
    public void testFeatureLimitsEnforcementSuccess(){
        LOGGER.info("Testing feature limit enforcement at /items endpoint success");
        // World Administrative Country Boundaries CollectionId in the token- cfdecaed-54ae-49e2-bf49-43e7d2fe0338
        // FeatureId 5 in the token - United States of America
        // USA Administrative State Boundaries CollectionId in the request- ba56a3d7-a0bb-49b7-a610-e231c73ebb3d
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("feat", new JsonObject()
                                .put("cfdecaed-54ae-49e2-bf49-43e7d2fe0338", new JsonArray()
                                        .add("5")))))
                .build();
        Response response = sendRequest("ba56a3d7-a0bb-49b7-a610-e231c73ebb3d", token);
        response.then().statusCode(200);
    }

    @Test
    @Description("Failure: Test feature limit enforcement when CollectionId in the token does not exist")
    public void testFailureFeatureLimitsWhenCollectionIdNotExists(){
        LOGGER.info("Testing failure of feature limit enforcement when CollectionId in the token does not exist");
        // RANDOM_COLLECTION_ID in the token - 999024e5-0fa4-419a-9225-2604792fc504
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("feat", new JsonObject()
                                .put("999024e5-0fa4-419a-9225-2604792fc504", new JsonArray()
                                        .add("5")))))
                .build();
        Response response = sendRequest("ba56a3d7-a0bb-49b7-a610-e231c73ebb3d", token);
        response.then().statusCode(404).body(DESCRIPTION_KEY, is("Collection not found"));
    }

    @Test
    @Description("Failure: Test feature limit enforcement when FeatureIds in the token do not exist")
    public void testFailureFeatureLimitsWhenFeatureIdsNotExist(){
        LOGGER.info("Testing failure of feature limit enforcement when FeatureIds in the token do not exist");
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("feat", new JsonObject()
                                .put("cfdecaed-54ae-49e2-bf49-43e7d2fe0338", new JsonArray()
                                        .add("990").add("992").add("994")))))
                .build();
        Response response = sendRequest("ba56a3d7-a0bb-49b7-a610-e231c73ebb3d", token);
        response.then().statusCode(403).body(DESCRIPTION_KEY, is("One or more features in the token do not exist"));
    }

    @Test
    @Description("Failure: Test feature limit enforcement when more than 10 FeatureIds exist in the token")
    public void testFailureFeatureLimitsWhenMoreThan10FeatureIdsExist(){
        LOGGER.info("Testing failure of feature limit enforcement when more than 10 FeatureIds exist in the token");
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("feat", new JsonObject()
                                .put("cfdecaed-54ae-49e2-bf49-43e7d2fe0338", new JsonArray()
                                        .add("1").add("2").add("3").add("4").add("5").add("6")
                                        .add("7").add("8").add("9").add("10").add("11").add("12")
                                ))))
                .build();
        Response response = sendRequest("ba56a3d7-a0bb-49b7-a610-e231c73ebb3d", token);
        response.then().statusCode(400).body(DESCRIPTION_KEY, is("Maximum 10 feature IDs allowed per collection"));
    }

    @Test
    @Description("Failure: Test feature limit enforcement at /items endpoint")
    public void testFeatureLimitsEnforcementFailure(){
        LOGGER.info("Testing feature limit enforcement at /items endpoint failure");
        // World Administrative Country Boundaries CollectionId in the token - cfdecaed-54ae-49e2-bf49-43e7d2fe0338
        // FeatureId 99 in the token - India
        // USA Administrative States Boundaries CollectionId in the request - ba56a3d7-a0bb-49b7-a610-e231c73ebb3d
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("feat", new JsonObject()
                                .put("cfdecaed-54ae-49e2-bf49-43e7d2fe0338", new JsonArray()
                                        .add("99")))))
                .build();
        Response response = sendRequest("ba56a3d7-a0bb-49b7-a610-e231c73ebb3d", token);
        response.then().statusCode(403).body(DESCRIPTION_KEY, is("Feature not found within the allowed feature boundaries"));
    }

    @Test
    @Description("Failure: Test feature limit enforcement when no FeatureIds exist in the token")
    public void testFailureFeatureLimitsWhenNoFeatureIdsExist() {
        LOGGER.info("Testing failure of feature limit enforcement when no FeatureIds exist in the token");

        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("feat", new JsonObject()
                                .put("cfdecaed-54ae-49e2-bf49-43e7d2fe0338", new JsonArray()))
                ))
                .build();
        Response response = sendRequest("ba56a3d7-a0bb-49b7-a610-e231c73ebb3d", token);
        response.then().statusCode(400).body(DESCRIPTION_KEY, is("Invalid feature limit format for collection in the token"));
    }

    @Test
    @Description("Failure: Test feature limit enforcement when invalid collection format in the token")
    public void testFailureFeatureLimitsWhenInvalidCollectionFormat() {
        LOGGER.info("Testing failure of feature limit enforcement when invalid collection format in the token");

        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("feat", new JsonObject()
                                .put("World Administrative Country Boundaries", new JsonArray()
                                        .add("5")))
                ))
                .build();
        Response response = sendRequest("ba56a3d7-a0bb-49b7-a610-e231c73ebb3d", token);
        response.then().statusCode(400).body(DESCRIPTION_KEY, is("Invalid collection ID format (must be UUID) in the token"));
    }

    @Test
    @Description("Failure: Test feature limit enforcement when invalid feature id format in the token")
    public void testFailureFeatureLimitsWhenInvalidFeatureIdFormat() {
        LOGGER.info("Testing failure of feature limit enforcement when invalid feature id format in the token");

        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("feat", new JsonObject()
                                .put("cfdecaed-54ae-49e2-bf49-43e7d2fe0338", new JsonArray()
                                        .add("India")))
                ))
                .build();
        Response response = sendRequest("ba56a3d7-a0bb-49b7-a610-e231c73ebb3d", token);
        response.then().statusCode(400).body(DESCRIPTION_KEY, is("Invalid feature ID in token, must be a numeric value for collection"));
    }

    @Test
    @Description("Success: Test feature limit enforcement at /items/{featureId} endpoint")
    public void testFeatureLimitsInFeatureIdEndpointSuccess(){
        LOGGER.info("Testing feature limit enforcement at /items/{featureId} endpoint success");
        // World Administrative Country Boundaries CollectionId in the token - cfdecaed-54ae-49e2-bf49-43e7d2fe0338
        // FeatureId 5 in the token - United States of America
        // USA Administrative States Boundaries CollectionId in the request- ba56a3d7-a0bb-49b7-a610-e231c73ebb3d
        // FeatureId in the request 6 - Washington
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("feat", new JsonObject()
                                .put("cfdecaed-54ae-49e2-bf49-43e7d2fe0338", new JsonArray()
                                        .add("5")))))
                .build();
        Response response =
                given().pathParam("collectionId", "ba56a3d7-a0bb-49b7-a610-e231c73ebb3d")
                        .pathParam("featureId", 6)
                        .auth().oauth2(token)
                        .contentType("application/json")
                        .when().get("/collections/{collectionId}/items/{featureId}");
        response.then().statusCode(200);
    }

    @Test
    @Description("Failure: Test feature limit enforcement at /items/{featureId} endpoint")
    public void testFeatureLimitsInFeatureIdEndpointFailure(){
        LOGGER.info("Testing feature limit enforcement at /items/{featureId} endpoint failure");
        // World Administrative Country Boundaries CollectionId in the token  - cfdecaed-54ae-49e2-bf49-43e7d2fe0338
        // FeatureId 99 in the token - India
        // USA Administrative States Boundaries CollectionId in the request - ba56a3d7-a0bb-49b7-a610-e231c73ebb3d
        // FeatureId in the request 6 - Washington
        String token = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer()
                .withRoleProvider()
                .withCons(new JsonObject().put("limits", new JsonObject()
                        .put("feat", new JsonObject()
                                .put("cfdecaed-54ae-49e2-bf49-43e7d2fe0338", new JsonArray()
                                        .add("99")))))
                .build();
        Response response =
                given().pathParam("collectionId", "ba56a3d7-a0bb-49b7-a610-e231c73ebb3d")
                        .pathParam("featureId", 6)
                        .auth().oauth2(token)
                        .contentType("application/json")
                        .when().get("/collections/{collectionId}/items/{featureId}");
        response.then().statusCode(403).body(DESCRIPTION_KEY, is("Feature not found within the allowed feature boundaries"));
    }
}
