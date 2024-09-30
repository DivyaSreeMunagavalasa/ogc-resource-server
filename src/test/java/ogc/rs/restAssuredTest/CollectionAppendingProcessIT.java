package ogc.rs.restAssuredTest;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonObject;
import jdk.jfr.Description;
import ogc.rs.util.FakeTokenBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static ogc.rs.common.Constants.*;
import static ogc.rs.processes.collectionAppending.Constants.*;
import static ogc.rs.processes.collectionAppending.Constants.RESOURCE_OWNERSHIP_ERROR;
import static ogc.rs.processes.util.Status.ACCEPTED;
import static ogc.rs.restAssuredTest.Constant.*;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(RestAssuredConfigExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CollectionAppendingProcessIT {
    private static final Logger LOGGER = LogManager.getLogger(CollectionAppendingProcessIT.class);
    String executionEndpoint = "/processes/{processId}/execution";
    String jobStatusEndpoint = "/jobs/{jobId}";
    String processId = "b118b4d4-0bc1-4d0b-b137-fdf5b0558c1d";


    @BeforeAll
    public static void setup() throws IOException {
        // Set up files for the tests

        // put the file in /fileName path for Ogr
        File validFile = new File("src/test/resources/processFiles/append_1000_point_features.json");
        given().port(PORT).body(validFile).when().put(BUCKET_PATH + "append_1000_point_features.json")
                .then().statusCode(200);

        // put the file in bucket1/fileName to read file directly from S3
        given().port(PORT).body(validFile).when().put(BUCKET_PATH_FOR_S3 + "append_1000_point_features.json")
                .then().statusCode(200);

        File notEPSGFile = new File("src/test/resources/processFiles/not_EPSG_crs.json");
        given().port(PORT).body(notEPSGFile).when().put(BUCKET_PATH + "not_EPSG_crs.json")
                .then().statusCode(200);

        File invalidSRIDFile = new File("src/test/resources/processFiles/not_registered_EPSG.json");
        given().port(PORT).body(invalidSRIDFile).when().put(BUCKET_PATH + "not_registered_EPSG.json")
                .then().statusCode(200);

        File invalidSchemaFile = new File("src/test/resources/processFiles/invalid_schema_file.json");
        given().port(PORT).body(invalidSchemaFile).when().put(BUCKET_PATH + "invalid_schema_file.json")
                .then().statusCode(200);

        File layersWithDiffGeomFile = new File("src/test/resources/processFiles/2LayersWithDifferentGeom.json");
        given().port(PORT).body(layersWithDiffGeomFile).when().put(BUCKET_PATH + "2LayersWithDifferentGeom.json");

    }

    private JsonObject requestBody() {
        JsonObject requestBody = new JsonObject();

        JsonObject inputs = new JsonObject();
        inputs.put("fileName", "append_1000_point_features.json");
        inputs.put("title", "Valid Append File");
        inputs.put("description", "Valid file for appending test.");
        inputs.put("resourceId", "61f2187e-affe-4f28-be0e-fe1cd37dbd4e");
        inputs.put("version", "1.0.0");

        requestBody.put("inputs", inputs);

        requestBody.put("response", "raw");
        return requestBody;
    }

    private String getToken() {
        return new FakeTokenBuilder().withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceServer().withRoleProvider().withCons(new JsonObject()).build();
    }

    private Response sendExecutionRequest(String processId, String token, JsonObject requestBody) {
        return RestAssured.given().pathParam("processId", processId).auth().oauth2(token)
                .contentType("application/json").body(requestBody.toString()).when().post(executionEndpoint);
    }

    private Response sendJobStatusRequest(String jobId, String token) {
        return RestAssured.given().pathParam("jobId", jobId).auth().oauth2(token)
                .get(jobStatusEndpoint);
    }

    @Test
    @Order(1)
    @Description("Failure: Unauthorized user.")
    public void testExecuteUnauthorizedFail() {
        LOGGER.debug("Testing Failure: Unauthorized user.");
        String invalidToken = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceServer().withRoleConsumer().withCons(new JsonObject()).build();
        Response response = sendExecutionRequest(processId, invalidToken, requestBody());
        response.then().statusCode(401).body(CODE_KEY, is("Not Authorized"));
    }

    @Test
    @Order(2)
    @Description("Failure: Consumer Delegate User")
    public void testExecuteWithConsumerDelegateUserFail() {
        LOGGER.debug("Testing Failure: Consumer Delegate User");
        String invalidToken = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceServer().withDelegate(UUID.fromString("9304cb99-7125-47f1-8686-a070bb6c3eaf"), "consumer")
                .withCons(new JsonObject()).build();
        Response response = sendExecutionRequest(processId, invalidToken, requestBody());
        response.then().statusCode(401).body(CODE_KEY, is("Not Authorized"));
    }

    @Test
    @Order(3)
    @Description("Failure: Provider Delegate with different provider id")
    public void testExecuteWithDifferentDelegateUserIdFail() {
        LOGGER.debug("Testing Failure: Provider Delegate with different DID");
        String invalidToken = new FakeTokenBuilder()
                .withSub(UUID.randomUUID())
                .withResourceServer().withDelegate(UUID.fromString("9304cb99-7125-47f1-8686-a070bb6c3eaf"), "provider")
                .withCons(new JsonObject()).build();
        Response sendExecutionRequest = sendExecutionRequest(processId, invalidToken, requestBody());
        String jobId = sendExecutionRequest.body().path("jobId");
        try {
            // Use Awaitility to wait for the job status response
            await().atMost(25, TimeUnit.SECONDS).until(() -> {
                Response getJobStatus = sendJobStatusRequest(jobId, invalidToken);
                return getJobStatus.body().path("message").equals(RESOURCE_OWNERSHIP_ERROR);
            });
        } catch (ConditionTimeoutException e) {
            fail("Test failed due to timeout while waiting for job status indicating that the job status is not retrieved within time:" + " " +e.getMessage());
        }
    }

    @Test
    @Order(4)
    @Description("Success: Provider Delegate Flow")
    public void testExecuteWithProviderDelegateUser() {
        LOGGER.debug("Testing Success: Provider Delegate User");
        String token = new FakeTokenBuilder()
                .withSub(UUID.fromString("0ff3d306-9402-4430-8e18-6f95e4c03c97"))
                .withResourceServer().withDelegate(UUID.fromString("9304cb99-7125-47f1-8686-a070bb6c3eaf"), "provider")
                .withCons(new JsonObject()).build();
        Response response = sendExecutionRequest(processId, token, requestBody());
        response.then().statusCode(201).body("status", is(ACCEPTED.toString()));
    }

    @Test
    @Order(5)
    @Description("Success: Process Accepted")
    public void testExecuteProcess() {
        LOGGER.debug("Testing Success: Process Accepted");
        String token = getToken();
        Response response = sendExecutionRequest(processId, token, requestBody());
        response.then().statusCode(201).body("status", is(ACCEPTED.toString()));
    }

    @Test
    @Order(6)
    @Description("Failure: Process does not Exist")
    public void testExecuteFailProcessNotPresent() {
        LOGGER.debug("Testing Failure: Process does not Exist");
        String invalidProcessId = "b118b4d4-0bc1-4d0b-b137-fdf5b0558c1b";
        Response response = sendExecutionRequest(invalidProcessId, getToken(), requestBody());
        response.then().statusCode(404).body(TYPE_KEY, is(NOT_FOUND));
    }

    @Test
    @Order(7)
    @Description("Failure: Invalid input")
    public void testExecuteFailInvalidInput() {
        LOGGER.debug("Testing Failure: Invalid input");
        JsonObject invalidRequest = new JsonObject()
                .put("inputs", requestBody().getJsonObject("inputs").remove("fileName"));
        Response response = sendExecutionRequest(processId, getToken(), invalidRequest);
        response.then().statusCode(400).body(CODE_KEY, is("Bad Request"));
    }

    @Test
    @Order(8)
    @Description("Failure: Ownership Error")
    public void testExecuteFailOwnershipError() {
        LOGGER.debug("Failure: Ownership Error");

        String token = new FakeTokenBuilder().withSub(UUID.randomUUID())
                .withResourceServer().withRoleProvider().withCons(new JsonObject()).build();
        Response sendExecutionRequest = sendExecutionRequest(processId, token, requestBody());
        String jobId = sendExecutionRequest.body().path("jobId");
        try {
            // Use Awaitility to wait for the job status response
            await().atMost(25, TimeUnit.SECONDS).until(() -> {
                Response getJobStatus = sendJobStatusRequest(jobId, token);
                return getJobStatus.body().path("message").equals(RESOURCE_OWNERSHIP_ERROR);
            });
        } catch (ConditionTimeoutException e) {
            fail("Test failed due to timeout while waiting for job status indicating that the job status is not retrieved within time:" + " " +e.getMessage());
        }
    }

    @Test
    @Order(9)
    @Description("Failure: Check if collection is present in collection_details table")
    public void testFailCheckIfCollectionPresent() {
        LOGGER.debug("Failure: checkIfCollectionPresent");

        String token = getToken();
        JsonObject requestBody = requestBody();
        requestBody.getJsonObject("inputs").put("resourceId", UUID.randomUUID().toString())
                .put("title", "Non-existing Collection test").put("description", "Invalid collection id for testing");
        Response sendExecutionRequest = sendExecutionRequest(processId, token, requestBody);
        String jobId = sendExecutionRequest.body().path("jobId");
        try {
            // Use Awaitility to wait for the job status response
            await().atMost(45, TimeUnit.SECONDS).until(() -> {
                Response getJobStatus = sendJobStatusRequest(jobId, token);
                return getJobStatus.body().path("message").equals(COLLECTION_NOT_FOUND_MESSAGE);
            });
        } catch (ConditionTimeoutException e) {
            fail("Test failed due to timeout while waiting for job status indicating that the job status is not retrieved within time:" + " " +e.getMessage());
        }
    }

    @Test
    @Order(10)
    @Description("Failure: Invalid organization")
    public void testExecuteFailInvalidOrganization() {
        LOGGER.debug("Failure: Invalid organization");

        String token = getToken();
        JsonObject requestBody = requestBody();
        requestBody.getJsonObject("inputs").put("fileName", "not_EPSG_crs.json")
                .put("title", "Invalid Organization test").put("description", "File with invalid Organization for testing.");
        Response sendExecutionRequest = sendExecutionRequest(processId, token, requestBody);
        String jobId = sendExecutionRequest.body().path("jobId");
        try {
            // Use Awaitility to wait for the job status response
            await().atMost(60, TimeUnit.SECONDS).until(() -> {
                Response getJobStatus = sendJobStatusRequest(jobId, token);
                return getJobStatus.body().path("message").equals(INVALID_ORGANISATION_MESSAGE);
            });
        } catch (ConditionTimeoutException e) {
            fail("Test failed due to timeout while waiting for job status indicating that the job status is not retrieved within time:" + " " +e.getMessage());
        }
    }

    @Test
    @Order(11)
    @Description("Failure: Invalid CRS- SRID")
    public void testExecuteFailInvalidCode() {
        LOGGER.debug("Failure: Invalid SRID");

        String token = getToken();
        JsonObject requestBody = requestBody();
        requestBody.getJsonObject("inputs").put("fileName", "not_registered_EPSG.json")
                .put("title", "Invalid CRS- SRID test").put("description", "File with invalid CRS- SRID for testing.");
        Response sendExecutionRequest = sendExecutionRequest(processId, token, requestBody);
        String jobId = sendExecutionRequest.body().path("jobId");

        try {
            // Use Awaitility to wait for the job status response
            await().atMost(60, TimeUnit.SECONDS).until(() -> {
                Response getJobStatus = sendJobStatusRequest(jobId, token);
                return getJobStatus.body().path("message").equals(INVALID_SR_ID_MESSAGE);
            });
        } catch (ConditionTimeoutException e) {
            fail("Test failed due to timeout while waiting for job status indicating that the job status is not retrieved within time:" + " " +e.getMessage());
        }
    }

    @Test
    @Order(12)
    @Description("Failure: checkSchema")
    public void testFailCheckSchema() {
        LOGGER.debug("Failure: checkSchema");

        String token = getToken();
        JsonObject requestBody = requestBody();
        requestBody.getJsonObject("inputs").put("fileName", "invalid_schema_file.json");
        Response sendExecutionRequest = sendExecutionRequest(processId, token, requestBody);
        String jobId = sendExecutionRequest.body().path("jobId");
        try {
            // Use Awaitility to wait for the job status response
            await().atMost(60, TimeUnit.SECONDS).until(() -> {
                Response getJobStatus = sendJobStatusRequest(jobId, token);
                return getJobStatus.body().path("message").equals(SCHEMA_VALIDATION_FAILURE_MESSAGE);
            });
        } catch (ConditionTimeoutException e) {
            fail("Test failed due to timeout while waiting for job status indicating that the job status is not retrieved within time:" + " " +e.getMessage());
        }
    }

    @Test
    @Order(13)
    @Description("Failure: Invalid file having 2 layers with different geometry")
    public void testExecuteFail2DiffGeo() {
        LOGGER.debug("Failure: Invalid file having 2 layers with different geometry");

        String token = getToken();
        JsonObject requestBody = requestBody();
        requestBody.getJsonObject("inputs").put("fileName", "2LayersWithDifferentGeom.json")
                .put("title", "Invalid Geometry test").put("description", "File having 2 layers with different geometry.");
        Response sendExecutionRequest = sendExecutionRequest(processId, token, requestBody);
        String jobId = sendExecutionRequest.body().path("jobId");
        try {
            // Use Awaitility to wait for the job status response
            await().atMost(90, TimeUnit.SECONDS).until(() -> {
                Response getJobStatus = sendJobStatusRequest(jobId, token);
                return getJobStatus.body().path("message").equals(OGR_2_OGR_FAILED_MESSAGE);
            });
        } catch (ConditionTimeoutException e) {
            fail("Test failed due to timeout while waiting for job status indicating that the job status is not retrieved within time:" + " " +e.getMessage());
        }
    }

    @Test
    @Order(14)
    @Description("Success: Appending data to collection")
    public void testExecuteAppendingSuccess() {
        LOGGER.debug("Success: Appending data to collection");

        String token = getToken();
        JsonObject requestBody = requestBody();
        requestBody.getJsonObject("inputs").put("fileName", "bucket1/append_1000_point_features.json")
                .put("title", "Valid Append File").put("description", "Valid file for appending test.");
        Response sendExecutionRequest = sendExecutionRequest(processId, token, requestBody);
        String jobId = sendExecutionRequest.body().path("jobId");
        try {
            // Use Awaitility to wait for the job status response
            await().atMost(120, TimeUnit.SECONDS).until(() -> {
                Response getJobStatus = sendJobStatusRequest(jobId, token);
                return getJobStatus.body().path("message").equals(BBOX_UPDATE_MESSAGE);
            });
        } catch (ConditionTimeoutException e) {
            fail("Test failed due to timeout while waiting for job status indicating that the job status is not retrieved within time:" + " " +e.getMessage());
        }
    }
}
