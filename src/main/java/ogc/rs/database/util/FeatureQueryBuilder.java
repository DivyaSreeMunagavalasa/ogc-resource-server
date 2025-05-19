package ogc.rs.database.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Tuple;
import static ogc.rs.common.Constants.DEFAULT_CRS_SRID;
import static ogc.rs.database.util.Constants.STAC_ITEMS_DATETIME_KEY;

public class FeatureQueryBuilder {
  private static final Logger LOGGER = LogManager.getLogger(FeatureQueryBuilder.class);

  private String tableName;
  private String[] stacCollectionIds = {};
  private int limit;
  private String bbox;
  private String datetime;
  private String filter;
  private String additionalParams;
  private String sqlString;
  private int offset;
  private String defaultCrsSrid;
  private String bboxCrsSrid;
  private String geoColumn;
  private String datetimeKey;
  private String[] stacItemIds = {};
  private String stacIntersectsGeom;

  public FeatureQueryBuilder(String tableName) {
    this.tableName = tableName;
    limit = 10;
    offset = 0;
    bbox = "";
    datetime = "";
    filter = "";
    additionalParams = "";
    sqlString = "";
    datetimeKey = "";
    defaultCrsSrid = String.valueOf(DEFAULT_CRS_SRID);
    bboxCrsSrid = "";
    geoColumn = "cast(st_asgeojson(st_transform(geom," + defaultCrsSrid + ")) as json)";
  }

  /**
   * {@link FeatureQueryBuilder} meant for building STAC Item Search queries.
   */
  public FeatureQueryBuilder() {
    bbox = "";
    datetime = "";
    additionalParams = "";
    sqlString = "";
    datetimeKey = "datetime";
    defaultCrsSrid = String.valueOf(DEFAULT_CRS_SRID);
    bboxCrsSrid = "";
    geoColumn = "cast(st_asgeojson(geom) as json)";
    stacIntersectsGeom = "";
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }
  public void setOffset(int offset) {
    this.offset = offset-1;
  }

  // bboxCrsSrid is srid equivalent of crs-url
  public void setBboxCrsSrid(String bboxCrsSrid) {
    this.bboxCrsSrid = bboxCrsSrid;
  }

  public void setBbox(String coordinates, String storageCrs) {
    if (!bboxCrsSrid.isEmpty() && !bboxCrsSrid.equalsIgnoreCase(defaultCrsSrid))
      coordinates = coordinates.concat(",").concat(bboxCrsSrid);
    else
      coordinates = coordinates.concat(",").concat(defaultCrsSrid);

    if (bboxCrsSrid.equalsIgnoreCase(storageCrs))
      this.bbox = "st_intersects(geom, st_makeenvelope(" + coordinates + "))";
    else
      this.bbox = "st_intersects(geom, st_transform(st_makeenvelope(" + coordinates + "),"+ storageCrs +"))";

    this.additionalParams = "where";
  }

  public void setBboxWhenTokenBboxExists(String queryBbox, String tokenBbox, String storageCrs) {
    // Prepare the query bbox part
    String queryBboxCondition = "";
    if (queryBbox != null) {
      String queryCoordinates = queryBbox;
      if (!bboxCrsSrid.isEmpty() && !bboxCrsSrid.equalsIgnoreCase(defaultCrsSrid))
        queryCoordinates = queryCoordinates.concat(",").concat(bboxCrsSrid);
      else
        queryCoordinates = queryCoordinates.concat(",").concat(defaultCrsSrid);

      if (bboxCrsSrid.equalsIgnoreCase(storageCrs))
        queryBboxCondition = "st_intersects(geom, st_makeenvelope(" + queryCoordinates + "))";
      else
        queryBboxCondition = "st_intersects(geom, st_transform(st_makeenvelope(" + queryCoordinates + "),"+ storageCrs +"))";
    }

    // Prepare the token bbox part
    String tokenBboxCondition = "";
    if (tokenBbox != null) {
      String tokenCoordinates = tokenBbox;
      if (!bboxCrsSrid.isEmpty() && !bboxCrsSrid.equalsIgnoreCase(defaultCrsSrid))
        tokenCoordinates = tokenCoordinates.concat(",").concat(bboxCrsSrid);
      else
        tokenCoordinates = tokenCoordinates.concat(",").concat(defaultCrsSrid);

      if (bboxCrsSrid.equalsIgnoreCase(storageCrs))
        tokenBboxCondition = "st_intersects(geom, st_makeenvelope(" + tokenCoordinates + "))";
      else
        tokenBboxCondition = "st_intersects(geom, st_transform(st_makeenvelope(" + tokenCoordinates + "),"+ storageCrs +"))";
    }

    // Combine conditions
    if (!queryBboxCondition.isEmpty() && !tokenBboxCondition.isEmpty()) {
      this.bbox = "(" + queryBboxCondition + " AND " + tokenBboxCondition + ")";
    } else if (!queryBboxCondition.isEmpty()) {
      this.bbox = queryBboxCondition;
    } else if (!tokenBboxCondition.isEmpty()) {
      this.bbox = tokenBboxCondition;
    }
    this.additionalParams = "where";
  }

  public void setCrs (String crs) {
    // st_asgeojson(geometry, maxdecimaldigits, options); options = 0 means no extra options
    geoColumn = "cast(st_asgeojson(st_transform(geom," + crs + "), 9,0) as json)";
  }
  public void setDatetime(String datetime) {
    if (datetimeKey.isEmpty()) {
      return;
    }
    this.additionalParams = "where";
    
    String datetimeFormat = "'yyyy-mm-dd\"T\"HH24:MI:SS\"Z\"'";
    
    // to_timestamp(datetimeKey, 'datetimeFormat') 'operator' 'datetime' (from request);
    String concatString =
        " to_timestamp(" .concat(datetimeKey).concat(",").concat(datetimeFormat).concat(") ");
    
    if (!datetime.contains("/")) {
      this.datetime = concatString.concat("= '").concat(datetime).concat("'");
      return;
    }
    String[] dateTimeArr = datetime.split("/");
      if (dateTimeArr[0].equals("..")) { // -- before
      this.datetime = concatString.concat("<'").concat(dateTimeArr[1]).concat("'");
  }
    else if (dateTimeArr[1].equals("..")) { // -- after
      this.datetime = concatString.concat(">'").concat(dateTimeArr[0]).concat("'");
    }
    else {
      this.datetime = concatString.concat(" between '").concat(dateTimeArr[0]).concat("' and '")
          .concat(dateTimeArr[1]).concat("'");
    }
  }

  public void setFilter(String key, String value) {
    this.filter = "\"" + key + "\"='" + value + "'";
    this.additionalParams = "where";
  }

  public void setDatetimeKey(String datetimeKey) {
    this.datetimeKey = datetimeKey;

    // if the datetime key is not the STAC datetime key (when used with OGC features), then
    // add double-quotes around the key. Double-quotes are needed as the column can have hypens or
    // caps in it. The STAC datetime key is a JSONB expression, so no double-quotes are required.
    if (STAC_ITEMS_DATETIME_KEY.equals(datetimeKey)) {
      this.datetimeKey = datetimeKey; 
    }
    else {
      this.datetimeKey = "\"" + datetimeKey + "\"";
    }
  }

  public void setStacItemIds(String[] itemIds) {
    this.stacItemIds = itemIds;
  }
  
  public void setStacCollectionIds(String[] collectionIds) {
    this.stacCollectionIds = collectionIds;
  }

  public void setStacIntersectsGeom(JsonObject geometry) {
    // this is a geojson geometry
    this.stacIntersectsGeom =
        "st_intersects(geom, st_geomfromgeojson('" + geometry.toString() + "'))";
  }

  public String buildSqlString() {
    //TODO: refactor to build the sql query
    this.sqlString = String.format("select id, 'Feature' as type, %4$s as geometry, (row_to_json(\"%1$s\")::jsonb - " +
            " 'id' - 'geom') as properties from \"%1$s\" where id > %3$d ORDER BY id limit %2$d"
        , this.tableName, this.limit, this.offset, this.geoColumn);

    if (!bbox.isEmpty()) {
      this.sqlString = String.format("select id, 'Feature' as type, %6$s as geometry, (row_to_json(\"%1$s\")::jsonb - " +
              " 'id' - 'geom') as properties from \"%1$s\" %3$s %4$s and id > %5$d ORDER BY id limit %2$d"
          ,this.tableName,this.limit, this.additionalParams, this.bbox, this.offset, this.geoColumn);
    }

    if(!datetime.isEmpty() ){
      this.sqlString = String.format("select id, 'Feature' as type, %6$s as geometry, (row_to_json(\"%1$s\")::jsonb - " +
              "'id' - 'geom') as properties from \"%1$s\" %3$s %4$s and id > %5$d ORDER BY id limit %2$d"
          ,this.tableName,this.limit, this.additionalParams, this.datetime, this.offset, this.geoColumn);
    }

    if (!filter.isEmpty()) {
      this.sqlString = String.format("select id, 'Feature' as type, %6$s as geometry, (row_to_json(\"%1$s\")::jsonb - " +
              " 'id' - 'geom') as properties from \"%1$s\" %3$s %4$s and id > %5$d ORDER BY id limit %2$d"
          ,this.tableName,this.limit, this.additionalParams, this.filter, this.offset, this.geoColumn);
    }

    if (!bbox.isEmpty() && !filter.isEmpty()) {
      this.sqlString = String.format("select id, 'Feature' as type, %7$s as geometry, (row_to_json(\"%1$s\")::jsonb - " +
              "'id' - 'geom') as properties from \"%1$s\" %3$s %4$s and %5$s and id > %6$d ORDER BY id limit %2$d"
          ,this.tableName,this.limit, this.additionalParams, this.bbox, this.filter, this.offset, this.geoColumn);
    }

    if (!bbox.isEmpty() && !datetime.isEmpty()) {
      this.sqlString = String.format("select id, 'Feature' as type, %7$s as geometry, (row_to_json(\"%1$s\")::jsonb - " +
              " 'id' - 'geom') as properties from \"%1$s\" %3$s %4$s and %5$s and %6$d ORDER BY id limit %2$d"
          ,this.tableName,this.limit, this.additionalParams, this.bbox, this.datetime, this.offset, this.geoColumn);
    }

    if (!datetime.isEmpty() && !filter.isEmpty()) {
      this.sqlString = String.format("select id, 'Feature' as type, %7$s as geometry, (row_to_json(\"%1$s\")::jsonb - " +
              " 'id' - 'geom') as properties from \"%1$s\" %3$s %4$s and %5$s and id > %6$d ORDER BY id limit %2$d"
          ,this.tableName,this.limit, this.additionalParams, this.datetime, this.filter, this.offset, this.geoColumn);
    }

    if (!bbox.isEmpty() && !filter.isEmpty() && !datetime.isEmpty()) {
      this.sqlString = String.format("select id, 'Feature' as type, %8$s as geometry, (row_to_json(\"%1$s\")::jsonb - " +
              " 'id' - 'geom') as properties from \"%1$s\" %3$s %4$s and %5$s and %7$s and id > %6$d ORDER BY id limit %2$d"
          ,this.tableName,this.limit, this.additionalParams, this.bbox, this.filter, this.offset, this.datetime,
          this.geoColumn);
    }
    LOGGER.debug("<builder>Sql query- {}", sqlString);
    return sqlString;
  }

  public String buildSqlString(String isCountQuery) {

    this.sqlString = String.format("select count(id) from \"%1$s\" "
        , this.tableName);

    if (!bbox.isEmpty()) {
    this.sqlString = String.format("select count(id) from \"%1$s\" %2$s %3$s"
        ,this.tableName, this.additionalParams, this.bbox);
    }
    if(!datetime.isEmpty() ){
      this.sqlString = String.format("select count(id) from \"%1$s\" %2$s %3$s"
          ,this.tableName, this.additionalParams, this.datetime);
    }

    if (!filter.isEmpty()) {
      this.sqlString = String.format("select count(id) from \"%1$s\" %2$s %3$s"
          ,this.tableName, this.additionalParams, this.filter);
    }

    if (!bbox.isEmpty() && !filter.isEmpty()) {
      this.sqlString = String.format("select count(id) from \"%1$s\" %2$s %3$s and %4$s"
          ,this.tableName, this.additionalParams, this.bbox, this.filter);
    }

    if (!bbox.isEmpty() && !datetime.isEmpty()) {
      this.sqlString = String.format("select count(id) from \"%1$s\" %2$s %3$s and %4$s"
          ,this.tableName, this.additionalParams, this.bbox, this.datetime);
    }

    if (!datetime.isEmpty() && !filter.isEmpty()) {
      this.sqlString = String.format("select count(id) from \"%1$s\" %2$s %3$s and %4$s"
          ,this.tableName, this.additionalParams, this.datetime, this.filter);
    }

    if (!bbox.isEmpty() && !filter.isEmpty() && !datetime.isEmpty()) {
      this.sqlString = String.format("select count(id) from \"%1$s\" %2$s %3$s and %4$s and %5$s"
          ,this.tableName, this.additionalParams, this.bbox, this.filter, this.datetime);
    }
    LOGGER.debug("<builder>Count query- {}", sqlString);
    return sqlString;
  }
  
  /**
   * Build query string needed for STAC Item Search. An empty {@link Tuple} is passed in as a
   * parameter. Query params are added to tuple when a safe-query string formed by appending cannot
   * be created. The returned query must be executed with the tuple.
   * 
   * STAC Item Search uses PostgreSQL table partitioning. A partitioned table called
   * <em>stac_collections_part</em> is queried instead of querying individual STAC collection
   * tables.
   * 
   * @param tup empty tuple to which query params can be added
   * @return the formed query which must be run with the passed-in tuple
   */
  public String buildItemSearchSqlString(Tuple tup) {

    StringBuilder stacPartitionTableQuery = new StringBuilder();

    stacPartitionTableQuery.append(
        "SELECT scp.id AS id, 'Feature' AS type, collection_id AS collection, " + this.geoColumn
            + " AS geometry, properties, p_id FROM stac_collections_part scp WHERE 1=1");

    // integer that stores the parameter index as params are added to the tuple
    int parameterIndex = 0;

    if (!bbox.isEmpty()) {
      stacPartitionTableQuery.append(" AND ").append(this.bbox);
    }

    if (!datetime.isEmpty()) {
      stacPartitionTableQuery.append(" AND ").append(this.datetime);
    }

    if (!stacIntersectsGeom.isEmpty()) {
      stacPartitionTableQuery.append(" AND ").append(this.stacIntersectsGeom);
    }

    // need to use tuple here otherwise need to do a lot of work to create the collection ID string
    if (stacCollectionIds.length != 0) {
      parameterIndex++;
      stacPartitionTableQuery
          .append(" AND collection_id::text = ANY($" + parameterIndex + ")");
      tup.addArrayOfString(stacCollectionIds);
    }

    // need to use tuple here since anything can be passed in as item ID, so chance of SQL injection
    // (?)
    if (stacItemIds.length != 0) {
      parameterIndex++;
      stacPartitionTableQuery.append(" AND id = ANY($" + parameterIndex + ")");
      tup.addArrayOfString(stacItemIds);
    }

    if (offset != 0) {
      stacPartitionTableQuery.append(" AND p_id > ").append(offset);
    }
    
    // limit always added
    stacPartitionTableQuery.append(" LIMIT ").append(this.limit);

    // forming CTE with the stac_collections_part query to get required data from stac_items_assets
    // and then joining the result
    StringBuilder finalCteQuery = new StringBuilder().append("WITH items AS (")
        .append(stacPartitionTableQuery.toString())
        .append("), assets AS (SELECT collection_id, item_id,"
            + " jsonb_agg((row_to_json(stac_items_assets.*)::jsonb - 'item_id'))"
            + " AS assetobjects FROM stac_items_assets"
            + " JOIN items ON item_id = items.id AND collection_id = items.collection"
            + " GROUP BY collection_id, item_id)"
            + " SELECT items.*, assets.assetobjects FROM assets"
            + " JOIN items ON items.collection = assets.collection_id AND assets.item_id = items.id"
            + " ORDER BY items.p_id");

    LOGGER.debug("<builder> Item Search SQL query - {}", finalCteQuery.toString());

    return finalCteQuery.toString();
  }


}
