package ogc.rs.database.util;

import java.sql.SQLOutput;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class FeatureQueryBuilder {
  private String tableName;
  private int limit;
  private String bbox;
  private String datetime;
  private String filter;
  private String additionalParams;
  private String sqlString;
  private int offset;

  private String datetimeKey;

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
  }

  public void setLimit(int limit) {
    if(limit > 1000 || limit < 1) {
      return;
    }
    this.limit = limit;
  }
  public void setOffset(int offset) {
    if (offset > 1000)
      return;
    this.offset = offset-1;
  }

  public void setBbox(String coordinates) {
    coordinates = coordinates.concat(",4326");
    //TODO: validation for lat, lon values (0<=lat<=90, 0<=lon<=180);
    this.bbox = "st_intersects(geom, st_makeenvelope(" + coordinates + "))";
    this.additionalParams = "where";
  }

  public void setDatetime(String datetime) {
    if (datetimeKey.isEmpty()) {
      return;
    }
    this.additionalParams = "where";
    String datetimeFormat = "'yyyy-mm-dd\"T\"HH24:MI:SS\"Z\"'";
//    datetime query where clause -
//    to_timestamp(properties ->> 'datetimeKey', 'datetimeFormat') 'operator' 'datetime' (from request);
    String concatString =
        " to_timestamp(properties ->> '" .concat(datetimeKey).concat("',").concat(datetimeFormat).concat(") ");
    if (!datetime.contains("/")) {
      this.datetime = concatString.concat("= '").concat(datetime).concat("'");
      return;
    }
    String[] dateTimeArr = datetime.split("/");
      if (dateTimeArr[0].equals("..")) { // -- before\
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
    this.filter = "properties->>'" + key + "'='" + value + "'";
    this.additionalParams = "where";
  }

  public void setDatetimeKey(String datetimeKey) {
    this.datetimeKey = datetimeKey;
  }

  public String buildSqlString() {
    //TODO: refactor to build the sql query
    this.sqlString = String.format("select id, itemType as type, cast(st_asgeojson(geom) as json) as geometry, properties" +
            " from \"%1$s\" limit %2$d offset %3$d"
        , this.tableName, this.limit, this.offset);

    if (!bbox.isEmpty()) {
      this.sqlString = String.format("select id, itemType as type, cast(st_asgeojson(geom) as json) as geometry, properties" +
              " from \"%1$s\" %3$s %4$s limit %2$d offset %5$d"
          ,this.tableName,this.limit, this.additionalParams, this.bbox, this.offset);
    }

    if(!datetime.isEmpty() ){
      this.sqlString = String.format("select id, itemType as type, cast(st_asgeojson(geom) as json) as geometry, properties " +
              " from \"%1$s\" %3$s %4$s limit %2$d offset %5$d"
          ,this.tableName,this.limit, this.additionalParams, this.datetime, this.offset);
    }

    if (!filter.isEmpty()) {
      this.sqlString = String.format("select id, itemType as type, cast(st_asgeojson(geom) as json) as geometry, properties" +
              " from \"%1$s\" %3$s %4$s limit %2$d offset %5$d"
          ,this.tableName,this.limit, this.additionalParams, this.filter, this.offset);
    }

    if (!bbox.isEmpty() && !filter.isEmpty()) {
      this.sqlString = String.format("select id, itemType as type, cast(st_asgeojson(geom) as json) as geometry, properties" +
              " from \"%1$s\" %3$s %4$s and %5$s limit %2$d offset %6$d"
          ,this.tableName,this.limit, this.additionalParams, this.bbox, this.filter, this.offset);
    }

    if (!bbox.isEmpty() && !datetime.isEmpty()) {
      this.sqlString = String.format("select id, itemType as type, cast(st_asgeojson(geom) as json) as geometry, properties" +
              " from \"%1$s\" %3$s %4$s and %5$s limit %2$d offset %6$d"
          ,this.tableName,this.limit, this.additionalParams, this.bbox, this.datetime, this.offset);
    }

    if (!datetime.isEmpty() && !filter.isEmpty()) {
      this.sqlString = String.format("select id, itemType as type, cast(st_asgeojson(geom) as json) as geometry, properties" +
              " from \"%1$s\" %3$s %4$s and %5$s limit %2$d offset %6$d"
          ,this.tableName,this.limit, this.additionalParams, this.datetime, this.filter, this.offset);
    }

    if (!bbox.isEmpty() && !filter.isEmpty() && !datetime.isEmpty()) {
      this.sqlString = String.format("select id, itemType as type, cast(st_asgeojson(geom) as json) as geometry, properties" +
              " from \"%1$s\" %3$s %4$s and %5$s and %7$s limit %2$d offset %6$d"
          ,this.tableName,this.limit, this.additionalParams, this.bbox, this.filter, this.offset, this.datetime);
    }

    System.out.println("<builder>Sql query- " + sqlString);
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
    System.out.println("<builder>Count query- " + sqlString);
    return sqlString;
  }


  public int getLimit() {
    return this.limit;
  }
}