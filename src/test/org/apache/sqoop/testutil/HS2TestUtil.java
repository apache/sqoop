package org.apache.sqoop.testutil;

import org.apache.sqoop.hive.hiveserver2.HiveServer2ConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class HS2TestUtil {

  private static final String SELECT_TABLE_QUERY = "SELECT * FROM %s";

  private HiveServer2ConnectionFactory hs2ConnectionFactory;

  public HS2TestUtil(String url) {
    this(url, null, null);
  }

  public HS2TestUtil(String url, String username, String password) {
    hs2ConnectionFactory = new HiveServer2ConnectionFactory(url, username, password);
  }

  public List<LinkedHashMap<String, Object>> loadRowsFromTable(String tableName) {
    List<LinkedHashMap<String, Object>> result = new ArrayList<>();
    try(Connection connection = hs2ConnectionFactory.createConnection();
        PreparedStatement query = connection.prepareStatement(String.format(SELECT_TABLE_QUERY, tableName))) {

      ResultSet resultSet = query.executeQuery();
      ResultSetMetaData metaData = resultSet.getMetaData();

      while (resultSet.next()) {
        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          row.put(metaData.getColumnName(i), resultSet.getObject(i));
        }
        result.add(row);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  public List<List<Object>> loadRawRowsFromTable(String tableName) {
    List<List<Object>> result = new ArrayList<>();
    List<LinkedHashMap<String, Object>> rowsWithColumnNames = loadRowsFromTable(tableName);

    for (LinkedHashMap<String, Object> rowWithColumnNames : rowsWithColumnNames) {
      result.add(new ArrayList<>(rowWithColumnNames.values()));
    }

    return result;
  }

}
