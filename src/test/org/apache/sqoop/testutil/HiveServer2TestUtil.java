/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.testutil;

import org.apache.commons.lang3.StringUtils;
import org.apache.sqoop.hive.HiveServer2ConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class HiveServer2TestUtil {

  private static final String SELECT_TABLE_QUERY = "SELECT * FROM %s";

  private HiveServer2ConnectionFactory hs2ConnectionFactory;

  public HiveServer2TestUtil(String url) {
    this(url, null, null);
  }

  public HiveServer2TestUtil(String url, String username, String password) {
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

  public List<String> loadCsvRowsFromTable(String tableName) {
    return loadRawRowsFromTable(tableName).stream()
            .map(list -> StringUtils.join(list, ","))
            .collect(toList());
  }

}
