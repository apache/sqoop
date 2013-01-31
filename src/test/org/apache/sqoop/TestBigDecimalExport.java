/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ExportJobTestCase;

/**
 * Test exporting lines that are created via both options of
 * sqoop.bigdecimal.format.string parameter.
 */
public class TestBigDecimalExport extends ExportJobTestCase {

  private void runBigDecimalExport(String line)
      throws IOException, SQLException {
    FileSystem fs = FileSystem.get(getConf());
    Path tablePath = getTablePath();
    fs.mkdirs(tablePath);
    Path filePath = getDataFilePath();
    DataOutputStream stream = fs.create(filePath);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stream));
    writer.write(line);
    writer.close();
    String[] types =
      { "DECIMAL", "NUMERIC" };
    createTableWithColTypes(types, null);

    List<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    args.add("--table");
    args.add(getTableName());
    args.add("--export-dir");
    args.add(tablePath.toString());
    args.add("--connect");
    args.add(getConnectString());
    args.add("-m");
    args.add("1");

    runExport(args.toArray(new String[args.size()]));

    BigDecimal actual1 = null;
    BigDecimal actual2 = null;

    Connection conn = getConnection();
    try {
      PreparedStatement stmt = conn.prepareStatement("SELECT * FROM "
          + getTableName());
      try {
        ResultSet rs = stmt.executeQuery();
        try {
          rs.next();
          actual1 = rs.getBigDecimal(1);
          actual2 = rs.getBigDecimal(2);
        } finally {
          rs.close();
        }
      } finally {
        stmt.close();
      }
    } finally {
      conn.close();
    }

    BigDecimal expected1 = new BigDecimal("0.000001");
    BigDecimal expected2 = new BigDecimal("0.0000001");

    assertEquals(expected1, actual1);
    assertEquals(expected2, actual2);
  }

  public void testBigDecimalDefault() throws IOException, SQLException {
    runBigDecimalExport("0.000001,0.0000001");
  }

  public void testBigDecimalNoFormat() throws IOException, SQLException {
    runBigDecimalExport("0.000001,1E-7");
  }

}
