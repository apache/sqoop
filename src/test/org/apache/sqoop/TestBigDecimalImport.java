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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;

/**
 * Test the sqoop.bigdecimal.format.string parameter default behavior and when
 * set to false.
 */
public class TestBigDecimalImport extends ImportJobTestCase {

  private String runBigDecimalImport(List<String> extraArgs)
      throws IOException {
    String[] types =
      { "DECIMAL", "NUMERIC" };
    String[] vals = { "0.000001", "0.0000001" };
    createTableWithColTypes(types, vals);
    List<String> args = new ArrayList<String>();

    CommonArgs.addHadoopFlags(args);

    if (extraArgs!=null) {
      args.addAll(extraArgs);
    }
    args.add("--table");
    args.add(getTableName());
    args.add("--warehouse-dir");
    args.add(getWarehouseDir());
    args.add("--connect");
    args.add(getConnectString());
    args.add("-m");
    args.add("1");

    runImport(args.toArray(new String[args.size()]));

    Path outputFile = getDataFilePath();
    FileSystem fs = FileSystem.get(getConf());
    DataInputStream stream = fs.open(outputFile);
    BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
    String line = reader.readLine();
    reader.close();
    return line;
  }

  public void testBigDecimalDefault() throws IOException {
    String line = runBigDecimalImport(null);
    assertEquals("0.000001,0.0000001", line);
  }

  public void testBigDecimalNoFormat() throws IOException {
    List<String> args = new ArrayList<String>();
    args.add("-Dsqoop.bigdecimal.format.string=false");

    String line = runBigDecimalImport(args);
    assertEquals("0.000001,1E-7", line);
  }

}
