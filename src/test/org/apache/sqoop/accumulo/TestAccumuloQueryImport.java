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

package org.apache.sqoop.accumulo;

import java.io.IOException;

import org.junit.Test;

/**
 * Test import of free-form query into Accumulo.
 */
public class TestAccumuloQueryImport extends AccumuloTestCase {

  @Test
  public void testImportFromQuery() throws IOException {
    String [] types = { "INT", "INT", "INT" };
    String [] vals = { "0", "42", "43" };
    createTableWithColTypes(types, vals);

    String [] argv = getArgv("queryT", "queryF", true,
          "SELECT " + getColName(0) + ", " + getColName(1) + " FROM "
          + getTableName() + " WHERE $CONDITIONS");
    runImport(argv);

    // This cell should import correctly.
    verifyAccumuloCell("queryT", "0", "queryF", getColName(1), "42");

    // This cell should not be placed in the results..
    verifyAccumuloCell("queryT", "0", "queryF", getColName(2), null);
  }

  @Test
  public void testExitFailure() throws IOException {
    String [] argv = getArgv("NoAccumuloT", "NoAccumuloF", true, null);
    String [] types = { "INT", "INT", "INT" };
    String [] vals = { "0", "42", "43" };
    createTableWithColTypes(types, vals);
    try {
      AccumuloUtil.setAlwaysNoAccumuloJarMode(true);
      runImport(argv);
      fail("should have gotten exception");
    } catch (IOException e)  {
      // Got the exception, so we're happy
      LOG.info("Got exception -- ok; we expected that to fail.");
    } finally {
      AccumuloUtil.setAlwaysNoAccumuloJarMode(false);
    }
  }
}
