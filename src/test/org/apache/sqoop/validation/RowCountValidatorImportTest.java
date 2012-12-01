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

package org.apache.sqoop.validation;

import com.cloudera.sqoop.testutil.ImportJobTestCase;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests for RowCountValidator.
 */
public class RowCountValidatorImportTest extends ImportJobTestCase {

  protected List<String> getExtraArgs(Configuration conf) {
    ArrayList<String> list = new ArrayList<String>(1);
    list.add("--validate");
    return list;
  }

  /**
   * Test the implementation for AbsoluteValidationThreshold.
   * Both arguments should be same else fail.
   */
  public void testAbsoluteValidationThreshold() {
    ValidationThreshold validationThreshold = new AbsoluteValidationThreshold();
    assertTrue(validationThreshold.compare(100, 100));
    assertFalse(validationThreshold.compare(100, 90));
    assertFalse(validationThreshold.compare(90, 100));
  }

  /**
   * Test if teh --validate flag actually made it through the options.
   *
   * @throws Exception
   */
  public void testValidateOptionIsEnabled() throws Exception {
    String[] types = {"INT NOT NULL PRIMARY KEY", "VARCHAR(32)", "VARCHAR(32)"};
    String[] insertVals = {"1", "'Bob'", "'sales'"};

    try {
      createTableWithColTypes(types, insertVals);

      String[] args = getArgv(true, null, getConf());
      ArrayList<String> argsList = new ArrayList<String>();
      Collections.addAll(argsList, args);
      assertTrue("Validate option missing.", argsList.contains("--validate"));
    } finally {
      dropTableIfExists(getTableName());
    }
  }

  /**
   * Test the validation for a sample import, positive case.
   *
   * @throws Exception
   */
  public void testValidatorForImportTable() throws Exception {
    String[] types = {"INT NOT NULL PRIMARY KEY", "VARCHAR(32)", "VARCHAR(32)"};
    String[] insertVals = {"1", "'Bob'", "'sales'"};
    String validateLine = "1,Bob,sales";

    try {
      createTableWithColTypes(types, insertVals);

      verifyImport(validateLine, null);
      LOG.debug("Verified input line as " + validateLine + " -- ok!");
    } finally {
      dropTableIfExists(getTableName());
    }
  }
}
