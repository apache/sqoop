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

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.tool.ImportTool;

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
   * Test if the --validate flag actually made it through the options.
   *
   * @throws Exception
   */
  public void testValidateOptionIsEnabledInCLI() throws Exception {
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

  public void testValidationOptionsParsedCorrectly() throws Exception {
    String[] types = {"INT NOT NULL PRIMARY KEY", "VARCHAR(32)", "VARCHAR(32)"};
    String[] insertVals = {"1", "'Bob'", "'sales'"};

    try {
      createTableWithColTypes(types, insertVals);

      String[] args = getArgv(true, null, getConf());
      ArrayList<String> argsList = new ArrayList<String>();
      argsList.add("--validator");
      argsList.add("org.apache.sqoop.validation.RowCountValidator");
      argsList.add("--validation-threshold");
      argsList.add("org.apache.sqoop.validation.AbsoluteValidationThreshold");
      argsList.add("--validation-failurehandler");
      argsList.add("org.apache.sqoop.validation.AbortOnFailureHandler");
      Collections.addAll(argsList, args);

      assertTrue("Validate option missing.", argsList.contains("--validate"));
      assertTrue("Validator option missing.", argsList.contains("--validator"));

      String[] optionArgs = toStringArray(argsList);

      SqoopOptions validationOptions = new ImportTool().parseArguments(
        optionArgs, getConf(), getSqoopOptions(getConf()), true);
      assertEquals(RowCountValidator.class,
        validationOptions.getValidatorClass());
      assertEquals(AbsoluteValidationThreshold.class,
        validationOptions.getValidationThresholdClass());
      assertEquals(AbortOnFailureHandler.class,
        validationOptions.getValidationFailureHandlerClass());
    } catch (Exception e) {
      fail("The validation options are passed correctly: " + e.getMessage());
    } finally {
      dropTableIfExists(getTableName());
    }
  }

  public void testInvalidValidationOptions() throws Exception {
    String[] types = {"INT NOT NULL PRIMARY KEY", "VARCHAR(32)", "VARCHAR(32)"};
    String[] insertVals = {"1", "'Bob'", "'sales'"};

    try {
      createTableWithColTypes(types, insertVals);

      String[] args = getArgv(true, null, getConf());
      ArrayList<String> argsList = new ArrayList<String>();
      argsList.add("--validator");
      argsList.add("org.apache.sqoop.validation.NullValidator");
      argsList.add("--validation-threshold");
      argsList.add("org.apache.sqoop.validation.NullValidationThreshold");
      argsList.add("--validation-failurehandler");
      argsList.add("org.apache.sqoop.validation.NullFailureHandler");
      Collections.addAll(argsList, args);

      String[] optionArgs = toStringArray(argsList);

      new ImportTool().parseArguments(optionArgs, getConf(),
        getSqoopOptions(getConf()), true);
      fail("The validation options are incorrect and must throw an exception");
    } catch (Exception e) {
      System.out.println("e.getMessage() = " + e.getMessage());
      System.out.println("e.getClass() = " + e.getClass());
      assertEquals(
        com.cloudera.sqoop.SqoopOptions.InvalidOptionsException.class,
        e.getClass());
    } finally {
      dropTableIfExists(getTableName());
    }
  }

  private String[] toStringArray(ArrayList<String> argsList) {
    String[] optionArgs = new String[argsList.size()];
    for (int i = 0; i < argsList.size(); i++) {
      optionArgs[i] = argsList.get(i);
    }
    return optionArgs;
  }

  /**
   * Negative case where the row counts do NOT match.
   */
  public void testValidatorWithDifferentRowCounts() {
    try {
      Validator validator = new RowCountValidator();
      validator.validate(new ValidationContext(100, 90));
      fail("FailureHandler should have thrown an exception");
    } catch (ValidationException e) {
      assertEquals("Validation failed by RowCountValidator. "
        + "Reason: The expected counter value was 100 but the actual value "
        + "was 90, Row Count at Source: 100, Row Count at Target: 90",
        e.getMessage());
    }
  }

  /**
   * Positive case where the row counts match.
   */
  public void testValidatorWithMatchingRowCounts() {
    try {
      Validator validator = new RowCountValidator();
      validator.validate(new ValidationContext(100, 100));
    } catch (ValidationException e) {
      fail("FailureHandler should NOT have thrown an exception");
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
