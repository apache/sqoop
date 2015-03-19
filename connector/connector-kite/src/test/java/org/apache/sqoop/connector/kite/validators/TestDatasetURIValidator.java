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
package org.apache.sqoop.connector.kite.validators;

import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.validators.AbstractValidator;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 */
public class TestDatasetURIValidator {

  AbstractValidator validator = new DatasetURIValidator();

  @BeforeMethod(alwaysRun = true)
  public void resetValidator() {
    validator.reset();
  }

  @Test
  public void testNull() {
    validator.validate(null);
    assertEquals(validator.getStatus(), Status.ERROR);
    assertEquals(validator.getMessages().size(), 1);
  }

  @Test
  public void testEmpty() {
    validator.validate("");
    assertEquals(validator.getStatus(), Status.ERROR);
    assertEquals(validator.getMessages().size(), 1);
  }

  @Test
  public void testRandomString() {
    validator.validate("!@#$%");
    assertEquals(validator.getStatus(), Status.ERROR);
    assertEquals(validator.getMessages().size(), 1);
  }

  @Test
  public void testHdfsDataset() {
    validator.validate("dataset:hive:");
    assertEquals(validator.getStatus(), Status.OK);
    assertEquals(validator.getMessages().size(), 0);
  }

  @Test
  public void testHiveDataset() {
    validator.validate("dataset:hive:");
    assertEquals(validator.getStatus(), Status.OK);
    assertEquals(validator.getMessages().size(), 0);
  }

  @Test
  public void testFileDataset() {
    validator.validate("dataset:file:");
    assertEquals(validator.getStatus(), Status.OK);
    assertEquals(validator.getMessages().size(), 0);
  }
}
