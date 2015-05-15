/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.connector.ftp.configuration;

import org.apache.sqoop.validation.ConfigValidationResult;
import org.apache.sqoop.validation.ConfigValidationRunner;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link org.apache.sqoop.connector.ftp.configuration.ToJobConfiguration} class.
 */
public class TestToJobConfiguration {

  /**
   * Test a non-empty directory name.
   */
  @Test
  public void testValidDirectory() {
    ConfigValidationRunner runner = new ConfigValidationRunner();
    ConfigValidationResult result;
    ToJobConfiguration config = new ToJobConfiguration();
    config.toJobConfig.outputDirectory = "testdir";
    result = runner.validate(config);
    Assert.assertTrue(result.getStatus().canProceed(),
                      "Test of valid directory failed");
  }

  /**
   * Test an invalid, empty directory name.
   */
  @Test
  public void testEmptyDirectory() {
    ConfigValidationRunner runner = new ConfigValidationRunner();
    ConfigValidationResult result;
    ToJobConfiguration config = new ToJobConfiguration();
    config.toJobConfig.outputDirectory = "";
    result = runner.validate(config);
    Assert.assertFalse(result.getStatus().canProceed(),
                       "Test of empty directory failed");
  }
}
