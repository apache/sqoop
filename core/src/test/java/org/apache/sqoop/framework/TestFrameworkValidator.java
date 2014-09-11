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
package org.apache.sqoop.framework;

import org.apache.sqoop.framework.configuration.ConnectionConfiguration;
import org.apache.sqoop.framework.configuration.JobConfiguration;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.ValidationResult;
import org.apache.sqoop.validation.ValidationRunner;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestFrameworkValidator {

  FrameworkValidator validator;

  @Before
  public void setUp() {
    validator = new FrameworkValidator();
  }

  @Test
  public void testConnectionValidation() {
    ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration();
    ValidationRunner runner = new ValidationRunner();
    ValidationResult result = runner.validate(connectionConfiguration);
    assertEquals(Status.FINE, result.getStatus());
    assertEquals(0, result.getMessages().size());
  }

  @Test
  public void testJobValidation() {
    ValidationRunner runner = new ValidationRunner();
    ValidationResult result;
    JobConfiguration configuration;

    // Empty form is allowed
    configuration = new JobConfiguration();
    result = runner.validate(configuration);
    assertEquals(Status.FINE, result.getStatus());

    // Explicitly setting extractors and loaders
    configuration = new JobConfiguration();
    configuration.throttling.extractors = 3;
    configuration.throttling.loaders = 3;
    result = runner.validate(configuration);
    assertEquals(Status.FINE, result.getStatus());
    assertEquals(0, result.getMessages().size());

    // Negative and zero values for extractors and loaders
//    configuration = new JobConfiguration();
//    configuration.throttling.extractors = 0;
//    configuration.throttling.loaders = -1;
//    result = runner.validate(configuration);
//    assertEquals(Status.FINE, result.getStatus());
//    assertTrue(result.getMessages().containsKey("throttling.extractors"));
//    assertTrue(result.getMessages().containsKey("throttling.loaders"));
  }
}
