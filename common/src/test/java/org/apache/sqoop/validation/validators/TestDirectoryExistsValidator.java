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
package org.apache.sqoop.validation.validators;

import com.google.common.io.Files;
import org.apache.sqoop.validation.Status;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.assertEquals;

/**
 */
public class TestDirectoryExistsValidator {

  AbstractValidator<String> validator = new DirectoryExistsValidator();

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    validator.reset();
    assertEquals(0, validator.getMessages().size());
  }

  @Test
  public void testNull() {
    validator.validate(null);
    assertEquals(Status.OK, validator.getStatus());
  }

  @Test
  public void testEmpty() {
    validator.validate("");
    assertEquals(Status.OK, validator.getStatus());
  }


  @Test
  public void testExistingDirectory() {
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();

    validator.validate(tmpDir.getAbsolutePath());
    assertEquals(Status.OK, validator.getStatus());
  }

  @Test
  public void testFileInsteadOfDirectory() throws Exception {
    File tmpDir = File.createTempFile("ABCDEFG", "12345");
    tmpDir.createNewFile();
    tmpDir.deleteOnExit();

    validator.validate(tmpDir.getAbsolutePath());
    assertEquals(Status.ERROR, validator.getStatus());
    assertEquals(1, validator.getMessages().size());
  }

  @Test
  public void testNonExistingPath() throws Exception {
    validator.validate("/X/Y/Z/This/Can/Not/Exists/Right/Question-mark");
    assertEquals(Status.ERROR, validator.getStatus());
    assertEquals(1, validator.getMessages().size());
  }
}
