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
package org.apache.sqoop.utils;

import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestPasswordUtils {

  private static String PASSWORD_KEY = "PASSWORD_KEY";
  private static String PASSWORD_GENERATOR_KEY = "PASSWORD_GENERATOR_KEY";

  private static String PASSWORD = "password";
  private static String PASSWORD_FROM_GENERATOR = "password_from_generator";
  private static String PASSWORD_GENERATOR = "echo " + PASSWORD_FROM_GENERATOR;

  @Test
  public void passwordExistsAndPasswordGeneratorExists() {
    Map<String, String> passwordMap = new HashMap<>();
    passwordMap.put(PASSWORD_KEY, PASSWORD);
    passwordMap.put(PASSWORD_GENERATOR_KEY, PASSWORD_GENERATOR);

    assertEquals(
      PasswordUtils.readPassword(new MapContext(passwordMap), PASSWORD_KEY, PASSWORD_GENERATOR_KEY),
      PASSWORD);
  }

  @Test
  public void passwordExistsAndPasswordGeneratorDoesNotExist() {
    Map<String, String> passwordMap = new HashMap<>();
    passwordMap.put(PASSWORD_KEY, PASSWORD);

    assertEquals(
      PasswordUtils.readPassword(new MapContext(passwordMap), PASSWORD_KEY, PASSWORD_GENERATOR_KEY),
      PASSWORD);
  }

  @Test
  public void passwordDoesNotExistAndPasswordGeneratorExists() {
    Map<String, String> passwordMap = new HashMap<>();
    passwordMap.put(PASSWORD_GENERATOR_KEY, PASSWORD_GENERATOR);

    assertEquals(
      PasswordUtils.readPassword(new MapContext(passwordMap), PASSWORD_KEY, PASSWORD_GENERATOR_KEY),
      PASSWORD_FROM_GENERATOR);
  }

  @Test
  public void passwordDoesNotExistAndPasswordGeneratorDoesNotExist() {
    Map<String, String> passwordMap = new HashMap<>();

    assertNull(PasswordUtils.readPassword(new MapContext(passwordMap), PASSWORD_KEY, PASSWORD_GENERATOR_KEY));
  }

  @Test(
    expectedExceptions = {SqoopException.class},
    expectedExceptionsMessageRegExp = ".*No password returned from generator")
  public void passwordGeneratorFailsToExecute() {
    Map<String, String> passwordMap = new HashMap<>();
    passwordMap.put(PASSWORD_GENERATOR_KEY, "ISUREDOHOPEYOUDONTHAVESOMETHINGWITHTHISNAMEINYOURPATH");

    PasswordUtils.readPassword(new MapContext(passwordMap), PASSWORD_KEY, PASSWORD_GENERATOR_KEY);
  }
}
