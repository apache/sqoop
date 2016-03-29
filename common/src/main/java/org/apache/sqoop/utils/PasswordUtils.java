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

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.PasswordError;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public class PasswordUtils {
  private static final Logger LOG = Logger.getLogger(PasswordUtils.class);

  /**
   * Attempts to read a password from the configuration via plaintext or a generator.
   *
   * First, we attempt to read the plaintext password, if that value does not exist
   * we will look for and try to run a configured password generator.
   *
   * If a password cannot be found and a password generator cannot be found, an
   * exception is thrown.
   *
   * If both a password and a password generator are set, we prefer the password.
   *
   * @param configurationContext MapContext holding the sqoop configuration
   * @param passwordProperty String containing the property that maps to the
   *                         plaintext version of the password
   * @param passwordGeneratorProperty String containing the property that maps to
   *                                  the generator that prints the password to
   *                                  standard out
   * @return A String password value, null if neither password property is set
   * @throws SqoopException
   */
  public static String readPassword(MapContext configurationContext, String passwordProperty, String passwordGeneratorProperty) {
    String plaintextPassword = configurationContext.getString(passwordProperty);
    String passwordGenerator = configurationContext.getString(passwordGeneratorProperty);

    if (StringUtils.isNotBlank(plaintextPassword)) {
      if (StringUtils.isNotBlank(passwordGenerator)) {
        LOG.warn(passwordProperty + " and " + passwordGeneratorProperty + " are both set, using " + passwordProperty);
      }
      return plaintextPassword;
    } else if (StringUtils.isNotBlank(passwordGenerator)) {
      try {
        String password = PasswordUtils.readOutputFromGenerator(passwordGenerator);

        if (StringUtils.isNotBlank(password)) {
          return password;
        } else {
          throw new SqoopException(PasswordError.PASSWORD_0001);
        }
      } catch (IOException exception) {
        throw new SqoopException(PasswordError.PASSWORD_0000, exception);
      }
    } else {
      return null;
    }
  }

  /**
   * Executes the given command under /bin/sh and reads one line of standard output
   *
   * @param generatorCommand String containing the command to execute
   * @return The first line of standard output from the generator command
   * @throws IOException
   */
  public static String readOutputFromGenerator(String generatorCommand) throws IOException {
    ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", generatorCommand);
    Process process = processBuilder.start();
    String output;
    try (
      InputStreamReader inputStreamReader = new InputStreamReader(process.getInputStream(), Charset.forName("UTF-8"));
      BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
    ) {
      output =  bufferedReader.readLine();
    }
    return output;
  }
}
