/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.cloud.tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;

public class CredentialGenerator {

  public static final Log LOG = LogFactory.getLog(CredentialGenerator.class.getName());

  public Iterable<String> invokeGeneratorCommand(String generatorCommand) throws IOException {
    ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", generatorCommand);
    Process process = processBuilder.start();

    try (
        InputStreamReader inputStreamReader = new InputStreamReader(process.getInputStream(), Charset.forName("UTF-8"));
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
    ) {
      String output = bufferedReader.readLine();

      return Arrays.asList(output.split(" "));
    } catch (IOException ioE) {
      LOG.error("Issue invoking generating credentials", ioE);
      throw new RuntimeException(ioE);
    }
  }
}
