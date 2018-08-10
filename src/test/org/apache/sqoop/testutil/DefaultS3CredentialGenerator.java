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

package org.apache.sqoop.testutil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class DefaultS3CredentialGenerator extends S3CredentialGenerator {

    public static final Log LOG = LogFactory.getLog(
            DefaultS3CredentialGenerator.class.getName());

    /**
     * By calling this constructor the {@link #generateS3Credentials(String)} method is called and the values of the
     * returned map are assigned to the corresponding fields of the instantiated class.
     *
     * @param generatorCommand String containing the command to generate S3 credentials
     * @throws IOException
     */
    public DefaultS3CredentialGenerator(String generatorCommand) throws IOException {
        super(generatorCommand);
    }

    @Override
    /**
     * Executes the given command under /bin/sh and reads space separated S3 credentials from
     * the first line of standard output in the following order: access key, secret key and session token
     * (the latter one only in case of temporary credentials).
     *
     * @param {@inheritDoc}
     * @return {@inheritDoc}
     * @throws {@inheritDoc}
     */
    protected Map<String, String> generateS3Credentials(String generatorCommand) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", generatorCommand);
        Process process = processBuilder.start();
        String output;
        Map<String, String> credentials = new HashMap<String, String>();

        try (
                InputStreamReader inputStreamReader = new InputStreamReader(process.getInputStream(), Charset.forName("UTF-8"));
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        ) {
            output =  bufferedReader.readLine();

            if (output != null) {

                String[] splitOutput = output.split(" ");

                credentials.put(S3_ACCESS_KEY, splitOutput[0]);
                credentials.put(S3_SECRET_KEY, splitOutput[1]);

                if (splitOutput.length > 2) {
                    credentials.put(S3_SESSION_TOKEN, splitOutput[2]);
                }

            } else {
                LOG.info("No S3 credential generator command is given or output of the command is null thus S3 tests are being skipped.");
            }

        } catch (IOException ioE) {
            LOG.error("Issue with generating S3 credentials", ioE);
            throw new RuntimeException(ioE);
        }

        return credentials;
    }
}
