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

import java.io.IOException;
import java.util.Map;

public abstract class S3CredentialGenerator {

    protected static final String S3_ACCESS_KEY = "S3_ACCESS_KEY";
    protected static final String S3_SECRET_KEY = "S3_SECRET_KEY";
    protected static final String S3_SESSION_TOKEN = "S3_SESSION_TOKEN";

    protected String s3AccessKey;
    protected String s3SecretKey;
    protected String s3SessionToken;

    /**
     * By calling this constructor the {@link #generateS3Credentials(String)} method is called and the values of the
     * returned map are assigned to the corresponding fields of the instantiated class.
     *
     * @param generatorCommand String containing the command to generate S3 credentials
     * @throws IOException
     */
    public S3CredentialGenerator(String generatorCommand) throws IOException {
        Map<String, String> s3Credentials = generateS3Credentials(generatorCommand);
        if (s3Credentials != null) {
            s3AccessKey = s3Credentials.get(S3_ACCESS_KEY);
            s3SecretKey = s3Credentials.get(S3_SECRET_KEY);
            s3SessionToken = s3Credentials.get(S3_SESSION_TOKEN);
        }
    }

    /**
     * Executes the given S3 credential generator command and builds a map containing the credentials
     *
     * @param generatorCommand String containing the command to execute
     * @return Map containing S3 credentials by keys {@link #S3_ACCESS_KEY}, {@link #S3_SECRET_KEY} and {@link #S3_SESSION_TOKEN}
     * @throws IOException
     */
    protected abstract Map<String, String> generateS3Credentials(String generatorCommand) throws IOException;

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public String getS3SecretKey() {
        return s3SecretKey;
    }

    public String getS3SessionToken() {
        return s3SessionToken;
    }

}
