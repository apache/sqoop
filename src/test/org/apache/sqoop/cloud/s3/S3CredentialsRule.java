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

package org.apache.sqoop.cloud.s3;

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;
import static org.apache.hadoop.fs.s3a.Constants.SESSION_TOKEN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider;
import org.apache.sqoop.cloud.tools.CloudCredentialsRule;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;

import java.util.Iterator;

public class S3CredentialsRule extends CloudCredentialsRule {

  private static final String PROPERTY_GENERATOR_COMMAND = "s3.generator.command";

  private static final String PROPERTY_BUCKET_URL = "s3.bucket.url";

  @Override
  public void addCloudCredentialProperties(Configuration hadoopConf) {
    hadoopConf.set(ACCESS_KEY, credentialsMap.get(ACCESS_KEY));
    hadoopConf.set(SECRET_KEY, credentialsMap.get(SECRET_KEY));

    if (credentialsMap.containsKey(SESSION_TOKEN)) {
      hadoopConf.set(SESSION_TOKEN, credentialsMap.get(SESSION_TOKEN));
      hadoopConf.set(Constants.AWS_CREDENTIALS_PROVIDER, TemporaryAWSCredentialsProvider.class.getName());
    }

    // Default filesystem needs to be set to S3 for the output verification phase
    hadoopConf.set("fs.defaultFS", getBaseCloudDirectoryUrl());

    // FileSystem has a static cache that should be disabled during tests to make sure
    // Sqoop relies on the S3 credentials set via the -D system properties.
    // For details please see SQOOP-3383
    hadoopConf.setBoolean("fs.s3a.impl.disable.cache", true);
  }

  @Override
  public void addCloudCredentialProperties(ArgumentArrayBuilder builder) {
    builder
        .withProperty(Constants.ACCESS_KEY, credentialsMap.get(ACCESS_KEY))
        .withProperty(Constants.SECRET_KEY, credentialsMap.get(SECRET_KEY));
    if (credentialsMap.containsKey(SESSION_TOKEN)) {
      builder.withProperty(Constants.SESSION_TOKEN, credentialsMap.get(SESSION_TOKEN))
          .withProperty(Constants.AWS_CREDENTIALS_PROVIDER, TemporaryAWSCredentialsProvider.class.getName());
    }
  }

  @Override
  public void addCloudCredentialProviderProperties(ArgumentArrayBuilder builder) {
    builder.withProperty("fs.s3a.impl.disable.cache", "true");
    if (credentialsMap.containsKey(SESSION_TOKEN)) {
      builder.withProperty(Constants.AWS_CREDENTIALS_PROVIDER, TemporaryAWSCredentialsProvider.class.getName());
    }
  }

  @Override
  public String getBaseCloudDirectoryUrl() {
    String propertyBucketUrl = System.getProperty(PROPERTY_BUCKET_URL);
    return propertyBucketUrl.concat("/");
  }

  @Override
  protected void initializeCredentialsMap(Iterable<String> credentials) {
    Iterator<String> credentialsIterator = credentials.iterator();

    credentialsMap.put(ACCESS_KEY, credentialsIterator.next());
    credentialsMap.put(SECRET_KEY, credentialsIterator.next());
    if (credentialsIterator.hasNext()) {
      credentialsMap.put(SESSION_TOKEN, credentialsIterator.next());
    }
  }

  @Override
  protected String getGeneratorCommand() {
    return System.getProperty(PROPERTY_GENERATOR_COMMAND);
  }
}
