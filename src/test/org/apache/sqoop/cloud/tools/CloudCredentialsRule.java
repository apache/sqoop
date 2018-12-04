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

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.junit.Assume.assumeTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialShell;
import org.apache.hadoop.util.ToolRunner;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class CloudCredentialsRule implements TestRule {

  protected final Map<String, String> credentialsMap;

  private final CredentialGenerator credentialGenerator;

  protected CloudCredentialsRule() {
    this(new CredentialGenerator());
  }

  public CloudCredentialsRule(CredentialGenerator credentialGenerator) {
    this.credentialGenerator = credentialGenerator;
    this.credentialsMap = new HashMap<>();
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        assumeTrue(isNotBlank(getGeneratorCommand()));

        setupCredentials();
        base.evaluate();
      }
    };
  }

  public void fillCredentialProvider(Configuration conf, String providerPath) {
    credentialsMap.forEach(
        (key, value) -> runCredentialProviderCreateCommand(getCreateCommand(key, value, providerPath), conf)
    );
  }

  protected void setupCredentials() throws IOException {
    Iterable<String> credentials = credentialGenerator.invokeGeneratorCommand(getGeneratorCommand());

    initializeCredentialsMap(credentials);
  }

  private void runCredentialProviderCreateCommand(String command, Configuration conf) {
    try {
      ToolRunner.run(conf, new CredentialShell(), command.split(" "));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String getCreateCommand(String credentialKey, String credentialValue, String providerPath) {
    return "create " + credentialKey + " -value " + credentialValue + " -provider " + providerPath;
  }

  public abstract void addCloudCredentialProperties(Configuration hadoopConf);

  public abstract void addCloudCredentialProperties(ArgumentArrayBuilder builder);

  public abstract void addCloudCredentialProviderProperties(ArgumentArrayBuilder builder);

  public abstract String getBaseCloudDirectoryUrl();

  protected abstract void initializeCredentialsMap(Iterable<String> credentials);

  protected abstract String getGeneratorCommand();
}
