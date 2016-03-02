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
package org.apache.sqoop.test.kdc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL;
import org.apache.sqoop.client.SqoopClient;

import java.net.URL;
import java.util.Map;

/**
 * Special instance of KdcRunner that allows us to not use kerberos
 */
public class NoKdcRunner extends KdcRunner {
  @Override
  public Configuration prepareHadoopConfiguration(Configuration config) throws Exception {
    return config;
  }

  @Override
  public Map<String, String> prepareSqoopConfiguration(Map<String, String> properties) {
    return properties;
  }

  @Override
  public void start() throws Exception {
    // Do nothing
  }

  @Override
  public void stop() throws Exception {
    // Do nothing
  }

  @Override
  public void authenticateWithSqoopServer(SqoopClient client) throws Exception {
    // Do nothing
  }

  @Override
  public void authenticateWithSqoopServer(URL url, DelegationTokenAuthenticatedURL.Token authToken) throws Exception {
    // Do nothing
  }
}
