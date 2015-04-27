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
package org.apache.sqoop.security.authentication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.sqoop.security.AuthenticationHandler;
import org.apache.sqoop.security.SecurityConstants;

public class SimpleAuthenticationHandler extends AuthenticationHandler {

  private static final Logger LOG = Logger.getLogger(SimpleAuthenticationHandler.class);

  public void doInitialize() {
    securityEnabled = false;
  }

  public void secureLogin() {
    //no secureLogin, just set configurations
    Configuration conf = new Configuration();
    conf.set(get_hadoop_security_authentication(),
            SecurityConstants.TYPE.SIMPLE.name());
    UserGroupInformation.setConfiguration(conf);
    LOG.info("Using simple/pseudo authentication, principal ["
            + System.getProperty("user.name") + "]");
  }
}