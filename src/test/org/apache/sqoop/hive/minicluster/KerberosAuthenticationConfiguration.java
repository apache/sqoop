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

package org.apache.sqoop.hive.minicluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.sqoop.infrastructure.kerberos.KerberosConfigurationProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class KerberosAuthenticationConfiguration implements AuthenticationConfiguration {

  private final KerberosConfigurationProvider kerberosConfig;

  public KerberosAuthenticationConfiguration(KerberosConfigurationProvider kerberosConfig) {
    this.kerberosConfig = kerberosConfig;
  }

  private void authenticate() {
    try {
      Configuration conf = new Configuration();
      conf.set("hadoop.security.authentication", "Kerberos");
      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation.loginUserFromKeytab(kerberosConfig.getTestPrincipal(), kerberosConfig.getKeytabFilePath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, String> getAuthenticationConfig() {
    authenticate();

    Map<String, String> result = new HashMap<>();

    result.put(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL.varname, kerberosConfig.getTestPrincipal());
    result.put(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB.varname, kerberosConfig.getKeytabFilePath());
    result.put(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION.varname, HiveAuthFactory.AuthTypes.KERBEROS.toString());
    result.put(YarnConfiguration.RM_PRINCIPAL, kerberosConfig.getTestPrincipal());

    return result;
  }

  @Override
  public String getUrlParams() {
    return ";principal=" + kerberosConfig.getTestPrincipal() + "@" + kerberosConfig.getRealm();
  }

}
