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
    authenticate();
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
