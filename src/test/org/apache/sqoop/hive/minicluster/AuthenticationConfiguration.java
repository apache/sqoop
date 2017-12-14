package org.apache.sqoop.hive.minicluster;

import java.util.Map;

public interface AuthenticationConfiguration {

  Map<String, String> getAuthenticationConfig();

  String getUrlParams();

}
