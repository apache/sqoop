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
package org.apache.sqoop.json;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class VersionBean implements JsonBean {

  public static final String BUILD_VERSION = "build-version";
  public static final String SOURCE_REVISION = "source-revision";
  public static final String BUILD_DATE = "build-date";
  public static final String SYSTEM_USER_NAME = "user";
  public static final String SOURCE_URL = "source-url";
  public static final String SUPPORTED_API_VERSIONS = "api-versions";

  private String buildVersion;
  private String sourceRevision;
  private String buildDate;
  private String systemUser;
  private String sourceUrl;
  private String[] supportedRestAPIVersions;

  // for "extract"
  public VersionBean(String serverVersion, String sourceRevision, String buildDate,
      String user, String sourceUrl, String[] supportedAPIVersions) {
    this.buildVersion = serverVersion;
    this.sourceRevision = sourceRevision;
    this.buildDate = buildDate;
    this.systemUser = user;
    this.sourceUrl = sourceUrl;
    this.supportedRestAPIVersions = new String[supportedAPIVersions.length];
    System.arraycopy(supportedAPIVersions, 0, this.supportedRestAPIVersions, 0, supportedAPIVersions.length);
  }

  // for "restore"
  public VersionBean() {
  }

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject extract(boolean skipSensitive) {
    JSONObject result = new JSONObject();
    result.put(BUILD_VERSION, buildVersion);
    result.put(SOURCE_REVISION, sourceRevision);
    result.put(BUILD_DATE, buildDate);
    result.put(SYSTEM_USER_NAME, systemUser);
    result.put(SOURCE_URL, sourceUrl);
    JSONArray apiVersionsArray = new JSONArray();
    for (String version : supportedRestAPIVersions) {
      apiVersionsArray.add(version);
    }
    result.put(SUPPORTED_API_VERSIONS, apiVersionsArray);
    return result;
  }

  @Override
  public void restore(JSONObject jsonObject) {
    this.buildVersion = (String)jsonObject.get(BUILD_VERSION);
    this.sourceRevision = (String)jsonObject.get(SOURCE_REVISION);
    this.buildDate = (String)jsonObject.get(BUILD_DATE);
    this.systemUser = (String)jsonObject.get(SYSTEM_USER_NAME);
    this.sourceUrl = (String)jsonObject.get(SOURCE_URL);
    JSONArray apiVersionsArray = (JSONArray) jsonObject.get(SUPPORTED_API_VERSIONS);
    int size = apiVersionsArray.size();
    this.supportedRestAPIVersions = new String[size];
    for (int i = 0; i<size; i++) {
      supportedRestAPIVersions[i] = (String) apiVersionsArray.get(i);
    }
  }

  public String getBuildVersion() {
    return this.buildVersion;
  }

  public String getSourceRevision() {
    return this.sourceRevision;
  }

  public String getBuildDate() {
    return this.buildDate;
  }

  public String getSystemUser() {
    return this.systemUser;
  }

  public String getSourceUrl() {
    return this.sourceUrl;
  }

  public String[] getSupportedAPIVersions() {
    return this.supportedRestAPIVersions;
  }

}
