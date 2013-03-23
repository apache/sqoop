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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class VersionBean implements JsonBean {

  public static final String VERSION = "version";
  public static final String REVISION = "revision";
  public static final String DATE = "date";
  public static final String USER = "user";
  public static final String URL = "url";
  public static final String PROTOCOLS = "protocols";

  private String version;
  private String revision;
  private String date;
  private String user;
  private String url;
  private String[] protocols;

  // for "extract"
  public VersionBean(String version, String revision, String date,
      String user, String url, String[] protocols) {
    this.version = version;
    this.revision = revision;
    this.date = date;
    this.user = user;
    this.url = url;
    this.protocols = new String[protocols.length];
    System.arraycopy(protocols, 0, this.protocols, 0, protocols.length);
  }

  // for "restore"
  public VersionBean() {
  }

  @SuppressWarnings("unchecked")
  @Override
  public JSONObject extract(boolean skipSensitive) {
    JSONObject result = new JSONObject();
    result.put(VERSION, version);
    result.put(REVISION, revision);
    result.put(DATE, date);
    result.put(USER, user);
    result.put(URL, url);
    JSONArray protocolsArray = new JSONArray();
    for (String protocol : protocols) {
      protocolsArray.add(protocol);
    }
    result.put(PROTOCOLS, protocolsArray);
    return result;
  }

  @Override
  public void restore(JSONObject jsonObject) {
    this.version = (String)jsonObject.get(VERSION);
    this.revision = (String)jsonObject.get(REVISION);
    this.date = (String)jsonObject.get(DATE);
    this.user = (String)jsonObject.get(USER);
    this.url = (String)jsonObject.get(URL);
    JSONArray protocolsArray = (JSONArray) jsonObject.get(PROTOCOLS);
    int size = protocolsArray.size();
    this.protocols = new String[size];
    for (int i = 0; i<size; i++) {
      protocols[i] = (String) protocolsArray.get(i);
    }
  }

  public String getVersion() {
    return this.version;
  }

  public String getRevision() {
    return this.revision;
  }

  public String getDate() {
    return this.date;
  }

  public String getUser() {
    return this.user;
  }

  public String getUrl() {
    return this.url;
  }

  public String[] getProtocols() {
    return this.protocols;
  }

}
