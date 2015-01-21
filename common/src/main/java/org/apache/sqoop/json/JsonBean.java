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
import org.json.simple.JSONObject;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface JsonBean {

  // common JSON constants for the rest-api response
  static final String CONFIGURABLE_VERSION = "version";
  static final String ALL_CONFIGS= "all-config-resources";

  @Deprecated // should not be used anymore in the rest api
  static final String ALL = "all";
  static final String ID = "id";
  static final String NAME = "name";
  static final String CLASS = "class";
  static final String ENABLED = "enabled";
  static final String CREATION_USER = "creation-user";
  static final String CREATION_DATE = "creation-date";
  static final String UPDATE_USER = "update-user";
  static final String UPDATE_DATE = "update-date";

  JSONObject extract(boolean skipSensitive);

  void restore(JSONObject jsonObject);

  public static final JsonBean EMPTY_BEAN = new JsonBean() {

    @Override
    public JSONObject extract(boolean skipSensitive) {
      return new JSONObject();
    }

    @Override
    public void restore(JSONObject jsonObject) {
    }
  };
}
