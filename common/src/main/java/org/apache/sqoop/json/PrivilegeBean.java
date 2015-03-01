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
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PrivilegeBean implements JsonBean {

  public static final String PRIVILEGE = "privilege";
  private static final String RESOURCE_NAME = "resource-name";
  private static final String RESOURCE_TYPE = "resource-type";
  private static final String ACTION = "action";
  private static final String WITH_GRANT_OPTION = "with-grant-option";

  private List<MPrivilege> privileges;

  public List<MPrivilege> getPrivileges() {
    return privileges;
  }

  // For "extract"
  public PrivilegeBean(MPrivilege privilege) {
    this();
    this.privileges = new ArrayList<MPrivilege>();
    this.privileges.add(privilege);
  }

  public PrivilegeBean(List<MPrivilege> privileges) {
    this();
    this.privileges = privileges;
  }

  // For "restore"
  public PrivilegeBean() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public JSONObject extract(boolean skipSensitive) {
    JSONObject privilege = new JSONObject();
    privilege.put(PRIVILEGE, extractPrivilege(privileges.get(0)));
    return privilege;
  }

  @SuppressWarnings("unchecked")
  protected JSONArray extractPrivileges() {
    JSONArray privilegesArray = new JSONArray();
    if (privileges != null) {
      for (MPrivilege privilege : privileges) {
        privilegesArray.add(extractPrivilege(privilege));
      }
    }
    return privilegesArray;
  }

  @SuppressWarnings("unchecked")
  private JSONObject extractPrivilege(MPrivilege privilege) {
    JSONObject object = new JSONObject();
    object.put(RESOURCE_NAME, privilege.getResource().getName());
    object.put(RESOURCE_TYPE, privilege.getResource().getType());
    object.put(ACTION, privilege.getAction());
    object.put(WITH_GRANT_OPTION, privilege.isWith_grant_option());
    return object;
  }

  @Override
  public void restore(JSONObject json) {
    privileges = new ArrayList<MPrivilege>();
    JSONObject obj = (JSONObject) json.get(PRIVILEGE);
    privileges.add(restorePrivilege(obj));
  }

  protected void restorePrivileges(JSONArray array) {
    privileges = new ArrayList<MPrivilege>();
    for (Object obj : array) {
      privileges.add(restorePrivilege(obj));
    }
  }

  private MPrivilege restorePrivilege(Object obj) {
    JSONObject object = (JSONObject) obj;
    MResource resource = new MResource(
            (String) object.get(RESOURCE_NAME), (String) object.get(RESOURCE_TYPE));
    return new MPrivilege(resource, (String) object.get(ACTION),
            Boolean.valueOf(object.get(WITH_GRANT_OPTION).toString()));
  }
}
