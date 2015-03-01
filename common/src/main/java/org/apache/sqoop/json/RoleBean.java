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
import org.apache.sqoop.model.MRole;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RoleBean implements JsonBean {

  public static final String ROLE = "role";
  private static final String NAME = "name";

  private List<MRole> roles;

  public List<MRole> getRoles() {
    return roles;
  }

  // For "extract"
  public RoleBean(MRole role) {
    this();
    this.roles = new ArrayList<MRole>();
    this.roles.add(role);
  }

  public RoleBean(List<MRole> roles) {
    this();
    this.roles = roles;
  }

  // For "restore"
  public RoleBean() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public JSONObject extract(boolean skipSensitive) {
    JSONObject role = new JSONObject();
    role.put(ROLE, extractRole(roles.get(0)));
    return role;
  }

  @SuppressWarnings("unchecked")
  protected JSONArray extractRoles() {
    JSONArray rolesArray = new JSONArray();
    if (roles != null) {
      for (MRole role : roles) {
        rolesArray.add(extractRole(role));
      }
    }
    return rolesArray;
  }

  @SuppressWarnings("unchecked")
  private JSONObject extractRole(MRole role) {
    JSONObject object = new JSONObject();
    object.put(NAME, role.getName());
    return object;
  }

  @Override
  public void restore(JSONObject json) {
    roles = new ArrayList<MRole>();
    JSONObject obj = (JSONObject) json.get(ROLE);
    roles.add(restoreRole(obj));
  }

  protected void restoreRoles(JSONArray array) {
    roles = new ArrayList<MRole>();
    for (Object obj : array) {
      roles.add(restoreRole(obj));
    }
  }

  private MRole restoreRole(Object obj) {
    JSONObject object = (JSONObject) obj;
    return new MRole((String) object.get(NAME));
  }
}
