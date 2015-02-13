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

import java.util.List;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RolesBean extends RoleBean {

  private static final String ROLES = "roles";

  // For "extract"
  public RolesBean(MRole role) {
    super(role);
  }

  public RolesBean(List<MRole> roles) {
    super(roles);

  }

  // For "restore"
  public RolesBean() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public JSONObject extract(boolean skipSensitive) {
    JSONArray rolesArray = super.extractRoles();
    JSONObject roles = new JSONObject();
    roles.put(ROLES, rolesArray);
    return roles;
  }

  @Override
  public void restore(JSONObject json) {
    JSONArray rolesArray = (JSONArray) json.get(ROLES);
    restoreRoles(rolesArray);
  }

}
