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
import org.apache.sqoop.model.MPrincipal;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PrincipalBean implements JsonBean {

  public static final String PRINCIPAL = "principal";
  private static final String NAME = "name";
  private static final String TYPE = "type";

  private List<MPrincipal> principals;

  public List<MPrincipal> getPrincipals() {
    return principals;
  }

  // For "extract"
  public PrincipalBean(MPrincipal principal) {
    this();
    this.principals = new ArrayList<MPrincipal>();
    this.principals.add(principal);
  }

  public PrincipalBean(List<MPrincipal> principals) {
    this();
    this.principals = principals;
  }

  // For "restore"
  public PrincipalBean() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public JSONObject extract(boolean skipSensitive) {
    JSONObject principal = new JSONObject();
    principal.put(PRINCIPAL, extractPrincipal(principals.get(0)));
    return principal;
  }

  @SuppressWarnings("unchecked")
  protected JSONArray extractPrincipals() {
    JSONArray principalsArray = new JSONArray();
    if (principals != null) {
      for (MPrincipal principal : principals) {
        principalsArray.add(extractPrincipal(principal));
      }
    }
    return principalsArray;
  }

  @SuppressWarnings("unchecked")
  private JSONObject extractPrincipal(MPrincipal principal) {
    JSONObject object = new JSONObject();
    object.put(NAME, principal.getName());
    object.put(TYPE, principal.getType());
    return object;
  }

  @Override
  public void restore(JSONObject json) {
    principals = new ArrayList<MPrincipal>();
    JSONObject obj = (JSONObject) json.get(PRINCIPAL);
    principals.add(restorePrincipal(obj));
  }

  protected void restorePrincipals(JSONArray array) {
    principals = new ArrayList<MPrincipal>();
    for (Object obj : array) {
      principals.add(restorePrincipal(obj));
    }
  }

  private MPrincipal restorePrincipal(Object obj) {
    JSONObject object = (JSONObject) obj;
    return new MPrincipal((String) object.get(NAME), (String) object.get(TYPE));
  }
}
