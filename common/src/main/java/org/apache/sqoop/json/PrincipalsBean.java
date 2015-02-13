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

import java.util.List;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PrincipalsBean extends PrincipalBean {

  private static final String PRINCIPALS = "principals";

  // For "extract"
  public PrincipalsBean(MPrincipal principal) {
    super(principal);
  }

  public PrincipalsBean(List<MPrincipal> principals) {
    super(principals);

  }

  // For "restore"
  public PrincipalsBean() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public JSONObject extract(boolean skipSensitive) {
    JSONArray principalsArray = super.extractPrincipals();
    JSONObject principals = new JSONObject();
    principals.put(PRINCIPALS, principalsArray);
    return principals;
  }

  @Override
  public void restore(JSONObject json) {
    JSONArray principalsArray = (JSONArray) json.get(PRINCIPALS);
    restorePrincipals(principalsArray);
  }

}
