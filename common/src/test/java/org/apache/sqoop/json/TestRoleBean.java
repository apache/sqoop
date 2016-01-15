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

import org.apache.sqoop.model.MRole;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

public class TestRoleBean {

  @Test
  public void testLinkSerialization() {
    MRole godRole = new MRole("god");
    MRole janitorRole = new MRole("janitor");
    List<MRole> roles = new LinkedList<>();
    roles.add(godRole);
    roles.add(janitorRole);

    // Serialize it to JSON object
    RoleBean outputBean = new RoleBean(roles);
    JSONObject json = outputBean.extract(false);

    // "Move" it across network in text form
    String jsonString = json.toJSONString();

    // Retrieved transferred object
    JSONObject parsedJson = JSONUtils.parse(jsonString);
    RoleBean inputBean = new RoleBean(roles);
    inputBean.restore(parsedJson);

    assertEquals(inputBean.getRoles().size(), 2);
    assertEquals(inputBean.getRoles().get(0).getName(), "god");
    assertEquals(inputBean.getRoles().get(1).getName(), "janitor");

  }

}
