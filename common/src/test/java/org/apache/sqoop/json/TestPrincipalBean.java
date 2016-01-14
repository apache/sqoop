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

import org.apache.sqoop.model.MPrincipal;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

public class TestPrincipalBean {

  @Test
  public void testSerialization() {
    MPrincipal principalUser = new MPrincipal("jarcec", MPrincipal.TYPE.USER);
    MPrincipal principalGroup = new MPrincipal("sqoop", MPrincipal.TYPE.GROUP);
    List<MPrincipal> principals = new LinkedList<>();
    principals.add(principalUser);
    principals.add(principalGroup);

    // Serialize it to JSON object
    PrincipalBean outputBean = new PrincipalBean(principals);
    JSONObject json = outputBean.extract(false);

    // "Move" it across network in text form
    String jsonString = json.toJSONString();

    // Retrieved transferred object
    JSONObject parsedJson = JSONUtils.parse(jsonString);
    PrincipalBean inputBean = new PrincipalBean();
    inputBean.restore(parsedJson);

    assertEquals(inputBean.getPrincipals().size(), 2);

    assertEquals(inputBean.getPrincipals().get(0).getName(), "jarcec");
    assertEquals(inputBean.getPrincipals().get(0).getType(), "USER");

    assertEquals(inputBean.getPrincipals().get(1).getName(), "sqoop");
    assertEquals(inputBean.getPrincipals().get(1).getType(), "GROUP");
  }

}
