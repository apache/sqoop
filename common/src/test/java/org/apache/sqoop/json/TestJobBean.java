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

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MStringInput;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.junit.Test;

import java.util.Date;

import static junit.framework.Assert.assertEquals;
import static org.apache.sqoop.json.TestUtil.getJob;

/**
 *
 */
public class TestJobBean {
  @Test
  public void testSerialization() throws ParseException {
    Date created = new Date();
    Date updated = new Date();
    MJob job = getJob("ahoj");
    job.setName("The big job");
    job.setPersistenceId(666);
    job.setCreationDate(created);
    job.setLastUpdateDate(updated);
    job.setEnabled(false);

    // Fill some data at the beginning
    MStringInput input = (MStringInput) job.getConnectorPart(Direction.FROM)
        .getForms().get(0).getInputs().get(0);
    input.setValue("Hi there!");
    input = (MStringInput) job.getConnectorPart(Direction.TO)
        .getForms().get(0).getInputs().get(0);
    input.setValue("Hi there again!");

    // Serialize it to JSON object
    JobBean bean = new JobBean(job);
    JSONObject json = bean.extract(false);

    // "Move" it across network in text form
    String string = json.toJSONString();

    // Retrieved transferred object
    JSONObject retrievedJson = (JSONObject)JSONValue.parseWithException(string);
    JobBean retrievedBean = new JobBean();
    retrievedBean.restore(retrievedJson);
    MJob target = retrievedBean.getJobs().get(0);

    // Check id and name
    assertEquals(666, target.getPersistenceId());
    assertEquals(target.getLinkId(Direction.FROM), 1);
    assertEquals(target.getLinkId(Direction.TO), 2);
    assertEquals(target.getConnectorId(Direction.FROM), 1);
    assertEquals(target.getConnectorId(Direction.TO), 2);
    assertEquals("The big job", target.getName());
    assertEquals(created, target.getCreationDate());
    assertEquals(updated, target.getLastUpdateDate());
    assertEquals(false, target.getEnabled());

    // Test that value was correctly moved
    MStringInput targetInput = (MStringInput) target.getConnectorPart(Direction.FROM)
      .getForms().get(0).getInputs().get(0);
    assertEquals("Hi there!", targetInput.getValue());
    targetInput = (MStringInput) target.getConnectorPart(Direction.TO)
        .getForms().get(0).getInputs().get(0);
    assertEquals("Hi there again!", targetInput.getValue());
  }
}
