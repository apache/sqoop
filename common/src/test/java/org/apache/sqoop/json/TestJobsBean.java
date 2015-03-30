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

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.json.util.BeanTestUtil;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MStringInput;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.testng.annotations.Test;

public class TestJobsBean {

  @Test
  public void testJobsSerialization() throws ParseException {
    Date created = new Date();
    Date updated = new Date();
    MJob job1 = BeanTestUtil.createJob("ahoj", "The big Job", 22L, created, updated);
    MJob job2 = BeanTestUtil.createJob("ahoj", "The small Job", 44L, created, updated);

    List<MJob> jobs = new ArrayList<MJob>();
    jobs.add(job1);
    jobs.add(job2);

    // Fill some data at the beginning
    MStringInput input = (MStringInput) job1.getFromJobConfig().getConfigs().get(0)
        .getInputs().get(0);
    input.setValue("Hi there!");
    input = (MStringInput) job1.getToJobConfig().getConfigs().get(0).getInputs().get(0);
    input.setValue("Hi there again!");

    // Serialize it to JSON object
    JobsBean jobsBean = new JobsBean(jobs);
    JSONObject json = jobsBean.extract(false);

    // "Move" it across network in text form
    String jobJsonString = json.toJSONString();

    // Retrieved transferred object
    JSONObject parsedJobsJson = JSONUtils.parse(jobJsonString);
    JobsBean parsedJobsBean = new JobsBean();
    parsedJobsBean.restore(parsedJobsJson);
    MJob retrievedJob1 = parsedJobsBean.getJobs().get(0);
    MJob retrievedJob2 = parsedJobsBean.getJobs().get(1);

    // Check id and name
    assertEquals(22L, retrievedJob1.getPersistenceId());
    assertEquals("The big Job", retrievedJob1.getName());

    assertEquals(44L, retrievedJob2.getPersistenceId());
    assertEquals("The small Job", retrievedJob2.getName());

    assertEquals(retrievedJob1.getFromLinkId(), 1);
    assertEquals(retrievedJob1.getToLinkId(), 2);
    assertEquals(retrievedJob1.getFromConnectorId(), 1);
    assertEquals(retrievedJob1.getToConnectorId(), 2);
    assertEquals(created, retrievedJob1.getCreationDate());
    assertEquals(updated, retrievedJob1.getLastUpdateDate());
    assertEquals(false, retrievedJob1.getEnabled());

    // Test that value was correctly moved
    MStringInput targetInput = (MStringInput) retrievedJob1.getFromJobConfig()
        .getConfigs().get(0).getInputs().get(0);
    assertEquals("Hi there!", targetInput.getValue());
    targetInput = (MStringInput) retrievedJob1.getToJobConfig().getConfigs().get(0)
        .getInputs().get(0);
    assertEquals("Hi there again!", targetInput.getValue());
  }
}
