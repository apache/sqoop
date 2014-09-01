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
package org.apache.sqoop.repository.derby;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test job methods on Derby repository.
 */
public class TestJobHandling extends DerbyTestCase {

  DerbyRepositoryHandler handler;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    handler = new DerbyRepositoryHandler();

    // We always needs schema for this test case
    createSchema();

    // We always needs connector and framework structures in place
    loadConnectorAndFramework();

    // We always needs connection metadata in place
    loadConnections();
  }

  public void testFindJob() throws Exception {
    // Let's try to find non existing job
    try {
      handler.findJob(1, getDerbyConnection());
      fail();
    } catch(SqoopException ex) {
      assertEquals(DerbyRepoError.DERBYREPO_0030, ex.getErrorCode());
    }

    // Load prepared connections into database
    loadJobs();

    MJob jobImport = handler.findJob(1, getDerbyConnection());
    assertNotNull(jobImport);
    assertEquals(1, jobImport.getPersistenceId());
    assertEquals("JA", jobImport.getName());

    List<MForm> forms;

    // Check connector parts
    forms = jobImport.getConnectorPart(Direction.FROM).getForms();
    assertEquals(2, forms.size());
    assertEquals("Value5", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(0).getInputs().get(1).getValue());
    assertEquals("Value5", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(1).getInputs().get(1).getValue());

    forms = jobImport.getConnectorPart(Direction.TO).getForms();
    assertEquals(2, forms.size());
    assertEquals("Value9", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(0).getInputs().get(1).getValue());
    assertEquals("Value9", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(1).getInputs().get(1).getValue());

    // Check framework part
    forms = jobImport.getFrameworkPart().getForms();
    assertEquals(2, forms.size());
    assertEquals("Value17", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(0).getInputs().get(1).getValue());
    assertEquals("Value19", forms.get(1).getInputs().get(0).getValue());
    assertNull(forms.get(1).getInputs().get(1).getValue());
  }

  public void testFindJobs() throws Exception {
    List<MJob> list;

    // Load empty list on empty repository
    list = handler.findJobs(getDerbyConnection());
    assertEquals(0, list.size());

    loadJobs();

    // Load all two connections on loaded repository
    list = handler.findJobs(getDerbyConnection());
    assertEquals(4, list.size());

    assertEquals("JA", list.get(0).getName());

    assertEquals("JB", list.get(1).getName());

    assertEquals("JC", list.get(2).getName());

    assertEquals("JD", list.get(3).getName());
  }

  public void testExistsJob() throws Exception {
    // There shouldn't be anything on empty repository
    assertFalse(handler.existsJob(1, getDerbyConnection()));
    assertFalse(handler.existsJob(2, getDerbyConnection()));
    assertFalse(handler.existsJob(3, getDerbyConnection()));
    assertFalse(handler.existsJob(4, getDerbyConnection()));
    assertFalse(handler.existsJob(5, getDerbyConnection()));

    loadJobs();

    assertTrue(handler.existsJob(1, getDerbyConnection()));
    assertTrue(handler.existsJob(2, getDerbyConnection()));
    assertTrue(handler.existsJob(3, getDerbyConnection()));
    assertTrue(handler.existsJob(4, getDerbyConnection()));
    assertFalse(handler.existsJob(5, getDerbyConnection()));
  }

  public void testInUseJob() throws Exception {
    loadJobs();
    loadSubmissions();

    assertTrue(handler.inUseJob(1, getDerbyConnection()));
    assertFalse(handler.inUseJob(2, getDerbyConnection()));
    assertFalse(handler.inUseJob(3, getDerbyConnection()));
    assertFalse(handler.inUseJob(4, getDerbyConnection()));
  }

  public void testCreateJob() throws Exception {
    MJob job = getJob();

    // Load some data
    fillJob(job);

    handler.createJob(job, getDerbyConnection());

    assertEquals(1, job.getPersistenceId());
    assertCountForTable("SQOOP.SQ_JOB", 1);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 6);

    MJob retrieved = handler.findJob(1, getDerbyConnection());
    assertEquals(1, retrieved.getPersistenceId());

    List<MForm> forms;
    forms = job.getConnectorPart(Direction.FROM).getForms();
    assertEquals("Value1", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(0).getInputs().get(1).getValue());
    forms = job.getConnectorPart(Direction.TO).getForms();
    assertEquals("Value1", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(0).getInputs().get(1).getValue());

    forms = job.getFrameworkPart().getForms();
    assertEquals("Value13", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(0).getInputs().get(1).getValue());
    assertEquals("Value15", forms.get(1).getInputs().get(0).getValue());
    assertNull(forms.get(1).getInputs().get(1).getValue());

    // Let's create second job
    job = getJob();
    fillJob(job);

    handler.createJob(job, getDerbyConnection());

    assertEquals(2, job.getPersistenceId());
    assertCountForTable("SQOOP.SQ_JOB", 2);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 12);
  }

  public void testUpdateJob() throws Exception {
    loadJobs();

    assertCountForTable("SQOOP.SQ_JOB", 4);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 24);

    MJob job = handler.findJob(1, getDerbyConnection());

    List<MForm> forms;

    forms = job.getConnectorPart(Direction.FROM).getForms();
    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput)forms.get(0).getInputs().get(1)).setValue(null);
    forms = job.getConnectorPart(Direction.TO).getForms();
    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput)forms.get(0).getInputs().get(1)).setValue(null);

    forms = job.getFrameworkPart().getForms();
    ((MStringInput)forms.get(0).getInputs().get(0)).setValue("Updated");
    ((MMapInput)forms.get(0).getInputs().get(1)).setValue(new HashMap<String, String>()); // inject new map value
    ((MStringInput)forms.get(1).getInputs().get(0)).setValue("Updated");
    ((MMapInput)forms.get(1).getInputs().get(1)).setValue(new HashMap<String, String>()); // inject new map value

    job.setName("name");

    handler.updateJob(job, getDerbyConnection());

    assertEquals(1, job.getPersistenceId());
    assertCountForTable("SQOOP.SQ_JOB", 4);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 26);

    MJob retrieved = handler.findJob(1, getDerbyConnection());
    assertEquals("name", retrieved.getName());

    forms = job.getConnectorPart(Direction.FROM).getForms();
    assertEquals(2, forms.size());
    assertEquals("Updated", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(0).getInputs().get(1).getValue());
    forms = job.getConnectorPart(Direction.TO).getForms();
    assertEquals(2, forms.size());
    assertEquals("Updated", forms.get(0).getInputs().get(0).getValue());
    assertNull(forms.get(0).getInputs().get(1).getValue());

    forms = retrieved.getFrameworkPart().getForms();
    assertEquals(2, forms.size());
    assertEquals("Updated", forms.get(0).getInputs().get(0).getValue());
    assertNotNull(forms.get(0).getInputs().get(1).getValue());
    assertEquals(((Map)forms.get(0).getInputs().get(1).getValue()).size(), 0);
  }

  public void testEnableAndDisableJob() throws Exception {
    loadJobs();

    // disable job 1
    handler.enableJob(1, false, getDerbyConnection());

    MJob retrieved = handler.findJob(1, getDerbyConnection());
    assertNotNull(retrieved);
    assertEquals(false, retrieved.getEnabled());

    // enable job 1
    handler.enableJob(1, true, getDerbyConnection());

    retrieved = handler.findJob(1, getDerbyConnection());
    assertNotNull(retrieved);
    assertEquals(true, retrieved.getEnabled());
  }

  public void testDeleteJob() throws Exception {
    loadJobs();

    handler.deleteJob(1, getDerbyConnection());
    assertCountForTable("SQOOP.SQ_JOB", 3);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 18);

    handler.deleteJob(2, getDerbyConnection());
    assertCountForTable("SQOOP.SQ_JOB", 2);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 12);

    handler.deleteJob(3, getDerbyConnection());
    assertCountForTable("SQOOP.SQ_JOB", 1);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 6);

    handler.deleteJob(4, getDerbyConnection());
    assertCountForTable("SQOOP.SQ_JOB", 0);
    assertCountForTable("SQOOP.SQ_JOB_INPUT", 0);
  }

  public MJob getJob() {
    return new MJob(1, 1, 1, 1,
      handler.findConnector("A", getDerbyConnection()).getJobForms(Direction.FROM),
      handler.findConnector("A", getDerbyConnection()).getJobForms(Direction.TO),
      handler.findFramework(getDerbyConnection()).getJobForms()
    );
  }
}
