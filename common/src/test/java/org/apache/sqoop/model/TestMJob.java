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
package org.apache.sqoop.model;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

/**
 * Test class for org.apache.sqoop.model.MJob
 */
public class TestMJob {
  /**
   * Test class for initialization
   */
  @Test
  public void testInitialization() {
    List<MForm> forms = new ArrayList<MForm>();
    MJobForms jobform1 = new MJobForms(MJob.Type.EXPORT, forms);
    List<MForm> forms2 = new ArrayList<MForm>();
    MJobForms jobform2 = new MJobForms(MJob.Type.EXPORT, forms2);
    MJob job = new MJob(123l, 456l, MJob.Type.EXPORT, jobform1, jobform2);

    assertEquals(123l, job.getConnectorId());
    assertEquals(456l, job.getConnectionId());
    assertEquals(MJob.Type.EXPORT, job.getType());
    assertEquals(jobform1, job.getConnectorPart());
    assertEquals(jobform2, job.getFrameworkPart());
  }

  @Test
  public void testClone() {
    List<MForm> forms = new ArrayList<MForm>();
    forms.add(getInputValues());
    MJobForms jobform1 = new MJobForms(MJob.Type.EXPORT, forms);
    List<MForm> forms2 = new ArrayList<MForm>();
    forms2.add(getInputValues());
    MJobForms jobform2 = new MJobForms(MJob.Type.EXPORT, forms2);
    MJob job = new MJob(123l, 456l, MJob.Type.EXPORT, jobform1, jobform2);
    job.setPersistenceId(12l);

    MForm originalForm = job.getConnectorPart().getForms().get(0);
    //Clone job
    MJob cloneJob = job.clone(true);
    assertEquals(12l, cloneJob.getPersistenceId());
    assertEquals(123l, cloneJob.getConnectorId());
    assertEquals(456l, cloneJob.getConnectionId());
    assertEquals(MJob.Type.EXPORT, cloneJob.getType());
    MForm clonedForm = cloneJob.getConnectorPart().getForms().get(0);
    assertEquals(clonedForm.getInputs().get(0).getValue(), originalForm.getInputs().get(0).getValue());
    assertEquals(clonedForm.getInputs().get(1).getValue(), originalForm.getInputs().get(1).getValue());
    assertNotNull(clonedForm.getInputs().get(0).getValue());
    assertNotNull(clonedForm.getInputs().get(1).getValue());
    assertEquals(job, cloneJob);

    //Clone job without value
    MJob cloneJob1 = job.clone(false);
    assertEquals(123l, cloneJob1.getConnectorId());
    assertEquals(456l, cloneJob1.getConnectionId());
    assertEquals(MJob.Type.EXPORT, cloneJob1.getType());
    clonedForm = cloneJob1.getConnectorPart().getForms().get(0);
    assertNull(clonedForm.getInputs().get(0).getValue());
    assertNull(clonedForm.getInputs().get(1).getValue());
    assertNotSame(job, cloneJob1);
  }

  private MForm getInputValues() {
    MIntegerInput input = new MIntegerInput("INTEGER-INPUT", false);
    input.setValue(100);
    MStringInput strInput = new MStringInput("STRING-INPUT",false,(short)20);
    strInput.setValue("TEST-VALUE");
    List<MInput<?>> list = new ArrayList<MInput<?>>();
    list.add(input);
    list.add(strInput);
    MForm form = new MForm("FORMNAME", list);
    return form;
  }
}
