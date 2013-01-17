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
}
