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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 *
 */
public class TestMFramework {

  @Test
  public void testForms() {
    List<MForm> connectionFormList = new ArrayList<MForm>();
    List<MForm> jobFormList = new ArrayList<MForm>();
    connectionFormList.add(new MForm("connection-test", new ArrayList<MInput<?>>()));
    jobFormList.add(new MForm("job-test", new ArrayList<MInput<?>>()));
    MConnectionForms connectionForms = new MConnectionForms(connectionFormList);
    MJobForms jobForms = new MJobForms(jobFormList);

    MFramework framework = new MFramework(connectionForms, jobForms, "1");
    assertEquals(1, framework.getJobForms().getForms().size());
    assertEquals("job-test", framework.getJobForms().getForms().get(0).getName());
    assertEquals(1, framework.getConnectionForms().getForms().size());
    assertEquals("connection-test", framework.getConnectionForms().getForms().get(0).getName());
  }
}
