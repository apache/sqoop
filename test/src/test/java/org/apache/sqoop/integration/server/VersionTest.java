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
package org.apache.sqoop.integration.server;

import org.apache.sqoop.client.request.VersionResourceRequest;
import org.apache.sqoop.common.VersionInfo;
import org.apache.sqoop.test.testcases.TomcatTestCase;
import org.apache.sqoop.json.VersionBean;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Basic test to check that server is working and returning correct version info.
 */
public class VersionTest extends TomcatTestCase {

  @Test
  public void testVersion() {
    VersionResourceRequest versionRequest = new VersionResourceRequest();
    VersionBean versionBean = versionRequest.read(getServerUrl());

    assertEquals(versionBean.getBuildVersion(), VersionInfo.getBuildVersion());
    assertEquals(versionBean.getBuildDate(), VersionInfo.getBuildDate());
    assertEquals(versionBean.getSourceRevision(), VersionInfo.getSourceRevision());
    assertEquals(versionBean.getSystemUser(), VersionInfo.getUser());
    assertEquals(versionBean.getSourceRevision(), VersionInfo.getSourceRevision());
  }

}
