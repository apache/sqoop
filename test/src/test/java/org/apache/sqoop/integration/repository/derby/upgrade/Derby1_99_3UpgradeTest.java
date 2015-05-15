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
package org.apache.sqoop.integration.repository.derby.upgrade;

import org.apache.sqoop.model.MJob;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertEquals;

/**
 * This version contains the following structures:
 * Generic JDBC Connector link with name "Link1" and id 1
 * Generic JDBC Connector link with name "Link2" and id 2
 * Generic JDBC Connector link with name "Link3" and id 3
 * Generic JDBC Connector link with name "Link4" and id 4
 * Job IMPORT with name "Job1" and id 1
 * Job IMPORT with name "Job2" and id 2
 * Job IMPORT with name "Job3" and id 3
 * Job EXPORT with name "Job4" and id 4
 * Job EXPORT with name "nonunique" and id 5
 * Job EXPORT with name "nonunique" and id 6
 * Link with id 4 has been disabled
 * Job with id 3 has been disabled
 * Job with id 1 has been run 5 times
 */
public class Derby1_99_3UpgradeTest extends DerbyRepositoryUpgradeTest {

  @Override
  public String getPathToRepositoryTarball() {
    return "/repository/derby/derby-repository-1.99.3.tar.gz";
  }

  @Override
  public int getNumberOfLinks() {
    return 5;
  }

  @Override
  public int getNumberOfJobs() {
    return 6;
  }

  @Override
  public Map<Integer, Integer> getNumberOfSubmissions() {
    HashMap<Integer, Integer> ret = new HashMap<Integer, Integer>();
    ret.put(1, 5);
    ret.put(2, 0);
    ret.put(3, 0);
    ret.put(4, 0);
    return ret;
  }

  @Override
  public Integer[] getDisabledLinkIds() {
    return new Integer[] {4};
  }

  @Override
  public Integer[] getDisabledJobIds() {
    return new Integer[] {3};
  }

  @Override
  public Integer[] getDeleteLinkIds() {
    return new Integer[] {1, 2, 3, 4, 5};
  }

  @Override
  public Integer[] getDeleteJobIds() {
    return new Integer[] {1, 2, 3, 4, 5, 6};
  }

  @Test
  public void testNonuniqueNames() throws Exception {
    Set<String> jobNames = new TreeSet<String>();
    for(MJob job : getClient().getJobs()) {
      assertFalse(jobNames.contains(job.getName()));
      jobNames.add(job.getName());
    }
  }
}
