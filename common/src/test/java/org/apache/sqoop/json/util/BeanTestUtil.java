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
package org.apache.sqoop.json.util;

import java.util.Date;

import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MToConfig;

public class BeanTestUtil {

  public static MLink createLink(String connectorName, String linkName, Long linkId, Date created,
      Date updated) {
    MLink link1 = getLink(connectorName);
    link1.setName(linkName);
    link1.setPersistenceId(linkId);
    link1.setCreationUser("admin");
    link1.setCreationDate(created);
    link1.setLastUpdateUser("user");
    link1.setLastUpdateDate(updated);
    link1.setEnabled(false);
    return link1;
  }

  public static MJob createJob(String connectorName, String jobName, Long jobId, Date created, Date updated) {
    MJob job = BeanTestUtil.getJob(connectorName);
    job.setName(jobName);
    job.setPersistenceId(jobId);
    job.setCreationDate(created);
    job.setLastUpdateDate(updated);
    job.setEnabled(false);
    return job;
  }

  public static MLink getLink(String connectorName) {
    return new MLink(1, getConnector(1L, connectorName).getLinkConfig());
  }

  public static MConnector getConnector(Long connectorId, String connectorName) {
    return getConnector(connectorId, connectorName, true, true);
  }

  public static MConnector getConnector(Long id, String name, boolean from, boolean to) {
    MFromConfig fromConfig = null;
    MToConfig toConfig = null;
    if (from) {
      fromConfig = ConfigTestUtil.getFromConfig();
    }
    if (to) {
      toConfig = ConfigTestUtil.getToConfig();
    }

    MConnector connector = new MConnector(name, name + ".class", "1.0-test",
        ConfigTestUtil.getLinkConfig(), fromConfig, toConfig);
    // simulate a persistence id
    connector.setPersistenceId(id);
    return connector;
  }

  public static MJob getJob(String connectorName) {
    return new MJob(1, 2, 1, 2, getConnector(1L, connectorName).getFromConfig(), getConnector(1L, connectorName)
        .getToConfig(), ConfigTestUtil.getDriverConfig());
  }

}
