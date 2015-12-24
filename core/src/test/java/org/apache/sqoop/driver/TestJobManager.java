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
package org.apache.sqoop.driver;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.error.code.DriverError;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.repository.JdbcRepository;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.request.HttpEventContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestJobManager {
  private JobManager jobManager;
  private SqoopConnector sqoopConnectorMock;
  private ConnectorManager connectorMgrMock;
  private RepositoryManager repositoryManagerMock;
  private Repository jdbcRepoMock;
  private SqoopConfiguration configurationMock;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    configurationMock = mock(SqoopConfiguration.class);
    doReturn(new MapContext(Collections.EMPTY_MAP)).when(configurationMock).getContext();
    SqoopConfiguration.setInstance(configurationMock);

    jobManager = JobManager.getInstance();
    connectorMgrMock = mock(ConnectorManager.class);
    sqoopConnectorMock = mock(SqoopConnector.class);
    ConnectorManager.setInstance(connectorMgrMock);
    repositoryManagerMock = mock(RepositoryManager.class);
    RepositoryManager.setInstance(repositoryManagerMock);
    jdbcRepoMock = mock(JdbcRepository.class);
  }

  @Test
  public void testCreateJobSubmission() {

    HttpEventContext testCtx = new HttpEventContext();
    testCtx.setUsername("testUser");
    MSubmission jobSubmission = jobManager.createJobSubmission(testCtx, "jobName");
    assertEquals(jobSubmission.getCreationUser(), "testUser");
    assertEquals(jobSubmission.getLastUpdateUser(), "testUser");
  }

  @Test
  public void testUnsupportedDirectionForConnector() {
    // invalid job id/ direction
    SqoopException exception = new SqoopException(DriverError.DRIVER_0011, "Connector: "
        + sqoopConnectorMock.getClass().getCanonicalName());
    List<Direction> supportedDirections = getSupportedDirections();
    when(sqoopConnectorMock.getSupportedDirections()).thenReturn(supportedDirections);

    try {
      // invalid direction
      jobManager.validateSupportedDirection(sqoopConnectorMock, null);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(sqoopConnectorMock, times(1)).getSupportedDirections();
      return;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  @Test
  public void testGetLink() {
    MLink testLink = new MLink("linkName", null);
    testLink.setEnabled(true);
    MLink mConnectionSpy = org.mockito.Mockito.spy(testLink);
    when(repositoryManagerMock.getRepository()).thenReturn(jdbcRepoMock);
    when(jdbcRepoMock.findLink("linkName")).thenReturn(mConnectionSpy);
    assertEquals(jobManager.getLink("linkName"), mConnectionSpy);
    verify(repositoryManagerMock, times(1)).getRepository();
    verify(jdbcRepoMock, times(1)).findLink("linkName");
  }

  @Test
  public void testDisabledLink() {
    MLink testConnection = new MLink("linkName", null);
    testConnection.setPersistenceId(1234);
    testConnection.setEnabled(false);
    SqoopException exception = new SqoopException(DriverError.DRIVER_0010, "Connection: "
        + testConnection.getName());

    MLink mConnectionSpy = org.mockito.Mockito.spy(testConnection);
    when(repositoryManagerMock.getRepository()).thenReturn(jdbcRepoMock);
    when(jdbcRepoMock.findLink("linkName")).thenReturn(mConnectionSpy);
    try {
      jobManager.getLink("linkName");
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repositoryManagerMock, times(1)).getRepository();
      verify(jdbcRepoMock, times(1)).findLink("linkName");
    }
  }

  @Test
  public void testGetJob() {
    MJob testJob = job("jobName", "fromConnectorName", "toConnectorName");
    testJob.setEnabled(true);
    MJob mJobSpy = org.mockito.Mockito.spy(testJob);
    when(repositoryManagerMock.getRepository()).thenReturn(jdbcRepoMock);
    when(jdbcRepoMock.findJob("jobName")).thenReturn(mJobSpy);
    assertEquals(jobManager.getJob("jobName"), mJobSpy);
    verify(repositoryManagerMock, times(1)).getRepository();
    verify(jdbcRepoMock, times(1)).findJob("jobName");
  }

  @Test
  public void testDisabledJob() {
    MJob testJob = job("jobName", "fromConnectorName", "toConnectorName");
    testJob.setEnabled(false);
    testJob.setPersistenceId(1111);
    SqoopException exception = new SqoopException(DriverError.DRIVER_0009, "Job: "
        + testJob.getName());

    MJob mJobSpy = org.mockito.Mockito.spy(testJob);
    when(repositoryManagerMock.getRepository()).thenReturn(jdbcRepoMock);
    when(jdbcRepoMock.findJob("jobName")).thenReturn(mJobSpy);
    try {
      jobManager.getJob("jobName");
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repositoryManagerMock, times(1)).getRepository();
      verify(jdbcRepoMock, times(1)).findJob("jobName");
    }
  }

  @Test
  public void testUnknownJob() {
    SqoopException exception = new SqoopException(DriverError.DRIVER_0004, "Unknown job name: testJobName");
    when(repositoryManagerMock.getRepository()).thenReturn(jdbcRepoMock);
    when(jdbcRepoMock.findJob("testJobName")).thenReturn(null);
    try {
      jobManager.getJob("testJobName");
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repositoryManagerMock, times(1)).getRepository();
      verify(jdbcRepoMock, times(1)).findJob("testJobName");
    }
  }

  private MJob job(String jobName, String fromConnectorName, String toConnectorName) {
    MJob job = new MJob(fromConnectorName, toConnectorName, "fromLinkName", "toLinkName", null, null, null);
    job.setName(jobName);
    job.setCreationUser("Buffy");
    return job;
  }

  public List<Direction> getSupportedDirections() {
    return Arrays.asList(new Direction[] { Direction.FROM, Direction.TO });
  }
}