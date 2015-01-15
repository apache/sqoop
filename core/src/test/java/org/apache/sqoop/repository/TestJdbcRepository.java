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
package org.apache.sqoop.repository;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.connector.common.EmptyConfiguration;
import org.apache.sqoop.connector.spi.ConnectorConfigurableUpgrader;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.driver.Driver;
import org.apache.sqoop.driver.DriverUpgrader;
import org.apache.sqoop.json.DriverBean;
import org.apache.sqoop.model.ConfigUtils;
import org.apache.sqoop.model.ConfigurationClass;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriver;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.model.Validator;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.validators.AbstractValidator;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.mockito.InOrder;

public class TestJdbcRepository {

  private JdbcRepository repoSpy;
  private JdbcRepositoryTransaction repoTransactionMock;
  private ConnectorManager connectorMgrMock;
  private Driver driverMock;
  private JdbcRepositoryHandler repoHandlerMock;
  private ConnectorConfigurableUpgrader connectorUpgraderMock;
  private DriverUpgrader driverUpgraderMock;

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    repoTransactionMock = mock(JdbcRepositoryTransaction.class);
    connectorMgrMock = mock(ConnectorManager.class);
    driverMock = mock(Driver.class);
    repoHandlerMock = mock(JdbcRepositoryHandler.class);
    connectorUpgraderMock = mock(ConnectorConfigurableUpgrader.class);
    driverUpgraderMock = mock(DriverUpgrader.class);
    repoSpy = spy(new JdbcRepository(repoHandlerMock, null));

    // setup transaction and connector manager
    doReturn(repoTransactionMock).when(repoSpy).getTransaction();
    ConnectorManager.setInstance(connectorMgrMock);
    Driver.setInstance(driverMock);

    doNothing().when(connectorUpgraderMock).upgradeLinkConfig(any(MLinkConfig.class),
        any(MLinkConfig.class));
    doNothing().when(connectorUpgraderMock).upgradeFromJobConfig(any(MFromConfig.class),
        any(MFromConfig.class));
    doNothing().when(connectorUpgraderMock).upgradeToJobConfig(any(MToConfig.class),
        any(MToConfig.class));
    doNothing().when(driverUpgraderMock).upgradeJobConfig(any(MDriverConfig.class),
        any(MDriverConfig.class));

  }

  /**
   * Test the procedure when the connector auto upgrade option is enabled
   */
  @Test
  public void testConnectorConfigEnableAutoUpgrade() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1, "1.0");

    when(repoHandlerMock.findConnector(anyString(), any(Connection.class))).thenReturn(oldConnector);

    // make the upgradeConnector to throw an exception to prove that it has been called
    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "upgradeConnector() has been called.");
    doThrow(exception).when(connectorMgrMock).getSqoopConnector(anyString());

    try {
      repoSpy.registerConnector(newConnector, true);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnector(anyString(), any(Connection.class));
      verify(connectorMgrMock, times(1)).getSqoopConnector(anyString());
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the procedure when  the connector auto upgrade option is disabled
   */
  @Test
  public void testConnectorDisableAutoUpgrade() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    when(repoHandlerMock.findConnector(anyString(), any(Connection.class))).thenReturn(oldConnector);

    try {
      repoSpy.registerConnector(newConnector, false);
    } catch (SqoopException ex) {
      verify(repoHandlerMock, times(1)).findConnector(anyString(), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      assertEquals(ex.getErrorCode(), RepositoryError.JDBCREPO_0026);
      return ;
    }

    fail("Should throw out an exception with code: " + RepositoryError.JDBCREPO_0026);
  }

  /**
   * Test the procedure when the driverConfig auto upgrade option is enabled
   */
  @Test
  public void testDriverConfigEnableAutoUpgrade() {
    MDriver newDriver = driver();
    MDriver oldDriver = anotherDriver();

    when(repoHandlerMock.findDriver(anyString(), any(Connection.class))).thenReturn(oldDriver);

    // make the upgradeDriverConfig to throw an exception to prove that it has been called
    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "upgradeDriverConfig() has been called.");
    doThrow(exception).when(repoHandlerMock).findJobs(any(Connection.class));

    try {
      repoSpy.registerDriver(newDriver, true);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findDriver(anyString(), any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the procedure when the driverConfig auto upgrade option is disabled
   */
  @Test
  public void testDriverConfigDisableAutoUpgrade() {
    MDriver newDriver = driver();
    MDriver oldDriver = anotherDriver();

    when(repoHandlerMock.findDriver(anyString(), any(Connection.class))).thenReturn(oldDriver);

    try {
      repoSpy.registerDriver(newDriver, false);
    } catch (SqoopException ex) {
      assertEquals(ex.getErrorCode(), RepositoryError.JDBCREPO_0026);
      verify(repoHandlerMock, times(1)).findDriver(anyString(),any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with code: " + RepositoryError.JDBCREPO_0026);
  }

  /**
   * Test the connector upgrade procedure, when all the links and
   * jobs using the old connector are still valid for the new connector
   */
  @Test
  public void testConnectorConfigUpgradeWithValidLinksAndJobs() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    // prepare the sqoop connector
    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getConfigurableUpgrader()).thenReturn(connectorUpgraderMock);
    when(sqconnector.getLinkConfigurationClass()).thenReturn(EmptyConfiguration.class);
    when(sqconnector.getJobConfigurationClass(any(Direction.class))).thenReturn(EmptyConfiguration.class);
    when(connectorMgrMock.getSqoopConnector(anyString())).thenReturn(sqconnector);

    // prepare the links and jobs
    // the connector Id for both are the same
    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,2));

    // mock necessary methods for upgradeConnector() procedure
    doReturn(linkList).when(repoSpy).findLinksForConnector(anyLong());
    doReturn(jobList).when(repoSpy).findJobsForConnector(anyLong());
    doNothing().when(repoSpy).updateLink(any(MLink.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).upgradeConnectorAndConfigs(any(MConnector.class), any(RepositoryTransaction.class));

    repoSpy.upgradeConnector(oldConnector, newConnector);

    InOrder repoOrder = inOrder(repoSpy);
    InOrder txOrder = inOrder(repoTransactionMock);
    InOrder upgraderOrder = inOrder(connectorUpgraderMock);

    repoOrder.verify(repoSpy, times(1)).findLinksForConnector(anyLong());
    repoOrder.verify(repoSpy, times(1)).findJobsForConnector(anyLong());
    repoOrder.verify(repoSpy, times(1)).getTransaction();
    repoOrder.verify(repoSpy, times(1)).deleteJobInputs(1, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteJobInputs(2, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteLinkInputs(1, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteLinkInputs(2, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).upgradeConnectorAndConfigs(any(MConnector.class), any(RepositoryTransaction.class));
    repoOrder.verify(repoSpy, times(2)).updateLink(any(MLink.class), any(RepositoryTransaction.class));
    repoOrder.verify(repoSpy, times(4)).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    repoOrder.verifyNoMoreInteractions();
    txOrder.verify(repoTransactionMock, times(1)).begin();
    txOrder.verify(repoTransactionMock, times(1)).commit();
    txOrder.verify(repoTransactionMock, times(1)).close();
    txOrder.verifyNoMoreInteractions();
    upgraderOrder.verify(connectorUpgraderMock, times(2)).upgradeLinkConfig(any(MLinkConfig.class), any(MLinkConfig.class));
    upgraderOrder.verify(connectorUpgraderMock, times(1)).upgradeFromJobConfig(any(MFromConfig.class), any(MFromConfig.class));
    upgraderOrder.verify(connectorUpgraderMock, times(1)).upgradeToJobConfig(any(MToConfig.class), any(MToConfig.class));
    upgraderOrder.verify(connectorUpgraderMock, times(1)).upgradeFromJobConfig(any(MFromConfig.class), any(MFromConfig.class));
    upgraderOrder.verify(connectorUpgraderMock, times(1)).upgradeToJobConfig(any(MToConfig.class), any(MToConfig.class));
    upgraderOrder.verifyNoMoreInteractions();
  }

  /**
   * Test the driverConfig upgrade procedure, when all jobs
   * using the old connector are still valid for the new connector
   */
  @Test
  public void testDriverConfigUpgradeWithValidJobs() {
    MDriver newDriverConfig = driver();

    when(driverMock.getConfigurableUpgrader()).thenReturn(driverUpgraderMock);
    when(driverMock.getDriverJobConfigurationClass()).thenReturn(ValidConfiguration.class);
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));

    doReturn(jobList).when(repoSpy).findJobs();
    doNothing().when(repoSpy).updateLink(any(MLink.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).upgradeDriverAndConfigs(any(MDriver.class), any(RepositoryTransaction.class));

    repoSpy.upgradeDriver(newDriverConfig);

    InOrder repoOrder = inOrder(repoSpy);
    InOrder txOrder = inOrder(repoTransactionMock);
    InOrder upgraderOrder = inOrder(driverUpgraderMock);

    repoOrder.verify(repoSpy, times(1)).findJobs();
    repoOrder.verify(repoSpy, times(1)).getTransaction();
    repoOrder.verify(repoSpy, times(1)).deleteJobInputs(1, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteJobInputs(2, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).upgradeDriverAndConfigs(any(MDriver.class), any(RepositoryTransaction.class));
    repoOrder.verify(repoSpy, times(2)).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    repoOrder.verifyNoMoreInteractions();
    txOrder.verify(repoTransactionMock, times(1)).begin();
    txOrder.verify(repoTransactionMock, times(1)).commit();
    txOrder.verify(repoTransactionMock, times(1)).close();
    txOrder.verifyNoMoreInteractions();
    upgraderOrder.verify(driverUpgraderMock, times(2)).upgradeJobConfig(any(MDriverConfig.class), any(MDriverConfig.class));
    upgraderOrder.verifyNoMoreInteractions();
  }

  /**
   * Test the driverConfig upgrade procedure, when all the jobs
   * using the old connector are invalid for the new connector
   */
  @Test
  public void testDriverConfigUpgradeWithInvalidJobs() {
    MDriver newDriverConfig = driver();

    when(driverMock.getConfigurableUpgrader()).thenReturn(driverUpgraderMock);
    when(driverMock.getDriverJobConfigurationClass()).thenReturn(InvalidConfiguration.class);
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));

    doReturn(jobList).when(repoSpy).findJobs();
    doNothing().when(repoSpy).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).upgradeDriverAndConfigs(any(MDriver.class), any(RepositoryTransaction.class));

    try {
      repoSpy.upgradeDriver(newDriverConfig);
    } catch (SqoopException ex) {
      assertEquals(ex.getErrorCode(), RepositoryError.JDBCREPO_0027);

      InOrder repoOrder = inOrder(repoSpy);
      InOrder txOrder = inOrder(repoTransactionMock);
      InOrder upgraderOrder = inOrder(driverUpgraderMock);

      repoOrder.verify(repoSpy, times(1)).findJobs();
      repoOrder.verify(repoSpy, times(1)).getTransaction();
      repoOrder.verify(repoSpy, times(1)).deleteJobInputs(1, repoTransactionMock);
      repoOrder.verify(repoSpy, times(1)).deleteJobInputs(2, repoTransactionMock);
      repoOrder.verify(repoSpy, times(1)).upgradeDriverAndConfigs(any(MDriver.class), any(RepositoryTransaction.class));
      repoOrder.verifyNoMoreInteractions();
      txOrder.verify(repoTransactionMock, times(1)).begin();
      txOrder.verify(repoTransactionMock, times(1)).rollback();
      txOrder.verify(repoTransactionMock, times(1)).close();
      txOrder.verifyNoMoreInteractions();
      upgraderOrder.verify(driverUpgraderMock, times(2)).upgradeJobConfig(any(MDriverConfig.class), any(MDriverConfig.class));
      upgraderOrder.verifyNoMoreInteractions();
      return ;
    }

    fail("Should throw out an exception with code: " + RepositoryError.JDBCREPO_0027);
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * find links for a given connector
   */
  @Test
  public void testConnectorConfigUpgradeHandlerWithFindLinksForConnectorError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getConfigurableUpgrader()).thenReturn(connectorUpgraderMock);
    when(connectorMgrMock.getSqoopConnector(anyString())).thenReturn(sqconnector);

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "find links for connector error.");
    doThrow(exception).when(repoHandlerMock).findLinksForConnector(anyLong(), any(Connection.class));

    try {
      repoSpy.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findLinksForConnector(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * find jobs for a given connector
   */
  @Test
  public void testConnectorConfigUpgradeHandlerWithFindJobsForConnectorError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getConfigurableUpgrader()).thenReturn(connectorUpgraderMock);
    when(connectorMgrMock.getSqoopConnector(anyString())).thenReturn(sqconnector);

    List<MLink> linkList = links(link(1,1), link(2,1));
    doReturn(linkList).when(repoHandlerMock).findLinksForConnector(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "find jobs for connector error.");
    doThrow(exception).when(repoHandlerMock).findJobsForConnector(anyLong(), any(Connection.class));

    try {
      repoSpy.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findLinksForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * delete job inputs for a given connector
   */
  @Test
  public void testConnectorConfigUpgradeHandlerWithDeleteJobInputsError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getConfigurableUpgrader()).thenReturn(connectorUpgraderMock);
    when(connectorMgrMock.getSqoopConnector(anyString())).thenReturn(sqconnector);

    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(linkList).when(repoHandlerMock).findLinksForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobsForConnector(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "delete job inputs for connector error.");
    doThrow(exception).when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));

    try {
      repoSpy.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findLinksForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).deleteJobInputs(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * delete link inputs for a given connector
   */
  @Test
  public void testConnectorConfigUpgradeHandlerWithDeleteLinkInputsError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getConfigurableUpgrader()).thenReturn(connectorUpgraderMock);
    when(connectorMgrMock.getSqoopConnector(anyString())).thenReturn(sqconnector);

    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(linkList).when(repoHandlerMock).findLinksForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobsForConnector(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "delete link inputs for connector error.");
    doThrow(exception).when(repoHandlerMock).deleteLinkInputs(anyLong(), any(Connection.class));

    try {
      repoSpy.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findLinksForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).deleteLinkInputs(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * update the connector entity
   */
  @Test
  public void testConnectorConfigUpgradeHandlerWithUpdateConnectorError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getConfigurableUpgrader()).thenReturn(connectorUpgraderMock);
    when(connectorMgrMock.getSqoopConnector(anyString())).thenReturn(sqconnector);

    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(linkList).when(repoHandlerMock).findLinksForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobsForConnector(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteLinkInputs(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update connector error.");
    doThrow(exception).when(repoHandlerMock).upgradeConnectorAndConfigs(any(MConnector.class), any(Connection.class));

    try {
      repoSpy.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findLinksForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteLinkInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).upgradeConnectorAndConfigs(any(MConnector.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * update the link entity
   */
  @Test
  public void testConnectorConfigUpgradeHandlerWithUpdateLinkError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getConfigurableUpgrader()).thenReturn(connectorUpgraderMock);
    when(sqconnector.getLinkConfigurationClass()).thenReturn(ValidConfiguration.class);
    when(sqconnector.getJobConfigurationClass(any(Direction.class))).thenReturn(ValidConfiguration.class);
    when(connectorMgrMock.getSqoopConnector(anyString())).thenReturn(sqconnector);

    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(linkList).when(repoHandlerMock).findLinksForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobsForConnector(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteLinkInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).upgradeConnectorAndConfigs(any(MConnector.class), any(Connection.class));
    doReturn(true).when(repoHandlerMock).existsLink(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update link error.");
    doThrow(exception).when(repoHandlerMock).updateLink(any(MLink.class), any(Connection.class));

    try {
      repoSpy.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findLinksForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteLinkInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).upgradeConnectorAndConfigs(any(MConnector.class), any(Connection.class));
      verify(repoHandlerMock, times(1)).existsLink(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateLink(any(MLink.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * update the job entity
   */
  @Test
  public void testConnectorConfigUpgradeHandlerWithUpdateJobError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getConfigurableUpgrader()).thenReturn(connectorUpgraderMock);
    when(sqconnector.getLinkConfigurationClass()).thenReturn(ValidConfiguration.class);
    when(sqconnector.getJobConfigurationClass(any(Direction.class))).thenReturn(ValidConfiguration.class);
    when(connectorMgrMock.getSqoopConnector(anyString())).thenReturn(sqconnector);

    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(linkList).when(repoHandlerMock).findLinksForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobsForConnector(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteLinkInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).upgradeConnectorAndConfigs(any(MConnector.class), any(Connection.class));
    doNothing().when(repoHandlerMock).updateLink(any(MLink.class), any(Connection.class));
    doReturn(true).when(repoHandlerMock).existsLink(anyLong(), any(Connection.class));
    doReturn(true).when(repoHandlerMock).existsJob(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update job error.");
    doThrow(exception).when(repoHandlerMock).updateJob(any(MJob.class), any(Connection.class));

    try {
      repoSpy.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findLinksForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteLinkInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).upgradeConnectorAndConfigs(any(MConnector.class), any(Connection.class));
      verify(repoHandlerMock, times(2)).existsLink(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).updateLink(any(MLink.class), any(Connection.class));
      verify(repoHandlerMock, times(1)).existsJob(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateJob(any(MJob.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * find jobs for driverConfig
   */
  @Test
  public void testDriverConfigUpgradeHandlerWithFindJobsError() {
    MDriver newDriverConfig = driver();

    when(driverMock.getConfigurableUpgrader()).thenReturn(driverUpgraderMock);

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "find jobs error.");
    doThrow(exception).when(repoHandlerMock).findJobs(any(Connection.class));

    try {
      repoSpy.upgradeDriver(newDriverConfig);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * delete job inputs for driverConfig upgrade
   */
  @Test
  public void testDriverConfigUpgradeHandlerWithDeleteJobInputsError() {
    MDriver newDriverConfig = driver();

    when(driverMock.getConfigurableUpgrader()).thenReturn(driverUpgraderMock);

    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(jobList).when(repoHandlerMock).findJobs(any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "delete job inputs error.");
    doThrow(exception).when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));

    try {
      repoSpy.upgradeDriver(newDriverConfig);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verify(repoHandlerMock, times(1)).deleteJobInputs(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * update the driverConfig entity
   */
  @Test
  public void testDriverConfigUpgradeHandlerWithUpdateDriverConfigError() {
    MDriver newDriverConfig = driver();

    when(driverMock.getConfigurableUpgrader()).thenReturn(driverUpgraderMock);

    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(jobList).when(repoHandlerMock).findJobs(any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteLinkInputs(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update driverConfig entity error.");
    doThrow(exception).when(repoHandlerMock).upgradeDriverAndConfigs(any(MDriver.class), any(Connection.class));

    try {
      repoSpy.upgradeDriver(newDriverConfig);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).upgradeDriverAndConfigs(any(MDriver.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }


  /**
   * Test the exception handling procedure when the database handler fails to
   * update the job entity
   */
  @Test
  public void testDriverConfigUpgradeHandlerWithUpdateJobError() {
    MDriver driverConfig = driver();

    when(driverMock.getConfigurableUpgrader()).thenReturn(driverUpgraderMock);
    when(driverMock.getDriverJobConfigurationClass()).thenReturn(ValidConfiguration.class);
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(jobList).when(repoHandlerMock).findJobs(any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).upgradeDriverAndConfigs(any(MDriver.class), any(Connection.class));
    doReturn(true).when(repoHandlerMock).existsJob(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update job error.");
    doThrow(exception).when(repoHandlerMock).updateJob(any(MJob.class), any(Connection.class));

    try {
      repoSpy.upgradeDriver(driverConfig);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).upgradeDriverAndConfigs(any(MDriver.class), any(Connection.class));
      verify(repoHandlerMock, times(1)).existsJob(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateJob(any(MJob.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  private MConnector connector(long connectorId, String version) {
    MConnector connector = new MConnector("A" + connectorId, "A" + connectorId, version + connectorId,
        new MLinkConfig(new LinkedList<MConfig>()),
        new MFromConfig(ConfigUtils.toConfigs(ValidConfiguration.class)),
        new MToConfig(ConfigUtils.toConfigs(ValidConfiguration.class)));
    connector.setPersistenceId(connectorId);
    return connector;
  }

  private MConnector connector(long connectoId) {
    return connector(connectoId, "1.0");
  }

  private MDriver driver() {
    MDriver driver = new MDriver(new MDriverConfig(new LinkedList<MConfig>()),
        DriverBean.CURRENT_DRIVER_VERSION);
    driver.setPersistenceId(1);
    return driver;
  }

  private MDriver anotherDriver() {
    MDriver driver = new MDriver(null, DriverBean.CURRENT_DRIVER_VERSION);
    driver.setPersistenceId(1);
    return driver;
  }

  private MLink link(long linkId, long connectorId) {
    MLink link = new MLink(connectorId, new MLinkConfig(new LinkedList<MConfig>()));
    link.setPersistenceId(linkId);
    return link;
  }

  private MJob job(long id, long fromConnectorId, long toConnectorId, long fromLinkId, long toLinkId) {
    MJob job = new MJob(fromConnectorId, toConnectorId, fromLinkId, toLinkId,
        new MFromConfig(new LinkedList<MConfig>()),
        new MToConfig(new LinkedList<MConfig>()),
        new MDriverConfig(new LinkedList<MConfig>()));
    job.setPersistenceId(id);
    return job;
  }

  private List<MLink> links(MLink ... ls) {
    List<MLink> links = new ArrayList<MLink>();
    Collections.addAll(links, ls);
    return links;
  }

  private List<MJob> jobs(MJob ... js) {
    List<MJob> jobs = new ArrayList<MJob>();
    Collections.addAll(jobs, js);
    return jobs;
  }

  @ConfigurationClass
  public static class ValidConfiguration {
  }

  @ConfigurationClass(validators = { @Validator(InvalidConfiguration.InternalValidator.class)})
  public static class InvalidConfiguration {
    public static class InternalValidator extends AbstractValidator {
      @Override
      public void validate(Object instance) {
        addMessage(Status.ERROR, "Simply because.");
      }
    }
  }
}