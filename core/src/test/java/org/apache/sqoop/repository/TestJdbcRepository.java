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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.ConnectorManager;
import org.apache.sqoop.connector.spi.RepositoryUpgrader;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.driver.Driver;
import org.apache.sqoop.driver.configuration.JobConfiguration;
import org.apache.sqoop.model.ConfigurationClass;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;
import org.apache.sqoop.validation.Validator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
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

public class TestJdbcRepository {

  private JdbcRepository repoSpy;
  private JdbcRepositoryTransaction repoTransactionMock;
  private ConnectorManager connectorMgrMock;
  private Driver driverMock;
  private JdbcRepositoryHandler repoHandlerMock;
  private Validator validatorMock;
  private RepositoryUpgrader upgraderMock;

  private Validation validRepoMock;
  private Validation invalidRepoMock;

  @Before
  public void setUp() throws Exception {
    repoTransactionMock = mock(JdbcRepositoryTransaction.class);
    connectorMgrMock = mock(ConnectorManager.class);
    driverMock = mock(Driver.class);
    repoHandlerMock = mock(JdbcRepositoryHandler.class);
    validatorMock = mock(Validator.class);
    upgraderMock = mock(RepositoryUpgrader.class);
    repoSpy = spy(new JdbcRepository(repoHandlerMock, null));

    // setup transaction and connector manager
    doReturn(repoTransactionMock).when(repoSpy).getTransaction();
    ConnectorManager.setInstance(connectorMgrMock);
    Driver.setInstance(driverMock);

    validRepoMock = mock(Validation.class);
    when(validRepoMock.getStatus()).thenReturn(Status.ACCEPTABLE);
    invalidRepoMock = mock(Validation.class);
    when(invalidRepoMock.getStatus()).thenReturn(Status.UNACCEPTABLE);

    doNothing().when(upgraderMock).upgrade(any(MConnectionForms.class), any(MConnectionForms.class));
    doNothing().when(upgraderMock).upgrade(any(MJobForms.class), any(MJobForms.class));
  }

  /**
   * Test the procedure when the connector auto upgrade option is enabled
   */
  @Test
  public void testConnectorEnableAutoUpgrade() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1, "1.0");

    when(repoHandlerMock.findConnector(anyString(), any(Connection.class))).thenReturn(oldConnector);

    // make the upgradeConnector to throw an exception to prove that it has been called
    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "upgradeConnector() has been called.");
    doThrow(exception).when(connectorMgrMock).getConnector(anyString());

    try {
      repoSpy.registerConnector(newConnector, true);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnector(anyString(), any(Connection.class));
      verify(connectorMgrMock, times(1)).getConnector(anyString());
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
    MDriverConfig newDriverConfig = driverConfig();
    MDriverConfig oldDriverConfig = anotherDriverConfig();

    when(repoHandlerMock.findDriverConfig(any(Connection.class))).thenReturn(oldDriverConfig);

    // make the upgradeDriverConfig to throw an exception to prove that it has been called
    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "upgradeDriverConfig() has been called.");
    doThrow(exception).when(repoHandlerMock).findLinks(any(Connection.class));

    try {
      repoSpy.registerDriverConfig(newDriverConfig, true);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findDriverConfig(any(Connection.class));
      verify(repoHandlerMock, times(1)).findLinks(any(Connection.class));
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
    MDriverConfig newDriverConfig = driverConfig();
    MDriverConfig oldDriverConfig = anotherDriverConfig();

    when(repoHandlerMock.findDriverConfig(any(Connection.class))).thenReturn(oldDriverConfig);

    try {
      repoSpy.registerDriverConfig(newDriverConfig, false);
    } catch (SqoopException ex) {
      assertEquals(ex.getErrorCode(), RepositoryError.JDBCREPO_0026);
      verify(repoHandlerMock, times(1)).findDriverConfig(any(Connection.class));
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
  public void testConnectorUpgradeWithValidLinksAndJobs() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    // prepare the sqoop connector
    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(validatorMock.validateLink(any(MLink.class))).thenReturn(validRepoMock);
    when(validatorMock.validateJob(any(MJob.class))).thenReturn(validRepoMock);
    when(sqconnector.getValidator()).thenReturn(validatorMock);
    when(sqconnector.getRepositoryUpgrader()).thenReturn(upgraderMock);
    when(sqconnector.getLinkConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(sqconnector.getJobConfigurationClass(any(Direction.class))).thenReturn(JobConfiguration.class);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

    // prepare the links and jobs
    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));

    // mock necessary methods for upgradeConnector() procedure
    doReturn(linkList).when(repoSpy).findLinksForConnector(anyLong());
    doReturn(jobList).when(repoSpy).findJobsForConnector(anyLong());
    doNothing().when(repoSpy).updateLink(any(MLink.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).updateConnector(any(MConnector.class), any(RepositoryTransaction.class));

    repoSpy.upgradeConnector(oldConnector, newConnector);

    InOrder repoOrder = inOrder(repoSpy);
    InOrder txOrder = inOrder(repoTransactionMock);
    InOrder upgraderOrder = inOrder(upgraderMock);
    InOrder validatorOrder = inOrder(validatorMock);

    repoOrder.verify(repoSpy, times(1)).findLinksForConnector(anyLong());
    repoOrder.verify(repoSpy, times(1)).findJobsForConnector(anyLong());
    repoOrder.verify(repoSpy, times(1)).getTransaction();
    repoOrder.verify(repoSpy, times(1)).deleteJobInputs(1, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteJobInputs(2, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteLinkInputs(1, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteLinkInputs(2, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).updateConnector(any(MConnector.class), any(RepositoryTransaction.class));
    repoOrder.verify(repoSpy, times(2)).updateLink(any(MLink.class), any(RepositoryTransaction.class));
    repoOrder.verify(repoSpy, times(4)).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    repoOrder.verifyNoMoreInteractions();
    txOrder.verify(repoTransactionMock, times(1)).begin();
    txOrder.verify(repoTransactionMock, times(1)).commit();
    txOrder.verify(repoTransactionMock, times(1)).close();
    txOrder.verifyNoMoreInteractions();
    upgraderOrder.verify(upgraderMock, times(2)).upgrade(any(MConnectionForms.class), any(MConnectionForms.class));
    upgraderOrder.verify(upgraderMock, times(4)).upgrade(any(MJobForms.class), any(MJobForms.class));
    upgraderOrder.verifyNoMoreInteractions();
    validatorOrder.verify(validatorMock, times(2)).validateLink(anyObject());
    // @TODO(Abe): Re-enable job validation?
    validatorOrder.verify(validatorMock, times(0)).validateJob(anyObject());
    validatorOrder.verifyNoMoreInteractions();
  }

  /**
   * @TODO(Abe): To re-enable with Validation in Repository upgrade.
   * Test the connector upgrade procedure, when all the links and
   * jobs using the old connector are invalid for the new connector
   */
//  @Test
//  public void testConnectorUpgradeWithInvalidLinksAndJobs() {
//    MConnector newConnector = connector(1, "1.1");
//    MConnector oldConnector = connector(1);
//
//    // prepare the sqoop connector
//    SqoopConnector sqconnector = mock(SqoopConnector.class);
//    when(validator.validateLink(any(MLink.class))).thenReturn(invalid);
//    when(validator.validateJob(any(MJob.class))).thenReturn(invalid);
//    when(sqconnector.getValidator()).thenReturn(validator);
//    when(sqconnector.getDriverConfigRepositoryUpgrader()).thenReturn(upgrader);
//    when(sqconnector.getLinkConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
//    when(sqconnector.getJobConfigurationClass(any(Direction.class))).thenReturn(JobConfiguration.class);
//    when(connectorMgr.getConnector(anyString())).thenReturn(sqconnector);
//
//    // prepare the links and jobs
//    List<MLink> linkList = links(link(1,1), link(2,1));
//    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
//
//    doReturn(linkList).when(repo).findLinksForConnector(anyLong());
//    doReturn(jobList).when(repo).findJobsForConnector(anyLong());
//    doNothing().when(repo).updateLink(any(MLink.class), any(RepositoryTransaction.class));
//    doNothing().when(repo).updateJob(any(MJob.class), any(RepositoryTransaction.class));
//    doNothing().when(repo).updateConnector(any(MConnector.class), any(RepositoryTransaction.class));
//
//    try {
//      repo.upgradeConnector(oldConnector, newConnector);
//    } catch (SqoopException ex) {
//      assertEquals(ex.getErrorCode(), RepositoryError.JDBCREPO_0027);
//
//      InOrder repoOrder = inOrder(repo);
//      InOrder txOrder = inOrder(tx);
//      InOrder upgraderOrder = inOrder(upgrader);
//      InOrder validatorOrder = inOrder(validator);
//
//      repoOrder.verify(repo, times(1)).findLinksForConnector(anyLong());
//      repoOrder.verify(repo, times(1)).findJobsForConnector(anyLong());
//      repoOrder.verify(repo, times(1)).getTransaction();
//      repoOrder.verify(repo, times(1)).deleteJobInputs(1, tx);
//      repoOrder.verify(repo, times(1)).deleteJobInputs(2, tx);
//      repoOrder.verify(repo, times(1)).deleteLinkInputs(1, tx);
//      repoOrder.verify(repo, times(1)).deleteLinkInputs(2, tx);
//      repoOrder.verify(repo, times(1)).updateConnector(any(MConnector.class), any(RepositoryTransaction.class));
//      repoOrder.verifyNoMoreInteractions();
//      txOrder.verify(tx, times(1)).begin();
//      txOrder.verify(tx, times(1)).rollback();
//      txOrder.verify(tx, times(1)).close();
//      txOrder.verifyNoMoreInteractions();
//      upgraderOrder.verify(upgrader, times(2)).upgrade(any(MConnectionForms.class), any(MConnectionForms.class));
//      upgraderOrder.verify(upgrader, times(2)).upgrade(any(MJobForms.class), any(MJobForms.class));
//      upgraderOrder.verifyNoMoreInteractions();
//      validatorOrder.verify(validator, times(2)).validateLink(anyObject());
//      validatorOrder.verify(validator, times(2)).validateJob(anyObject());
//      validatorOrder.verifyNoMoreInteractions();
//      return ;
//    }
//
//    fail("Should throw out an exception with code: " + RepositoryError.JDBCREPO_0027);
//  }

  /**
   * Test the driverConfig upgrade procedure, when all the links and
   * jobs using the old connector are still valid for the new connector
   */
  @Test
  public void testDriverConfigUpgradeWithValidLinksAndJobs() {
    MDriverConfig newDriverConfig = driverConfig();

    when(validatorMock.validateLink(any(MLink.class))).thenReturn(validRepoMock);
    when(validatorMock.validateJob(any(MJob.class))).thenReturn(validRepoMock);
    when(driverMock.getValidator()).thenReturn(validatorMock);
    when(driverMock.getDriverConfigRepositoryUpgrader()).thenReturn(upgraderMock);
    when(driverMock.getLinkConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(driverMock.getJobConfigurationClass()).thenReturn(JobConfiguration.class);

    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));

    doReturn(linkList).when(repoSpy).findLinks();
    doReturn(jobList).when(repoSpy).findJobs();
    doNothing().when(repoSpy).updateLink(any(MLink.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).updateDriverConfig(any(MDriverConfig.class), any(RepositoryTransaction.class));

    repoSpy.upgradeDriverConfig(newDriverConfig);

    InOrder repoOrder = inOrder(repoSpy);
    InOrder txOrder = inOrder(repoTransactionMock);
    InOrder upgraderOrder = inOrder(upgraderMock);
    InOrder validatorOrder = inOrder(validatorMock);

    repoOrder.verify(repoSpy, times(1)).findLinks();
    repoOrder.verify(repoSpy, times(1)).findJobs();
    repoOrder.verify(repoSpy, times(1)).getTransaction();
    repoOrder.verify(repoSpy, times(1)).deleteJobInputs(1, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteJobInputs(2, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteLinkInputs(1, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteLinkInputs(2, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).updateDriverConfig(any(MDriverConfig.class), any(RepositoryTransaction.class));
    repoOrder.verify(repoSpy, times(2)).updateLink(any(MLink.class), any(RepositoryTransaction.class));
    repoOrder.verify(repoSpy, times(2)).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    repoOrder.verifyNoMoreInteractions();
    txOrder.verify(repoTransactionMock, times(1)).begin();
    txOrder.verify(repoTransactionMock, times(1)).commit();
    txOrder.verify(repoTransactionMock, times(1)).close();
    txOrder.verifyNoMoreInteractions();
    upgraderOrder.verify(upgraderMock, times(2)).upgrade(any(MConnectionForms.class), any(MConnectionForms.class));
    upgraderOrder.verify(upgraderMock, times(2)).upgrade(any(MJobForms.class), any(MJobForms.class));
    upgraderOrder.verifyNoMoreInteractions();
    validatorOrder.verify(validatorMock, times(2)).validateLink(anyObject());
    validatorOrder.verify(validatorMock, times(2)).validateJob(anyObject());
    validatorOrder.verifyNoMoreInteractions();
  }

  /**
   * Test the driverConfig upgrade procedure, when all the links and
   * jobs using the old connector are invalid for the new connector
   */
  @Test
  public void testDriverConfigUpgradeWithInvalidLinksAndJobs() {
    MDriverConfig newDriverConfig = driverConfig();

    when(validatorMock.validateLink(any(MLink.class))).thenReturn(invalidRepoMock);
    when(validatorMock.validateJob(any(MJob.class))).thenReturn(invalidRepoMock);
    when(driverMock.getValidator()).thenReturn(validatorMock);
    when(driverMock.getDriverConfigRepositoryUpgrader()).thenReturn(upgraderMock);
    when(driverMock.getLinkConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(driverMock.getJobConfigurationClass()).thenReturn(JobConfiguration.class);

    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));

    doReturn(linkList).when(repoSpy).findLinks();
    doReturn(jobList).when(repoSpy).findJobs();
    doNothing().when(repoSpy).updateLink(any(MLink.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).updateDriverConfig(any(MDriverConfig.class), any(RepositoryTransaction.class));

    try {
      repoSpy.upgradeDriverConfig(newDriverConfig);
    } catch (SqoopException ex) {
      assertEquals(ex.getErrorCode(), RepositoryError.JDBCREPO_0027);

      InOrder repoOrder = inOrder(repoSpy);
      InOrder txOrder = inOrder(repoTransactionMock);
      InOrder upgraderOrder = inOrder(upgraderMock);
      InOrder validatorOrder = inOrder(validatorMock);

      repoOrder.verify(repoSpy, times(1)).findLinks();
      repoOrder.verify(repoSpy, times(1)).findJobs();
      repoOrder.verify(repoSpy, times(1)).getTransaction();
      repoOrder.verify(repoSpy, times(1)).deleteJobInputs(1, repoTransactionMock);
      repoOrder.verify(repoSpy, times(1)).deleteJobInputs(2, repoTransactionMock);
      repoOrder.verify(repoSpy, times(1)).deleteLinkInputs(1, repoTransactionMock);
      repoOrder.verify(repoSpy, times(1)).deleteLinkInputs(2, repoTransactionMock);
      repoOrder.verify(repoSpy, times(1)).updateDriverConfig(any(MDriverConfig.class), any(RepositoryTransaction.class));
      repoOrder.verifyNoMoreInteractions();
      txOrder.verify(repoTransactionMock, times(1)).begin();
      txOrder.verify(repoTransactionMock, times(1)).rollback();
      txOrder.verify(repoTransactionMock, times(1)).close();
      txOrder.verifyNoMoreInteractions();
      upgraderOrder.verify(upgraderMock, times(2)).upgrade(any(MConnectionForms.class), any(MConnectionForms.class));
      upgraderOrder.verify(upgraderMock, times(2)).upgrade(any(MJobForms.class), any(MJobForms.class));
      upgraderOrder.verifyNoMoreInteractions();
      validatorOrder.verify(validatorMock, times(2)).validateLink(anyObject());
      validatorOrder.verify(validatorMock, times(2)).validateJob(anyObject());
      validatorOrder.verifyNoMoreInteractions();
      return ;
    }

    fail("Should throw out an exception with code: " + RepositoryError.JDBCREPO_0027);
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * find links for a given connector
   */
  @Test
  public void testConnectorUpgradeHandlerFindLinksForConnectorError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getValidator()).thenReturn(validatorMock);
    when(sqconnector.getRepositoryUpgrader()).thenReturn(upgraderMock);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

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
  public void testConnectorUpgradeHandlerFindJobsForConnectorError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getValidator()).thenReturn(validatorMock);
    when(sqconnector.getRepositoryUpgrader()).thenReturn(upgraderMock);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

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
  public void testConnectorUpgradeHandlerDeleteJobInputsError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getValidator()).thenReturn(validatorMock);
    when(sqconnector.getRepositoryUpgrader()).thenReturn(upgraderMock);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

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
  public void testConnectorUpgradeHandlerDeleteLinkInputsError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getValidator()).thenReturn(validatorMock);
    when(sqconnector.getRepositoryUpgrader()).thenReturn(upgraderMock);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

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
  public void testConnectorUpgradeHandlerUpdateConnectorError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getValidator()).thenReturn(validatorMock);
    when(sqconnector.getRepositoryUpgrader()).thenReturn(upgraderMock);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(linkList).when(repoHandlerMock).findLinksForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobsForConnector(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteLinkInputs(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update connector error.");
    doThrow(exception).when(repoHandlerMock).updateConnector(any(MConnector.class), any(Connection.class));

    try {
      repoSpy.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findLinksForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteLinkInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateConnector(any(MConnector.class), any(Connection.class));
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
  public void testConnectorUpgradeHandlerUpdateLinkError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(validatorMock.validateLink(any(MLink.class))).thenReturn(validRepoMock);
    when(validatorMock.validateJob(any(MJob.class))).thenReturn(validRepoMock);
    when(sqconnector.getValidator()).thenReturn(validatorMock);
    when(sqconnector.getRepositoryUpgrader()).thenReturn(upgraderMock);
    when(sqconnector.getLinkConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(sqconnector.getJobConfigurationClass(any(Direction.class))).thenReturn(JobConfiguration.class);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(linkList).when(repoHandlerMock).findLinksForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobsForConnector(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteLinkInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).updateConnector(any(MConnector.class), any(Connection.class));
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
      verify(repoHandlerMock, times(1)).updateConnector(any(MConnector.class), any(Connection.class));
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
  public void testConnectorUpgradeHandlerUpdateJobError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(validatorMock.validateLink(any(MLink.class))).thenReturn(validRepoMock);
    when(validatorMock.validateJob(any(MJob.class))).thenReturn(validRepoMock);
    when(sqconnector.getValidator()).thenReturn(validatorMock);
    when(sqconnector.getRepositoryUpgrader()).thenReturn(upgraderMock);
    when(sqconnector.getLinkConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(sqconnector.getJobConfigurationClass(any(Direction.class))).thenReturn(JobConfiguration.class);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(linkList).when(repoHandlerMock).findLinksForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobsForConnector(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteLinkInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).updateConnector(any(MConnector.class), any(Connection.class));
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
      verify(repoHandlerMock, times(1)).updateConnector(any(MConnector.class), any(Connection.class));
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
   * find links for driverConfig
   */
  @Test
  public void testDriverConfigUpgradeHandlerFindLinksError() {
    MDriverConfig newDriverConfig = driverConfig();

    when(driverMock.getValidator()).thenReturn(validatorMock);
    when(driverMock.getDriverConfigRepositoryUpgrader()).thenReturn(upgraderMock);

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "find links error.");
    doThrow(exception).when(repoHandlerMock).findLinks(any(Connection.class));

    try {
      repoSpy.upgradeDriverConfig(newDriverConfig);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findLinks(any(Connection.class));
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
  public void testDriverConfigUpgradeHandlerFindJobsError() {
    MDriverConfig newDriverConfig = driverConfig();

    when(driverMock.getValidator()).thenReturn(validatorMock);
    when(driverMock.getDriverConfigRepositoryUpgrader()).thenReturn(upgraderMock);

    List<MLink> linkList = links(link(1,1), link(2,1));
    doReturn(linkList).when(repoHandlerMock).findLinks(any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "find jobs error.");
    doThrow(exception).when(repoHandlerMock).findJobs(any(Connection.class));

    try {
      repoSpy.upgradeDriverConfig(newDriverConfig);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findLinks(any(Connection.class));
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
  public void testDriverConfigUpgradeHandlerDeleteJobInputsError() {
    MDriverConfig newDriverConfig = driverConfig();

    when(driverMock.getValidator()).thenReturn(validatorMock);
    when(driverMock.getDriverConfigRepositoryUpgrader()).thenReturn(upgraderMock);

    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(linkList).when(repoHandlerMock).findLinks(any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobs(any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "delete job inputs error.");
    doThrow(exception).when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));

    try {
      repoSpy.upgradeDriverConfig(newDriverConfig);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findLinks(any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verify(repoHandlerMock, times(1)).deleteJobInputs(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * delete link inputs for driverConfig upgrade
   */
  @Test
  public void testDriverConfigUpgradeHandlerDeleteConnectionInputsError() {
    MDriverConfig newDriverConfig = driverConfig();

    when(driverMock.getValidator()).thenReturn(validatorMock);
    when(driverMock.getDriverConfigRepositoryUpgrader()).thenReturn(upgraderMock);

    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(linkList).when(repoHandlerMock).findLinks(any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobs(any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "delete link inputs error.");
    doThrow(exception).when(repoHandlerMock).deleteLinkInputs(anyLong(), any(Connection.class));

    try {
      repoSpy.upgradeDriverConfig(newDriverConfig);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findLinks(any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).deleteLinkInputs(anyLong(), any(Connection.class));
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
  public void testDriverConfigUpgradeHandlerUpdateDriverConfigError() {
    MDriverConfig newDriverConfig = driverConfig();

    when(driverMock.getValidator()).thenReturn(validatorMock);
    when(driverMock.getDriverConfigRepositoryUpgrader()).thenReturn(upgraderMock);

    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(linkList).when(repoHandlerMock).findLinks(any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobs(any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteLinkInputs(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update driverConfig entity error.");
    doThrow(exception).when(repoHandlerMock).updateDriverConfig(any(MDriverConfig.class), any(Connection.class));

    try {
      repoSpy.upgradeDriverConfig(newDriverConfig);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findLinks(any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteLinkInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateDriverConfig(any(MDriverConfig.class), any(Connection.class));
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
  public void testDriverConfigUpgradeHandlerUpdateConnectionError() {
    MDriverConfig newDriverConfig = driverConfig();

    when(validatorMock.validateLink(any(MLink.class))).thenReturn(validRepoMock);
    when(validatorMock.validateJob(any(MJob.class))).thenReturn(validRepoMock);
    when(driverMock.getValidator()).thenReturn(validatorMock);
    when(driverMock.getDriverConfigRepositoryUpgrader()).thenReturn(upgraderMock);
    when(driverMock.getLinkConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(driverMock.getJobConfigurationClass()).thenReturn(JobConfiguration.class);

    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(linkList).when(repoHandlerMock).findLinks(any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobs(any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteLinkInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).updateDriverConfig(any(MDriverConfig.class), any(Connection.class));
    doReturn(true).when(repoHandlerMock).existsLink(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update link error.");
    doThrow(exception).when(repoHandlerMock).updateLink(any(MLink.class), any(Connection.class));

    try {
      repoSpy.upgradeDriverConfig(newDriverConfig);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findLinks(any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteLinkInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateDriverConfig(any(MDriverConfig.class), any(Connection.class));
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
  public void testDriverConfigUpgradeHandlerUpdateJobError() {
    MDriverConfig driverConfig = driverConfig();

    when(validatorMock.validateLink(any(MLink.class))).thenReturn(validRepoMock);
    when(validatorMock.validateJob(any(MJob.class))).thenReturn(validRepoMock);
    when(driverMock.getValidator()).thenReturn(validatorMock);
    when(driverMock.getDriverConfigRepositoryUpgrader()).thenReturn(upgraderMock);
    when(driverMock.getLinkConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(driverMock.getJobConfigurationClass()).thenReturn(JobConfiguration.class);

    List<MLink> linkList = links(link(1,1), link(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(linkList).when(repoHandlerMock).findLinks(any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobs(any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteLinkInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).updateDriverConfig(any(MDriverConfig.class), any(Connection.class));
    doReturn(true).when(repoHandlerMock).existsLink(anyLong(), any(Connection.class));
    doReturn(true).when(repoHandlerMock).existsJob(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).updateLink(any(MLink.class), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update job error.");
    doThrow(exception).when(repoHandlerMock).updateJob(any(MJob.class), any(Connection.class));

    try {
      repoSpy.upgradeDriverConfig(driverConfig);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findLinks(any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteLinkInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateDriverConfig(any(MDriverConfig.class), any(Connection.class));
      verify(repoHandlerMock, times(2)).existsLink(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).updateLink(any(MLink.class), any(Connection.class));
      verify(repoHandlerMock, times(1)).existsJob(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateJob(any(MJob.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  private MConnector connector(long connectorId, String version) {
    MConnector connector = new MConnector("A" + connectorId, "A" + connectorId, version + connectorId,
        new MConnectionForms(new LinkedList<MForm>()),
        new MJobForms(FormUtils.toForms(JobConfiguration.class)),
        new MJobForms(FormUtils.toForms(JobConfiguration.class)));
    connector.setPersistenceId(connectorId);
    return connector;
  }

  private MConnector connector(long connectoId) {
    return connector(connectoId, "1.0");
  }

  private MDriverConfig driverConfig() {
    MDriverConfig driverConfig = new MDriverConfig(
        new MConnectionForms(new LinkedList<MForm>()),
        new MJobForms(FormUtils.toForms(JobConfiguration.class)),
        Driver.CURRENT_DRIVER_VERSION);
    driverConfig.setPersistenceId(1);
    return driverConfig;
  }

  private MDriverConfig anotherDriverConfig() {
    MDriverConfig driverConfig = new MDriverConfig(null, null,
      Driver.CURRENT_DRIVER_VERSION);
    driverConfig.setPersistenceId(1);
    return driverConfig;
  }

  private MLink link(long linkId, long connectorId) {
    MLink link = new MLink(connectorId, new MConnectionForms(new LinkedList<MForm>()),
        new MConnectionForms(new LinkedList<MForm>()));
    link.setPersistenceId(linkId);
    return link;
  }

  private MJob job(long id, long fromConnectorId, long toConnectorId, long fromLinkId, long toLinkId) {
    MJob job = new MJob(fromConnectorId, toConnectorId, fromLinkId, toLinkId,
        new MJobForms(new LinkedList<MForm>()),
        new MJobForms(new LinkedList<MForm>()),
        new MJobForms(new LinkedList<MForm>()));
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
  public static class EmptyConfigurationClass {
  }
}
