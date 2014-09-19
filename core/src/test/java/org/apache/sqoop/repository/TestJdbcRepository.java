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
import org.apache.sqoop.connector.spi.MetadataUpgrader;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.framework.FrameworkManager;
import org.apache.sqoop.framework.configuration.JobConfiguration;
import org.apache.sqoop.model.ConfigurationClass;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;

import org.apache.sqoop.validation.Validator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestJdbcRepository {

  private JdbcRepository repoSpy;
  private JdbcRepositoryTransaction repoTransactionMock;
  private ConnectorManager connectorMgrMock;
  private FrameworkManager frameworkMgrMock;
  private JdbcRepositoryHandler repoHandlerMock;
  private Validator validatorMock;
  private MetadataUpgrader upgraderMock;

  private Validation validRepoMock;
  private Validation invalidRepoMock;

  @Before
  public void setUp() throws Exception {
    repoTransactionMock = mock(JdbcRepositoryTransaction.class);
    connectorMgrMock = mock(ConnectorManager.class);
    frameworkMgrMock = mock(FrameworkManager.class);
    repoHandlerMock = mock(JdbcRepositoryHandler.class);
    validatorMock = mock(Validator.class);
    upgraderMock = mock(MetadataUpgrader.class);
    repoSpy = spy(new JdbcRepository(repoHandlerMock, null));

    // setup transaction and connector manager
    doReturn(repoTransactionMock).when(repoSpy).getTransaction();
    ConnectorManager.setInstance(connectorMgrMock);
    FrameworkManager.setInstance(frameworkMgrMock);

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
   * Test the procedure when the framework auto upgrade option is enabled
   */
  @Test
  public void testFrameworkEnableAutoUpgrade() {
    MFramework newFramework = framework();
    MFramework oldFramework = anotherFramework();

    when(repoHandlerMock.findFramework(any(Connection.class))).thenReturn(oldFramework);

    // make the upgradeFramework to throw an exception to prove that it has been called
    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "upgradeFramework() has been called.");
    doThrow(exception).when(repoHandlerMock).findConnections(any(Connection.class));

    try {
      repoSpy.registerFramework(newFramework, true);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findFramework(any(Connection.class));
      verify(repoHandlerMock, times(1)).findConnections(any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the procedure when the framework auto upgrade option is disabled
   */
  @Test
  public void testFrameworkDisableAutoUpgrade() {
    MFramework newFramework = framework();
    MFramework oldFramework = anotherFramework();

    when(repoHandlerMock.findFramework(any(Connection.class))).thenReturn(oldFramework);

    try {
      repoSpy.registerFramework(newFramework, false);
    } catch (SqoopException ex) {
      assertEquals(ex.getErrorCode(), RepositoryError.JDBCREPO_0026);
      verify(repoHandlerMock, times(1)).findFramework(any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with code: " + RepositoryError.JDBCREPO_0026);
  }

  /**
   * Test the connector upgrade procedure, when all the connections and
   * jobs using the old connector are still valid for the new connector
   */
  @Test
  public void testConnectorUpgradeWithValidConnectionsAndJobs() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    // prepare the sqoop connector
    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(validatorMock.validateConnection(any(MConnection.class))).thenReturn(validRepoMock);
    when(validatorMock.validateJob(any(MJob.class))).thenReturn(validRepoMock);
    when(sqconnector.getValidator()).thenReturn(validatorMock);
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgraderMock);
    when(sqconnector.getConnectionConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(sqconnector.getJobConfigurationClass(any(Direction.class))).thenReturn(JobConfiguration.class);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

    // prepare the connections and jobs
    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));

    // mock necessary methods for upgradeConnector() procedure
    doReturn(connectionList).when(repoSpy).findConnectionsForConnector(anyLong());
    doReturn(jobList).when(repoSpy).findJobsForConnector(anyLong());
    doNothing().when(repoSpy).updateConnection(any(MConnection.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).updateConnector(any(MConnector.class), any(RepositoryTransaction.class));

    repoSpy.upgradeConnector(oldConnector, newConnector);

    InOrder repoOrder = inOrder(repoSpy);
    InOrder txOrder = inOrder(repoTransactionMock);
    InOrder upgraderOrder = inOrder(upgraderMock);
    InOrder validatorOrder = inOrder(validatorMock);

    repoOrder.verify(repoSpy, times(1)).findConnectionsForConnector(anyLong());
    repoOrder.verify(repoSpy, times(1)).findJobsForConnector(anyLong());
    repoOrder.verify(repoSpy, times(1)).getTransaction();
    repoOrder.verify(repoSpy, times(1)).deleteJobInputs(1, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteJobInputs(2, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteConnectionInputs(1, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteConnectionInputs(2, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).updateConnector(any(MConnector.class), any(RepositoryTransaction.class));
    repoOrder.verify(repoSpy, times(2)).updateConnection(any(MConnection.class), any(RepositoryTransaction.class));
    repoOrder.verify(repoSpy, times(4)).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    repoOrder.verifyNoMoreInteractions();
    txOrder.verify(repoTransactionMock, times(1)).begin();
    txOrder.verify(repoTransactionMock, times(1)).commit();
    txOrder.verify(repoTransactionMock, times(1)).close();
    txOrder.verifyNoMoreInteractions();
    upgraderOrder.verify(upgraderMock, times(2)).upgrade(any(MConnectionForms.class), any(MConnectionForms.class));
    upgraderOrder.verify(upgraderMock, times(4)).upgrade(any(MJobForms.class), any(MJobForms.class));
    upgraderOrder.verifyNoMoreInteractions();
    validatorOrder.verify(validatorMock, times(2)).validateConnection(anyObject());
    // @TODO(Abe): Re-enable job validation?
    validatorOrder.verify(validatorMock, times(0)).validateJob(anyObject());
    validatorOrder.verifyNoMoreInteractions();
  }

  /**
   * @TODO(Abe): To re-enable with Validation in Repository upgrade.
   * Test the connector upgrade procedure, when all the connections and
   * jobs using the old connector are invalid for the new connector
   */
//  @Test
//  public void testConnectorUpgradeWithInvalidConnectionsAndJobs() {
//    MConnector newConnector = connector(1, "1.1");
//    MConnector oldConnector = connector(1);
//
//    // prepare the sqoop connector
//    SqoopConnector sqconnector = mock(SqoopConnector.class);
//    when(validator.validateConnection(any(MConnection.class))).thenReturn(invalid);
//    when(validator.validateJob(any(MJob.class))).thenReturn(invalid);
//    when(sqconnector.getValidator()).thenReturn(validator);
//    when(sqconnector.getMetadataUpgrader()).thenReturn(upgrader);
//    when(sqconnector.getConnectionConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
//    when(sqconnector.getJobConfigurationClass(any(Direction.class))).thenReturn(JobConfiguration.class);
//    when(connectorMgr.getConnector(anyString())).thenReturn(sqconnector);
//
//    // prepare the connections and jobs
//    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
//    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
//
//    doReturn(connectionList).when(repo).findConnectionsForConnector(anyLong());
//    doReturn(jobList).when(repo).findJobsForConnector(anyLong());
//    doNothing().when(repo).updateConnection(any(MConnection.class), any(RepositoryTransaction.class));
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
//      repoOrder.verify(repo, times(1)).findConnectionsForConnector(anyLong());
//      repoOrder.verify(repo, times(1)).findJobsForConnector(anyLong());
//      repoOrder.verify(repo, times(1)).getTransaction();
//      repoOrder.verify(repo, times(1)).deleteJobInputs(1, tx);
//      repoOrder.verify(repo, times(1)).deleteJobInputs(2, tx);
//      repoOrder.verify(repo, times(1)).deleteConnectionInputs(1, tx);
//      repoOrder.verify(repo, times(1)).deleteConnectionInputs(2, tx);
//      repoOrder.verify(repo, times(1)).updateConnector(any(MConnector.class), any(RepositoryTransaction.class));
//      repoOrder.verifyNoMoreInteractions();
//      txOrder.verify(tx, times(1)).begin();
//      txOrder.verify(tx, times(1)).rollback();
//      txOrder.verify(tx, times(1)).close();
//      txOrder.verifyNoMoreInteractions();
//      upgraderOrder.verify(upgrader, times(2)).upgrade(any(MConnectionForms.class), any(MConnectionForms.class));
//      upgraderOrder.verify(upgrader, times(2)).upgrade(any(MJobForms.class), any(MJobForms.class));
//      upgraderOrder.verifyNoMoreInteractions();
//      validatorOrder.verify(validator, times(2)).validateConnection(anyObject());
//      validatorOrder.verify(validator, times(2)).validateJob(anyObject());
//      validatorOrder.verifyNoMoreInteractions();
//      return ;
//    }
//
//    fail("Should throw out an exception with code: " + RepositoryError.JDBCREPO_0027);
//  }

  /**
   * Test the framework upgrade procedure, when all the connections and
   * jobs using the old connector are still valid for the new connector
   */
  @Test
  public void testFrameworkUpgradeWithValidConnectionsAndJobs() {
    MFramework newFramework = framework();

    when(validatorMock.validateConnection(any(MConnection.class))).thenReturn(validRepoMock);
    when(validatorMock.validateJob(any(MJob.class))).thenReturn(validRepoMock);
    when(frameworkMgrMock.getValidator()).thenReturn(validatorMock);
    when(frameworkMgrMock.getMetadataUpgrader()).thenReturn(upgraderMock);
    when(frameworkMgrMock.getConnectionConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(frameworkMgrMock.getJobConfigurationClass()).thenReturn(JobConfiguration.class);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));

    doReturn(connectionList).when(repoSpy).findConnections();
    doReturn(jobList).when(repoSpy).findJobs();
    doNothing().when(repoSpy).updateConnection(any(MConnection.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).updateFramework(any(MFramework.class), any(RepositoryTransaction.class));

    repoSpy.upgradeFramework(newFramework);

    InOrder repoOrder = inOrder(repoSpy);
    InOrder txOrder = inOrder(repoTransactionMock);
    InOrder upgraderOrder = inOrder(upgraderMock);
    InOrder validatorOrder = inOrder(validatorMock);

    repoOrder.verify(repoSpy, times(1)).findConnections();
    repoOrder.verify(repoSpy, times(1)).findJobs();
    repoOrder.verify(repoSpy, times(1)).getTransaction();
    repoOrder.verify(repoSpy, times(1)).deleteJobInputs(1, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteJobInputs(2, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteConnectionInputs(1, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).deleteConnectionInputs(2, repoTransactionMock);
    repoOrder.verify(repoSpy, times(1)).updateFramework(any(MFramework.class), any(RepositoryTransaction.class));
    repoOrder.verify(repoSpy, times(2)).updateConnection(any(MConnection.class), any(RepositoryTransaction.class));
    repoOrder.verify(repoSpy, times(2)).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    repoOrder.verifyNoMoreInteractions();
    txOrder.verify(repoTransactionMock, times(1)).begin();
    txOrder.verify(repoTransactionMock, times(1)).commit();
    txOrder.verify(repoTransactionMock, times(1)).close();
    txOrder.verifyNoMoreInteractions();
    upgraderOrder.verify(upgraderMock, times(2)).upgrade(any(MConnectionForms.class), any(MConnectionForms.class));
    upgraderOrder.verify(upgraderMock, times(2)).upgrade(any(MJobForms.class), any(MJobForms.class));
    upgraderOrder.verifyNoMoreInteractions();
    validatorOrder.verify(validatorMock, times(2)).validateConnection(anyObject());
    validatorOrder.verify(validatorMock, times(2)).validateJob(anyObject());
    validatorOrder.verifyNoMoreInteractions();
  }

  /**
   * Test the framework upgrade procedure, when all the connections and
   * jobs using the old connector are invalid for the new connector
   */
  @Test
  public void testFrameworkUpgradeWithInvalidConnectionsAndJobs() {
    MFramework newFramework = framework();

    when(validatorMock.validateConnection(any(MConnection.class))).thenReturn(invalidRepoMock);
    when(validatorMock.validateJob(any(MJob.class))).thenReturn(invalidRepoMock);
    when(frameworkMgrMock.getValidator()).thenReturn(validatorMock);
    when(frameworkMgrMock.getMetadataUpgrader()).thenReturn(upgraderMock);
    when(frameworkMgrMock.getConnectionConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(frameworkMgrMock.getJobConfigurationClass()).thenReturn(JobConfiguration.class);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));

    doReturn(connectionList).when(repoSpy).findConnections();
    doReturn(jobList).when(repoSpy).findJobs();
    doNothing().when(repoSpy).updateConnection(any(MConnection.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    doNothing().when(repoSpy).updateFramework(any(MFramework.class), any(RepositoryTransaction.class));

    try {
      repoSpy.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getErrorCode(), RepositoryError.JDBCREPO_0027);

      InOrder repoOrder = inOrder(repoSpy);
      InOrder txOrder = inOrder(repoTransactionMock);
      InOrder upgraderOrder = inOrder(upgraderMock);
      InOrder validatorOrder = inOrder(validatorMock);

      repoOrder.verify(repoSpy, times(1)).findConnections();
      repoOrder.verify(repoSpy, times(1)).findJobs();
      repoOrder.verify(repoSpy, times(1)).getTransaction();
      repoOrder.verify(repoSpy, times(1)).deleteJobInputs(1, repoTransactionMock);
      repoOrder.verify(repoSpy, times(1)).deleteJobInputs(2, repoTransactionMock);
      repoOrder.verify(repoSpy, times(1)).deleteConnectionInputs(1, repoTransactionMock);
      repoOrder.verify(repoSpy, times(1)).deleteConnectionInputs(2, repoTransactionMock);
      repoOrder.verify(repoSpy, times(1)).updateFramework(any(MFramework.class), any(RepositoryTransaction.class));
      repoOrder.verifyNoMoreInteractions();
      txOrder.verify(repoTransactionMock, times(1)).begin();
      txOrder.verify(repoTransactionMock, times(1)).rollback();
      txOrder.verify(repoTransactionMock, times(1)).close();
      txOrder.verifyNoMoreInteractions();
      upgraderOrder.verify(upgraderMock, times(2)).upgrade(any(MConnectionForms.class), any(MConnectionForms.class));
      upgraderOrder.verify(upgraderMock, times(2)).upgrade(any(MJobForms.class), any(MJobForms.class));
      upgraderOrder.verifyNoMoreInteractions();
      validatorOrder.verify(validatorMock, times(2)).validateConnection(anyObject());
      validatorOrder.verify(validatorMock, times(2)).validateJob(anyObject());
      validatorOrder.verifyNoMoreInteractions();
      return ;
    }

    fail("Should throw out an exception with code: " + RepositoryError.JDBCREPO_0027);
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * find connections for a given connector
   */
  @Test
  public void testConnectorUpgradeHandlerFindConnectionsForConnectorError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getValidator()).thenReturn(validatorMock);
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgraderMock);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "find connections for connector error.");
    doThrow(exception).when(repoHandlerMock).findConnectionsForConnector(anyLong(), any(Connection.class));

    try {
      repoSpy.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnectionsForConnector(anyLong(), any(Connection.class));
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
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgraderMock);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    doReturn(connectionList).when(repoHandlerMock).findConnectionsForConnector(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "find jobs for connector error.");
    doThrow(exception).when(repoHandlerMock).findJobsForConnector(anyLong(), any(Connection.class));

    try {
      repoSpy.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnectionsForConnector(anyLong(), any(Connection.class));
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
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgraderMock);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandlerMock).findConnectionsForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobsForConnector(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "delete job inputs for connector error.");
    doThrow(exception).when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));

    try {
      repoSpy.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnectionsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).deleteJobInputs(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * delete connection inputs for a given connector
   */
  @Test
  public void testConnectorUpgradeHandlerDeleteConnectionInputsError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getValidator()).thenReturn(validatorMock);
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgraderMock);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandlerMock).findConnectionsForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobsForConnector(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "delete connection inputs for connector error.");
    doThrow(exception).when(repoHandlerMock).deleteConnectionInputs(anyLong(), any(Connection.class));

    try {
      repoSpy.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnectionsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * update the connector metadata
   */
  @Test
  public void testConnectorUpgradeHandlerUpdateConnectorError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(sqconnector.getValidator()).thenReturn(validatorMock);
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgraderMock);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandlerMock).findConnectionsForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobsForConnector(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteConnectionInputs(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update connector error.");
    doThrow(exception).when(repoHandlerMock).updateConnector(any(MConnector.class), any(Connection.class));

    try {
      repoSpy.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnectionsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateConnector(any(MConnector.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * update the connection metadata
   */
  @Test
  public void testConnectorUpgradeHandlerUpdateConnectionError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(validatorMock.validateConnection(any(MConnection.class))).thenReturn(validRepoMock);
    when(validatorMock.validateJob(any(MJob.class))).thenReturn(validRepoMock);
    when(sqconnector.getValidator()).thenReturn(validatorMock);
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgraderMock);
    when(sqconnector.getConnectionConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(sqconnector.getJobConfigurationClass(any(Direction.class))).thenReturn(JobConfiguration.class);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandlerMock).findConnectionsForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobsForConnector(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteConnectionInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).updateConnector(any(MConnector.class), any(Connection.class));
    doReturn(true).when(repoHandlerMock).existsConnection(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update connection error.");
    doThrow(exception).when(repoHandlerMock).updateConnection(any(MConnection.class), any(Connection.class));

    try {
      repoSpy.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnectionsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateConnector(any(MConnector.class), any(Connection.class));
      verify(repoHandlerMock, times(1)).existsConnection(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateConnection(any(MConnection.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * update the job metadata
   */
  @Test
  public void testConnectorUpgradeHandlerUpdateJobError() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1);

    SqoopConnector sqconnector = mock(SqoopConnector.class);
    when(validatorMock.validateConnection(any(MConnection.class))).thenReturn(validRepoMock);
    when(validatorMock.validateJob(any(MJob.class))).thenReturn(validRepoMock);
    when(sqconnector.getValidator()).thenReturn(validatorMock);
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgraderMock);
    when(sqconnector.getConnectionConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(sqconnector.getJobConfigurationClass(any(Direction.class))).thenReturn(JobConfiguration.class);
    when(connectorMgrMock.getConnector(anyString())).thenReturn(sqconnector);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandlerMock).findConnectionsForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobsForConnector(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteConnectionInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).updateConnector(any(MConnector.class), any(Connection.class));
    doNothing().when(repoHandlerMock).updateConnection(any(MConnection.class), any(Connection.class));
    doReturn(true).when(repoHandlerMock).existsConnection(anyLong(), any(Connection.class));
    doReturn(true).when(repoHandlerMock).existsJob(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update job error.");
    doThrow(exception).when(repoHandlerMock).updateJob(any(MJob.class), any(Connection.class));

    try {
      repoSpy.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnectionsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateConnector(any(MConnector.class), any(Connection.class));
      verify(repoHandlerMock, times(2)).existsConnection(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).updateConnection(any(MConnection.class), any(Connection.class));
      verify(repoHandlerMock, times(1)).existsJob(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateJob(any(MJob.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * find connections for framework
   */
  @Test
  public void testFrameworkUpgradeHandlerFindConnectionsError() {
    MFramework newFramework = framework();

    when(frameworkMgrMock.getValidator()).thenReturn(validatorMock);
    when(frameworkMgrMock.getMetadataUpgrader()).thenReturn(upgraderMock);

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "find connections error.");
    doThrow(exception).when(repoHandlerMock).findConnections(any(Connection.class));

    try {
      repoSpy.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnections(any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * find jobs for framework
   */
  @Test
  public void testFrameworkUpgradeHandlerFindJobsError() {
    MFramework newFramework = framework();

    when(frameworkMgrMock.getValidator()).thenReturn(validatorMock);
    when(frameworkMgrMock.getMetadataUpgrader()).thenReturn(upgraderMock);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    doReturn(connectionList).when(repoHandlerMock).findConnections(any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "find jobs error.");
    doThrow(exception).when(repoHandlerMock).findJobs(any(Connection.class));

    try {
      repoSpy.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnections(any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * delete job inputs for framework upgrade
   */
  @Test
  public void testFrameworkUpgradeHandlerDeleteJobInputsError() {
    MFramework newFramework = framework();

    when(frameworkMgrMock.getValidator()).thenReturn(validatorMock);
    when(frameworkMgrMock.getMetadataUpgrader()).thenReturn(upgraderMock);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandlerMock).findConnections(any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobs(any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "delete job inputs error.");
    doThrow(exception).when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));

    try {
      repoSpy.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnections(any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verify(repoHandlerMock, times(1)).deleteJobInputs(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * delete connection inputs for framework upgrade
   */
  @Test
  public void testFrameworkUpgradeHandlerDeleteConnectionInputsError() {
    MFramework newFramework = framework();

    when(frameworkMgrMock.getValidator()).thenReturn(validatorMock);
    when(frameworkMgrMock.getMetadataUpgrader()).thenReturn(upgraderMock);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandlerMock).findConnections(any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobs(any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "delete connection inputs error.");
    doThrow(exception).when(repoHandlerMock).deleteConnectionInputs(anyLong(), any(Connection.class));

    try {
      repoSpy.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnections(any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * update the framework metadata
   */
  @Test
  public void testFrameworkUpgradeHandlerUpdateFrameworkError() {
    MFramework newFramework = framework();

    when(frameworkMgrMock.getValidator()).thenReturn(validatorMock);
    when(frameworkMgrMock.getMetadataUpgrader()).thenReturn(upgraderMock);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandlerMock).findConnections(any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobs(any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteConnectionInputs(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update framework metadata error.");
    doThrow(exception).when(repoHandlerMock).updateFramework(any(MFramework.class), any(Connection.class));

    try {
      repoSpy.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnections(any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateFramework(any(MFramework.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * update the connection metadata
   */
  @Test
  public void testFrameworkUpgradeHandlerUpdateConnectionError() {
    MFramework newFramework = framework();

    when(validatorMock.validateConnection(any(MConnection.class))).thenReturn(validRepoMock);
    when(validatorMock.validateJob(any(MJob.class))).thenReturn(validRepoMock);
    when(frameworkMgrMock.getValidator()).thenReturn(validatorMock);
    when(frameworkMgrMock.getMetadataUpgrader()).thenReturn(upgraderMock);
    when(frameworkMgrMock.getConnectionConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(frameworkMgrMock.getJobConfigurationClass()).thenReturn(JobConfiguration.class);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandlerMock).findConnections(any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobs(any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteConnectionInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).updateFramework(any(MFramework.class), any(Connection.class));
    doReturn(true).when(repoHandlerMock).existsConnection(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update connection error.");
    doThrow(exception).when(repoHandlerMock).updateConnection(any(MConnection.class), any(Connection.class));

    try {
      repoSpy.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnections(any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateFramework(any(MFramework.class), any(Connection.class));
      verify(repoHandlerMock, times(1)).existsConnection(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateConnection(any(MConnection.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  /**
   * Test the exception handling procedure when the database handler fails to
   * update the job metadata
   */
  @Test
  public void testFrameworkUpgradeHandlerUpdateJobError() {
    MFramework newFramework = framework();

    when(validatorMock.validateConnection(any(MConnection.class))).thenReturn(validRepoMock);
    when(validatorMock.validateJob(any(MJob.class))).thenReturn(validRepoMock);
    when(frameworkMgrMock.getValidator()).thenReturn(validatorMock);
    when(frameworkMgrMock.getMetadataUpgrader()).thenReturn(upgraderMock);
    when(frameworkMgrMock.getConnectionConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(frameworkMgrMock.getJobConfigurationClass()).thenReturn(JobConfiguration.class);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandlerMock).findConnections(any(Connection.class));
    doReturn(jobList).when(repoHandlerMock).findJobs(any(Connection.class));
    doNothing().when(repoHandlerMock).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).deleteConnectionInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).updateFramework(any(MFramework.class), any(Connection.class));
    doReturn(true).when(repoHandlerMock).existsConnection(anyLong(), any(Connection.class));
    doReturn(true).when(repoHandlerMock).existsJob(anyLong(), any(Connection.class));
    doNothing().when(repoHandlerMock).updateConnection(any(MConnection.class), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update job error.");
    doThrow(exception).when(repoHandlerMock).updateJob(any(MJob.class), any(Connection.class));

    try {
      repoSpy.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandlerMock, times(1)).findConnections(any(Connection.class));
      verify(repoHandlerMock, times(1)).findJobs(any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateFramework(any(MFramework.class), any(Connection.class));
      verify(repoHandlerMock, times(2)).existsConnection(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(2)).updateConnection(any(MConnection.class), any(Connection.class));
      verify(repoHandlerMock, times(1)).existsJob(anyLong(), any(Connection.class));
      verify(repoHandlerMock, times(1)).updateJob(any(MJob.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandlerMock);
      return ;
    }

    fail("Should throw out an exception with message: " + exception.getMessage());
  }

  private MConnector connector(long id, String version) {
    MConnector connector = new MConnector("A" + id, "A" + id, version + id,
        new MConnectionForms(new LinkedList<MForm>()),
        new MJobForms(FormUtils.toForms(JobConfiguration.class)),
        new MJobForms(FormUtils.toForms(JobConfiguration.class)));
    connector.setPersistenceId(id);
    return connector;
  }

  private MConnector connector(long id) {
    return connector(id, "1.0");
  }

  private MFramework framework() {
    MFramework framework = new MFramework(
        new MConnectionForms(new LinkedList<MForm>()),
        new MJobForms(FormUtils.toForms(JobConfiguration.class)),
        FrameworkManager.CURRENT_FRAMEWORK_VERSION);
    framework.setPersistenceId(1);
    return framework;
  }

  private MFramework anotherFramework() {
    MFramework framework = new MFramework(null, null,
      FrameworkManager.CURRENT_FRAMEWORK_VERSION);
    framework.setPersistenceId(1);
    return framework;
  }

  private MConnection connection(long id, long cid) {
    MConnection connection = new MConnection(cid, new MConnectionForms(new LinkedList<MForm>()),
        new MConnectionForms(new LinkedList<MForm>()));
    connection.setPersistenceId(id);
    return connection;
  }

  private MJob job(long id, long fromConnectorId, long toConnectorId, long fromConnectionId, long toConnectionId) {
    MJob job = new MJob(fromConnectorId, toConnectorId, fromConnectionId, toConnectionId,
        new MJobForms(new LinkedList<MForm>()),
        new MJobForms(new LinkedList<MForm>()),
        new MJobForms(new LinkedList<MForm>()));
    job.setPersistenceId(id);
    return job;
  }

  private List<MConnection> connections(MConnection ... cs) {
    List<MConnection> connections = new ArrayList<MConnection>();
    Collections.addAll(connections, cs);
    return connections;
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
