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

  private JdbcRepository repo;
  private JdbcRepositoryTransaction tx;
  private ConnectorManager connectorMgr;
  private FrameworkManager frameworkMgr;
  private JdbcRepositoryHandler repoHandler;
  private Validator validator;
  private MetadataUpgrader upgrader;

  private Validation valid;
  private Validation invalid;

  @Before
  public void setUp() throws Exception {
    tx = mock(JdbcRepositoryTransaction.class);
    connectorMgr = mock(ConnectorManager.class);
    frameworkMgr = mock(FrameworkManager.class);
    repoHandler = mock(JdbcRepositoryHandler.class);
    validator = mock(Validator.class);
    upgrader = mock(MetadataUpgrader.class);
    repo = spy(new JdbcRepository(repoHandler, null));

    // setup transaction and connector manager
    doReturn(tx).when(repo).getTransaction();
    ConnectorManager.setInstance(connectorMgr);
    FrameworkManager.setInstance(frameworkMgr);

    valid = mock(Validation.class);
    when(valid.getStatus()).thenReturn(Status.ACCEPTABLE);
    invalid = mock(Validation.class);
    when(invalid.getStatus()).thenReturn(Status.UNACCEPTABLE);

    doNothing().when(upgrader).upgrade(any(MConnectionForms.class), any(MConnectionForms.class));
    doNothing().when(upgrader).upgrade(any(MJobForms.class), any(MJobForms.class));
  }

  /**
   * Test the procedure when the connector auto upgrade option is enabled
   */
  @Test
  public void testConnectorEnableAutoUpgrade() {
    MConnector newConnector = connector(1, "1.1");
    MConnector oldConnector = connector(1, "1.0");

    when(repoHandler.findConnector(anyString(), any(Connection.class))).thenReturn(oldConnector);

    // make the upgradeConnector to throw an exception to prove that it has been called
    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "upgradeConnector() has been called.");
    doThrow(exception).when(connectorMgr).getConnector(anyString());

    try {
      repo.registerConnector(newConnector, true);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findConnector(anyString(), any(Connection.class));
      verify(connectorMgr, times(1)).getConnector(anyString());
      verifyNoMoreInteractions(repoHandler);
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

    when(repoHandler.findConnector(anyString(), any(Connection.class))).thenReturn(oldConnector);

    try {
      repo.registerConnector(newConnector, false);
    } catch (SqoopException ex) {
      verify(repoHandler, times(1)).findConnector(anyString(), any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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

    when(repoHandler.findFramework(any(Connection.class))).thenReturn(oldFramework);

    // make the upgradeFramework to throw an exception to prove that it has been called
    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "upgradeFramework() has been called.");
    doThrow(exception).when(repoHandler).findConnections(any(Connection.class));

    try {
      repo.registerFramework(newFramework, true);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findFramework(any(Connection.class));
      verify(repoHandler, times(1)).findConnections(any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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

    when(repoHandler.findFramework(any(Connection.class))).thenReturn(oldFramework);

    try {
      repo.registerFramework(newFramework, false);
    } catch (SqoopException ex) {
      assertEquals(ex.getErrorCode(), RepositoryError.JDBCREPO_0026);
      verify(repoHandler, times(1)).findFramework(any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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
    when(validator.validateConnection(any(MConnection.class))).thenReturn(valid);
    when(validator.validateJob(any(MJob.class))).thenReturn(valid);
    when(sqconnector.getValidator()).thenReturn(validator);
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgrader);
    when(sqconnector.getConnectionConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(sqconnector.getJobConfigurationClass(any(Direction.class))).thenReturn(JobConfiguration.class);
    when(connectorMgr.getConnector(anyString())).thenReturn(sqconnector);

    // prepare the connections and jobs
    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));

    // mock necessary methods for upgradeConnector() procedure
    doReturn(connectionList).when(repo).findConnectionsForConnector(anyLong());
    doReturn(jobList).when(repo).findJobsForConnector(anyLong());
    doNothing().when(repo).updateConnection(any(MConnection.class), any(RepositoryTransaction.class));
    doNothing().when(repo).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    doNothing().when(repo).updateConnector(any(MConnector.class), any(RepositoryTransaction.class));

    repo.upgradeConnector(oldConnector, newConnector);

    InOrder repoOrder = inOrder(repo);
    InOrder txOrder = inOrder(tx);
    InOrder upgraderOrder = inOrder(upgrader);
    InOrder validatorOrder = inOrder(validator);

    repoOrder.verify(repo, times(1)).findConnectionsForConnector(anyLong());
    repoOrder.verify(repo, times(1)).findJobsForConnector(anyLong());
    repoOrder.verify(repo, times(1)).getTransaction();
    repoOrder.verify(repo, times(1)).deleteJobInputs(1, tx);
    repoOrder.verify(repo, times(1)).deleteJobInputs(2, tx);
    repoOrder.verify(repo, times(1)).deleteConnectionInputs(1, tx);
    repoOrder.verify(repo, times(1)).deleteConnectionInputs(2, tx);
    repoOrder.verify(repo, times(1)).updateConnector(any(MConnector.class), any(RepositoryTransaction.class));
    repoOrder.verify(repo, times(2)).updateConnection(any(MConnection.class), any(RepositoryTransaction.class));
    repoOrder.verify(repo, times(4)).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    repoOrder.verifyNoMoreInteractions();
    txOrder.verify(tx, times(1)).begin();
    txOrder.verify(tx, times(1)).commit();
    txOrder.verify(tx, times(1)).close();
    txOrder.verifyNoMoreInteractions();
    upgraderOrder.verify(upgrader, times(2)).upgrade(any(MConnectionForms.class), any(MConnectionForms.class));
    upgraderOrder.verify(upgrader, times(4)).upgrade(any(MJobForms.class), any(MJobForms.class));
    upgraderOrder.verifyNoMoreInteractions();
    validatorOrder.verify(validator, times(2)).validateConnection(anyObject());
    // @TODO(Abe): Re-enable job validation?
    validatorOrder.verify(validator, times(0)).validateJob(anyObject());
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

    when(validator.validateConnection(any(MConnection.class))).thenReturn(valid);
    when(validator.validateJob(any(MJob.class))).thenReturn(valid);
    when(frameworkMgr.getValidator()).thenReturn(validator);
    when(frameworkMgr.getMetadataUpgrader()).thenReturn(upgrader);
    when(frameworkMgr.getConnectionConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(frameworkMgr.getJobConfigurationClass()).thenReturn(JobConfiguration.class);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));

    doReturn(connectionList).when(repo).findConnections();
    doReturn(jobList).when(repo).findJobs();
    doNothing().when(repo).updateConnection(any(MConnection.class), any(RepositoryTransaction.class));
    doNothing().when(repo).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    doNothing().when(repo).updateFramework(any(MFramework.class), any(RepositoryTransaction.class));

    repo.upgradeFramework(newFramework);

    InOrder repoOrder = inOrder(repo);
    InOrder txOrder = inOrder(tx);
    InOrder upgraderOrder = inOrder(upgrader);
    InOrder validatorOrder = inOrder(validator);

    repoOrder.verify(repo, times(1)).findConnections();
    repoOrder.verify(repo, times(1)).findJobs();
    repoOrder.verify(repo, times(1)).getTransaction();
    repoOrder.verify(repo, times(1)).deleteJobInputs(1, tx);
    repoOrder.verify(repo, times(1)).deleteJobInputs(2, tx);
    repoOrder.verify(repo, times(1)).deleteConnectionInputs(1, tx);
    repoOrder.verify(repo, times(1)).deleteConnectionInputs(2, tx);
    repoOrder.verify(repo, times(1)).updateFramework(any(MFramework.class), any(RepositoryTransaction.class));
    repoOrder.verify(repo, times(2)).updateConnection(any(MConnection.class), any(RepositoryTransaction.class));
    repoOrder.verify(repo, times(2)).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    repoOrder.verifyNoMoreInteractions();
    txOrder.verify(tx, times(1)).begin();
    txOrder.verify(tx, times(1)).commit();
    txOrder.verify(tx, times(1)).close();
    txOrder.verifyNoMoreInteractions();
    upgraderOrder.verify(upgrader, times(2)).upgrade(any(MConnectionForms.class), any(MConnectionForms.class));
    upgraderOrder.verify(upgrader, times(2)).upgrade(any(MJobForms.class), any(MJobForms.class));
    upgraderOrder.verifyNoMoreInteractions();
    validatorOrder.verify(validator, times(2)).validateConnection(anyObject());
    validatorOrder.verify(validator, times(2)).validateJob(anyObject());
    validatorOrder.verifyNoMoreInteractions();
  }

  /**
   * Test the framework upgrade procedure, when all the connections and
   * jobs using the old connector are invalid for the new connector
   */
  @Test
  public void testFrameworkUpgradeWithInvalidConnectionsAndJobs() {
    MFramework newFramework = framework();

    when(validator.validateConnection(any(MConnection.class))).thenReturn(invalid);
    when(validator.validateJob(any(MJob.class))).thenReturn(invalid);
    when(frameworkMgr.getValidator()).thenReturn(validator);
    when(frameworkMgr.getMetadataUpgrader()).thenReturn(upgrader);
    when(frameworkMgr.getConnectionConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(frameworkMgr.getJobConfigurationClass()).thenReturn(JobConfiguration.class);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));

    doReturn(connectionList).when(repo).findConnections();
    doReturn(jobList).when(repo).findJobs();
    doNothing().when(repo).updateConnection(any(MConnection.class), any(RepositoryTransaction.class));
    doNothing().when(repo).updateJob(any(MJob.class), any(RepositoryTransaction.class));
    doNothing().when(repo).updateFramework(any(MFramework.class), any(RepositoryTransaction.class));

    try {
      repo.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getErrorCode(), RepositoryError.JDBCREPO_0027);

      InOrder repoOrder = inOrder(repo);
      InOrder txOrder = inOrder(tx);
      InOrder upgraderOrder = inOrder(upgrader);
      InOrder validatorOrder = inOrder(validator);

      repoOrder.verify(repo, times(1)).findConnections();
      repoOrder.verify(repo, times(1)).findJobs();
      repoOrder.verify(repo, times(1)).getTransaction();
      repoOrder.verify(repo, times(1)).deleteJobInputs(1, tx);
      repoOrder.verify(repo, times(1)).deleteJobInputs(2, tx);
      repoOrder.verify(repo, times(1)).deleteConnectionInputs(1, tx);
      repoOrder.verify(repo, times(1)).deleteConnectionInputs(2, tx);
      repoOrder.verify(repo, times(1)).updateFramework(any(MFramework.class), any(RepositoryTransaction.class));
      repoOrder.verifyNoMoreInteractions();
      txOrder.verify(tx, times(1)).begin();
      txOrder.verify(tx, times(1)).rollback();
      txOrder.verify(tx, times(1)).close();
      txOrder.verifyNoMoreInteractions();
      upgraderOrder.verify(upgrader, times(2)).upgrade(any(MConnectionForms.class), any(MConnectionForms.class));
      upgraderOrder.verify(upgrader, times(2)).upgrade(any(MJobForms.class), any(MJobForms.class));
      upgraderOrder.verifyNoMoreInteractions();
      validatorOrder.verify(validator, times(2)).validateConnection(anyObject());
      validatorOrder.verify(validator, times(2)).validateJob(anyObject());
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
    when(sqconnector.getValidator()).thenReturn(validator);
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgrader);
    when(connectorMgr.getConnector(anyString())).thenReturn(sqconnector);

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "find connections for connector error.");
    doThrow(exception).when(repoHandler).findConnectionsForConnector(anyLong(), any(Connection.class));

    try {
      repo.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findConnectionsForConnector(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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
    when(sqconnector.getValidator()).thenReturn(validator);
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgrader);
    when(connectorMgr.getConnector(anyString())).thenReturn(sqconnector);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    doReturn(connectionList).when(repoHandler).findConnectionsForConnector(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "find jobs for connector error.");
    doThrow(exception).when(repoHandler).findJobsForConnector(anyLong(), any(Connection.class));

    try {
      repo.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findConnectionsForConnector(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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
    when(sqconnector.getValidator()).thenReturn(validator);
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgrader);
    when(connectorMgr.getConnector(anyString())).thenReturn(sqconnector);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandler).findConnectionsForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandler).findJobsForConnector(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "delete job inputs for connector error.");
    doThrow(exception).when(repoHandler).deleteJobInputs(anyLong(), any(Connection.class));

    try {
      repo.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findConnectionsForConnector(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).deleteJobInputs(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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
    when(sqconnector.getValidator()).thenReturn(validator);
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgrader);
    when(connectorMgr.getConnector(anyString())).thenReturn(sqconnector);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandler).findConnectionsForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandler).findJobsForConnector(anyLong(), any(Connection.class));
    doNothing().when(repoHandler).deleteJobInputs(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "delete connection inputs for connector error.");
    doThrow(exception).when(repoHandler).deleteConnectionInputs(anyLong(), any(Connection.class));

    try {
      repo.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findConnectionsForConnector(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandler, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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
    when(sqconnector.getValidator()).thenReturn(validator);
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgrader);
    when(connectorMgr.getConnector(anyString())).thenReturn(sqconnector);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandler).findConnectionsForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandler).findJobsForConnector(anyLong(), any(Connection.class));
    doNothing().when(repoHandler).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandler).deleteConnectionInputs(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update connector error.");
    doThrow(exception).when(repoHandler).updateConnector(any(MConnector.class), any(Connection.class));

    try {
      repo.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findConnectionsForConnector(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandler, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandler, times(2)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).updateConnector(any(MConnector.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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
    when(validator.validateConnection(any(MConnection.class))).thenReturn(valid);
    when(validator.validateJob(any(MJob.class))).thenReturn(valid);
    when(sqconnector.getValidator()).thenReturn(validator);
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgrader);
    when(sqconnector.getConnectionConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(sqconnector.getJobConfigurationClass(any(Direction.class))).thenReturn(JobConfiguration.class);
    when(connectorMgr.getConnector(anyString())).thenReturn(sqconnector);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandler).findConnectionsForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandler).findJobsForConnector(anyLong(), any(Connection.class));
    doNothing().when(repoHandler).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandler).deleteConnectionInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandler).updateConnector(any(MConnector.class), any(Connection.class));
    doReturn(true).when(repoHandler).existsConnection(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update connection error.");
    doThrow(exception).when(repoHandler).updateConnection(any(MConnection.class), any(Connection.class));

    try {
      repo.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findConnectionsForConnector(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandler, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandler, times(2)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).updateConnector(any(MConnector.class), any(Connection.class));
      verify(repoHandler, times(1)).existsConnection(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).updateConnection(any(MConnection.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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
    when(validator.validateConnection(any(MConnection.class))).thenReturn(valid);
    when(validator.validateJob(any(MJob.class))).thenReturn(valid);
    when(sqconnector.getValidator()).thenReturn(validator);
    when(sqconnector.getMetadataUpgrader()).thenReturn(upgrader);
    when(sqconnector.getConnectionConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(sqconnector.getJobConfigurationClass(any(Direction.class))).thenReturn(JobConfiguration.class);
    when(connectorMgr.getConnector(anyString())).thenReturn(sqconnector);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandler).findConnectionsForConnector(anyLong(), any(Connection.class));
    doReturn(jobList).when(repoHandler).findJobsForConnector(anyLong(), any(Connection.class));
    doNothing().when(repoHandler).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandler).deleteConnectionInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandler).updateConnector(any(MConnector.class), any(Connection.class));
    doNothing().when(repoHandler).updateConnection(any(MConnection.class), any(Connection.class));
    doReturn(true).when(repoHandler).existsConnection(anyLong(), any(Connection.class));
    doReturn(true).when(repoHandler).existsJob(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update job error.");
    doThrow(exception).when(repoHandler).updateJob(any(MJob.class), any(Connection.class));

    try {
      repo.upgradeConnector(oldConnector, newConnector);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findConnectionsForConnector(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).findJobsForConnector(anyLong(), any(Connection.class));
      verify(repoHandler, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandler, times(2)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).updateConnector(any(MConnector.class), any(Connection.class));
      verify(repoHandler, times(2)).existsConnection(anyLong(), any(Connection.class));
      verify(repoHandler, times(2)).updateConnection(any(MConnection.class), any(Connection.class));
      verify(repoHandler, times(1)).existsJob(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).updateJob(any(MJob.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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

    when(frameworkMgr.getValidator()).thenReturn(validator);
    when(frameworkMgr.getMetadataUpgrader()).thenReturn(upgrader);

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "find connections error.");
    doThrow(exception).when(repoHandler).findConnections(any(Connection.class));

    try {
      repo.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findConnections(any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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

    when(frameworkMgr.getValidator()).thenReturn(validator);
    when(frameworkMgr.getMetadataUpgrader()).thenReturn(upgrader);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    doReturn(connectionList).when(repoHandler).findConnections(any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "find jobs error.");
    doThrow(exception).when(repoHandler).findJobs(any(Connection.class));

    try {
      repo.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findConnections(any(Connection.class));
      verify(repoHandler, times(1)).findJobs(any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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

    when(frameworkMgr.getValidator()).thenReturn(validator);
    when(frameworkMgr.getMetadataUpgrader()).thenReturn(upgrader);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandler).findConnections(any(Connection.class));
    doReturn(jobList).when(repoHandler).findJobs(any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "delete job inputs error.");
    doThrow(exception).when(repoHandler).deleteJobInputs(anyLong(), any(Connection.class));

    try {
      repo.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findConnections(any(Connection.class));
      verify(repoHandler, times(1)).findJobs(any(Connection.class));
      verify(repoHandler, times(1)).deleteJobInputs(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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

    when(frameworkMgr.getValidator()).thenReturn(validator);
    when(frameworkMgr.getMetadataUpgrader()).thenReturn(upgrader);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandler).findConnections(any(Connection.class));
    doReturn(jobList).when(repoHandler).findJobs(any(Connection.class));
    doNothing().when(repoHandler).deleteJobInputs(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "delete connection inputs error.");
    doThrow(exception).when(repoHandler).deleteConnectionInputs(anyLong(), any(Connection.class));

    try {
      repo.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findConnections(any(Connection.class));
      verify(repoHandler, times(1)).findJobs(any(Connection.class));
      verify(repoHandler, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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

    when(frameworkMgr.getValidator()).thenReturn(validator);
    when(frameworkMgr.getMetadataUpgrader()).thenReturn(upgrader);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandler).findConnections(any(Connection.class));
    doReturn(jobList).when(repoHandler).findJobs(any(Connection.class));
    doNothing().when(repoHandler).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandler).deleteConnectionInputs(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update framework metadata error.");
    doThrow(exception).when(repoHandler).updateFramework(any(MFramework.class), any(Connection.class));

    try {
      repo.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findConnections(any(Connection.class));
      verify(repoHandler, times(1)).findJobs(any(Connection.class));
      verify(repoHandler, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandler, times(2)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).updateFramework(any(MFramework.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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

    when(validator.validateConnection(any(MConnection.class))).thenReturn(valid);
    when(validator.validateJob(any(MJob.class))).thenReturn(valid);
    when(frameworkMgr.getValidator()).thenReturn(validator);
    when(frameworkMgr.getMetadataUpgrader()).thenReturn(upgrader);
    when(frameworkMgr.getConnectionConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(frameworkMgr.getJobConfigurationClass()).thenReturn(JobConfiguration.class);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandler).findConnections(any(Connection.class));
    doReturn(jobList).when(repoHandler).findJobs(any(Connection.class));
    doNothing().when(repoHandler).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandler).deleteConnectionInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandler).updateFramework(any(MFramework.class), any(Connection.class));
    doReturn(true).when(repoHandler).existsConnection(anyLong(), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update connection error.");
    doThrow(exception).when(repoHandler).updateConnection(any(MConnection.class), any(Connection.class));

    try {
      repo.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findConnections(any(Connection.class));
      verify(repoHandler, times(1)).findJobs(any(Connection.class));
      verify(repoHandler, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandler, times(2)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).updateFramework(any(MFramework.class), any(Connection.class));
      verify(repoHandler, times(1)).existsConnection(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).updateConnection(any(MConnection.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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

    when(validator.validateConnection(any(MConnection.class))).thenReturn(valid);
    when(validator.validateJob(any(MJob.class))).thenReturn(valid);
    when(frameworkMgr.getValidator()).thenReturn(validator);
    when(frameworkMgr.getMetadataUpgrader()).thenReturn(upgrader);
    when(frameworkMgr.getConnectionConfigurationClass()).thenReturn(EmptyConfigurationClass.class);
    when(frameworkMgr.getJobConfigurationClass()).thenReturn(JobConfiguration.class);

    List<MConnection> connectionList = connections(connection(1,1), connection(2,1));
    List<MJob> jobList = jobs(job(1,1,1,1,1), job(2,1,1,2,1));
    doReturn(connectionList).when(repoHandler).findConnections(any(Connection.class));
    doReturn(jobList).when(repoHandler).findJobs(any(Connection.class));
    doNothing().when(repoHandler).deleteJobInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandler).deleteConnectionInputs(anyLong(), any(Connection.class));
    doNothing().when(repoHandler).updateFramework(any(MFramework.class), any(Connection.class));
    doReturn(true).when(repoHandler).existsConnection(anyLong(), any(Connection.class));
    doReturn(true).when(repoHandler).existsJob(anyLong(), any(Connection.class));
    doNothing().when(repoHandler).updateConnection(any(MConnection.class), any(Connection.class));

    SqoopException exception = new SqoopException(RepositoryError.JDBCREPO_0000,
        "update job error.");
    doThrow(exception).when(repoHandler).updateJob(any(MJob.class), any(Connection.class));

    try {
      repo.upgradeFramework(newFramework);
    } catch (SqoopException ex) {
      assertEquals(ex.getMessage(), exception.getMessage());
      verify(repoHandler, times(1)).findConnections(any(Connection.class));
      verify(repoHandler, times(1)).findJobs(any(Connection.class));
      verify(repoHandler, times(2)).deleteJobInputs(anyLong(), any(Connection.class));
      verify(repoHandler, times(2)).deleteConnectionInputs(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).updateFramework(any(MFramework.class), any(Connection.class));
      verify(repoHandler, times(2)).existsConnection(anyLong(), any(Connection.class));
      verify(repoHandler, times(2)).updateConnection(any(MConnection.class), any(Connection.class));
      verify(repoHandler, times(1)).existsJob(anyLong(), any(Connection.class));
      verify(repoHandler, times(1)).updateJob(any(MJob.class), any(Connection.class));
      verifyNoMoreInteractions(repoHandler);
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
