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

package org.apache.sqoop.manager;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.metastore.JobData;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


public class TestDefaultManagerFactory {

    DefaultManagerFactory mf = new DefaultManagerFactory();

    @Test
    public void givenMySQLSchemaDirectFactoryReturnsMySQLManager() {
        assertCreationOfCorrectManagerClassDirect(SupportedManagers.MYSQL, MySQLManager.class);
    }

    @Test
    public void givenMySQLSchemaNonDirectFactoryReturnsMySQLManager() {
        assertCreationOfCorrectManagerClassNotDirect(SupportedManagers.MYSQL, MySQLManager.class);
    }

    @Test
    public void givenPostgresSchemaDirectFactoryReturnsDirectPostgresqlManager() {
        assertCreationOfCorrectManagerClassDirect(SupportedManagers.POSTGRES, DirectPostgresqlManager.class);
    }

    @Test
    public void givenPostgresSchemaNonDirectFactoryReturnsPostgresqlManager() {
        assertCreationOfCorrectManagerClassNotDirect(SupportedManagers.POSTGRES, PostgresqlManager.class);
    }

    @Test
    public void givenHsqlSchemaDirectFactoryReturnsHsqldbManager() {
        assertCreationOfCorrectManagerClassDirect(SupportedManagers.HSQLDB, HsqldbManager.class);
    }

    @Test
    public void givenHsqlSchemaNonDirectFactoryReturnsHsqldbManager() {
        assertCreationOfCorrectManagerClassNotDirect(SupportedManagers.HSQLDB, HsqldbManager.class);
    }

    @Test
    public void givenSqlServerSchemaDirectFactoryReturnsSQLServerManager() {
        assertCreationOfCorrectManagerClassDirect(SupportedManagers.SQLSERVER, SQLServerManager.class);
    }

    @Test
    public void givenSqlServerSchemaNonDirectFactoryReturnsSQLServerManager() {
        assertCreationOfCorrectManagerClassNotDirect(SupportedManagers.SQLSERVER, SQLServerManager.class);
    }

    @Test
    public void givenJTDSqlServerSchemaDirectFactoryReturnsSQLServerManager() {
        assertCreationOfCorrectManagerClassDirect(SupportedManagers.JTDS_SQLSERVER, SQLServerManager.class);
    }

    @Test
    public void givenJTDSqlServerSchemaNonDirectFactoryReturnsSQLServerManager() {
        assertCreationOfCorrectManagerClassNotDirect(SupportedManagers.JTDS_SQLSERVER, SQLServerManager.class);
    }

    @Test
    public void givenDb2SchemaDirectFactoryReturnsDb2Manager() {
        assertCreationOfCorrectManagerClassDirect(SupportedManagers.DB2, Db2Manager.class);
    }

    @Test
    public void givenDb2SchemaNonDirectFactoryReturnsDb2Manager() {
        assertCreationOfCorrectManagerClassNotDirect(SupportedManagers.DB2, Db2Manager.class);
    }

    @Test
    public void givenOracleSchemaDirectFactoryReturnsOracleManager() {
        //OraOop connector is created differently, but from the factory's perspective it creates an OracleManager currently
        assertCreationOfCorrectManagerClassDirect(SupportedManagers.ORACLE, OracleManager.class);
    }

    @Test
    public void givenOracleSchemaNonDirectFactoryReturnsOracleManager() {
        assertCreationOfCorrectManagerClassNotDirect(SupportedManagers.ORACLE, OracleManager.class);
    }

    @Test
    public void givenNetezzaSchemaNonDirectFactoryReturnsNetezzaManager() {
        assertCreationOfCorrectManagerClassNotDirect(SupportedManagers.NETEZZA, NetezzaManager.class);
    }

    @Test
    public void givenNetezzaSchemaDirectFactoryReturnsDirectNetezzaManager() {
        assertCreationOfCorrectManagerClassDirect(SupportedManagers.NETEZZA, DirectNetezzaManager.class);
    }

    @Test
    public void givenCubridSchemaNonDirectFactoryReturnsCubridManager() {
        assertCreationOfCorrectManagerClassNotDirect(SupportedManagers.CUBRID, CubridManager.class);
    }

    @Test
    public void givenCubridSchemaDirectFactoryReturnsCubridManager() {
        assertCreationOfCorrectManagerClassDirect(SupportedManagers.CUBRID, CubridManager.class);
    }

    private void assertCreationOfCorrectManagerClassNotDirect(SupportedManagers supportedManagers, Class type) {
        assertCreationOfCorrectManagerClass(supportedManagers, type, false);
    }

    private void assertCreationOfCorrectManagerClassDirect(SupportedManagers supportedManagers, Class type) {
        assertCreationOfCorrectManagerClass(supportedManagers, type, true);
    }

    private void assertCreationOfCorrectManagerClass(SupportedManagers supportedManagers, Class type, boolean isDirect) {
        JobData data = mock(JobData.class);
        SqoopOptions mockoptions = mockOptions(supportedManagers, isDirect);
        when(data.getSqoopOptions()).thenReturn(mockoptions);
        ConnManager connmanager = mf.accept(data);
        assertThat(connmanager, instanceOf(type));
        verifyCalls(supportedManagers, data, mockoptions);
    }

    private void verifyCalls(SupportedManagers supportedManagers, JobData data, SqoopOptions mockoptions) {
        verify(data).getSqoopOptions();
        verifyNoMoreInteractions(data);
        //Workaround as Oracle Direct Connector creation is not handled by the DefaultManagerFactory
        if (supportedManagers.hasDirectConnector() && !supportedManagers.equals(SupportedManagers.ORACLE)) {
            verify(mockoptions).isDirect();
        }
        else
            verify(mockoptions, never()).isDirect();

    }

    private SqoopOptions mockOptions(SupportedManagers supportedManagers, boolean isDirect) {
        SqoopOptions options = mock(SqoopOptions.class);
        when(options.getConnectString()).thenReturn(supportedManagers.getSchemePrefix() + "//" + RandomStringUtils.random(10));
        when(options.isDirect()).thenReturn(isDirect);
        when(options.getConf()).thenReturn(mock(Configuration.class));
        return options;
    }
}
