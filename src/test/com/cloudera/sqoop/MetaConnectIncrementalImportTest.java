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

package com.cloudera.sqoop;

import static org.junit.Assert.assertEquals;

import com.cloudera.sqoop.manager.MySQLTestUtils;
import com.cloudera.sqoop.manager.OracleUtils;
import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.testutil.CommonArgs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.manager.DefaultManagerFactory;
import org.apache.sqoop.manager.sqlserver.MSSQLTestUtils;
import org.apache.sqoop.tool.JobTool;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class MetaConnectIncrementalImportTest extends BaseSqoopTestCase {

    public static final Log LOG = LogFactory
            .getLog(MetaConnectIncrementalImportTest.class.getName());

    private static MySQLTestUtils mySQLTestUtils = new MySQLTestUtils();
    private static MSSQLTestUtils msSQLTestUtils = new MSSQLTestUtils();

    @Parameterized.Parameters(name = "metaConnectString = {0}, metaUser = {1}, metaPass = {2}")
    public static Iterable<? extends Object> dbConnectParameters() {
        return Arrays.asList(
                new Object[] {
                        mySQLTestUtils.getHostUrl(), mySQLTestUtils.getUserName(),
                        mySQLTestUtils.getUserPass()
                },
                new Object[] {
                        OracleUtils.CONNECT_STRING, OracleUtils.ORACLE_USER_NAME,
                        OracleUtils.ORACLE_USER_PASS
                },
                new Object[] {
                        msSQLTestUtils.getDBConnectString(), msSQLTestUtils.getDBUserName(),
                        msSQLTestUtils.getDBPassWord()
                },
                new Object[] {
                        System.getProperty(
                                "sqoop.test.postgresql.connectstring.host_url",
                                "jdbc:postgresql://localhost/"),
                        System.getProperty(
                                "sqoop.test.postgresql.connectstring.username",
                                "sqooptest"),
                        System.getProperty(
                                "sqoop.test.postgresql.connectstring.password"),
                },
                new Object[] {
                        System.getProperty(
                                "sqoop.test.db2.connectstring.host_url",
                                "jdbc:db2://db2host:50000"),
                        System.getProperty(
                                "sqoop.test.db2.connectstring.username",
                                "SQOOP"),
                        System.getProperty(
                                "sqoop.test.db2.connectstring.password",
                                "SQOOP"),
                },
                new Object[] { "jdbc:hsqldb:mem:sqoopmetastore", "SA" , "" }
        );
    }

    @Before
    public void setUp() {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    private String metaConnectString;
    private String metaUser;
    private String metaPass;

    private Connection connMeta;
    private ConnManager cm;


    public MetaConnectIncrementalImportTest(String metaConnectString, String metaUser, String metaPass) {
        this.metaConnectString = metaConnectString;
        this.metaUser = metaUser;
        this.metaPass = metaPass;
    }


    protected String[] getIncrementalJob(String metaConnectString, String metaUser, String metaPass) {
        List<String> args = new ArrayList<>();
        CommonArgs.addHadoopFlags(args);
        args.add("--create");
        args.add("testJob");
        args.add("--meta-connect");
        args.add(metaConnectString);
        args.add("--meta-username");
        args.add(metaUser);
        args.add("--meta-password");
        args.add(metaPass);
        args.add("--");
        args.add("import");
        args.add("-m");
        args.add("1");
        args.add("--connect");
        args.add(getConnectString());
        args.add("--table");
        args.add("CARLOCATIONS");
        args.add("--incremental");
        args.add("append");
        args.add("--check-column");
        args.add("CARID");
        args.add("--last-value");
        args.add("0");
        args.add("--as-textfile");

        return args.toArray(new String[0]);
    }


    protected String[] getExecJob(String metaConnectString, String metaUser, String metaPass) {
        List<String> args = new ArrayList<>();
        CommonArgs.addHadoopFlags(args);
        args.add("--exec");
        args.add("testJob");
        args.add("--meta-connect");
        args.add(metaConnectString);
        args.add("--meta-username");
        args.add(metaUser);
        args.add("--meta-password");
        args.add(metaPass);

        return args.toArray(new String[0]);
    }

    @Test
    public void testIncrementalJob() throws Exception {
        //Resets the target table
        dropTableIfExists("CARLOCATIONS");
        setCurTableName("CARLOCATIONS");
        createTableWithColTypesAndNames(
                new String [] {"CARID", "LOCATIONS"},
                new String [] {"INTEGER", "VARCHAR"},
                new String [] {"1", "'Lexus'"});

        initMetastoreConnection();

        try {
            //Resets the metastore schema
            Statement metastoreStatement = connMeta.createStatement();
            metastoreStatement.execute("DROP TABLE SQOOP_ROOT");
            metastoreStatement.execute("DROP TABLE SQOOP_SESSIONS");
            connMeta.commit();
        }
        catch (Exception e) {
            LOG.error( e.getLocalizedMessage() );
        }

        //creates Job
        Configuration conf = new Configuration();
        conf.set(org.apache.sqoop.SqoopOptions.METASTORE_PASSWORD_KEY, "true");
        JobTool jobToolCreate = new JobTool();
        org.apache.sqoop.Sqoop sqoopCreate = new org.apache.sqoop.Sqoop(jobToolCreate, conf);
        String[] argsCreate = getIncrementalJob(metaConnectString, metaUser, metaPass);
        org.apache.sqoop.Sqoop.runSqoop(sqoopCreate, argsCreate);

        //Executes the import
        JobTool jobToolExec = new JobTool();
        org.apache.sqoop.Sqoop sqoopExec = new org.apache.sqoop.Sqoop(jobToolExec);
        String[] argsExec = getExecJob(metaConnectString, metaUser, metaPass);
        assertEquals(0, org.apache.sqoop.Sqoop.runSqoop(sqoopExec, argsExec));

        //Ensures the saveIncrementalState saved the right row
        Statement getSaveIncrementalState = connMeta.createStatement();
        ResultSet lastCol = getSaveIncrementalState.executeQuery(
                "SELECT propVal FROM SQOOP_SESSIONS WHERE propname = 'incremental.last.value'");
        lastCol.next();
        assertEquals(1, lastCol.getInt("propVal"));


        //Adds rows to the import table
        Statement insertStmt = getConnection().createStatement();
        insertStmt.executeUpdate("INSERT INTO CARLOCATIONS VALUES (2, 'lexus')");
        getConnection().commit();

        //Execute the import again
        JobTool jobToolExec2 = new JobTool();
        org.apache.sqoop.Sqoop sqoopExec2 = new org.apache.sqoop.Sqoop(jobToolExec2);
        String[] argsExec2 = getExecJob(metaConnectString, metaUser, metaPass);
        assertEquals(0, org.apache.sqoop.Sqoop.runSqoop(sqoopExec2, argsExec2));

        //Ensures the last incremental value is updated correctly.
        Statement getSaveIncrementalState2 = connMeta.createStatement();
        ResultSet lastCol2 = getSaveIncrementalState2.executeQuery(
                "SELECT propVal FROM SQOOP_SESSIONS WHERE propName = 'incremental.last.value'");
        lastCol2.next();
        assertEquals(2, lastCol2.getInt("propVal"));

        cm.close();
    }


    private void initMetastoreConnection() throws SQLException{
        SqoopOptions options = new SqoopOptions();
        options.setConnectString(metaConnectString);
        options.setUsername(metaUser);
        options.setPassword(metaPass);
        com.cloudera.sqoop.metastore.JobData jd =
                new com.cloudera.sqoop.metastore.JobData(options, new JobTool());
        DefaultManagerFactory dmf = new DefaultManagerFactory();
        cm = dmf.accept(jd);
        connMeta= cm.getConnection();
    }
}