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

package org.apache.sqoop.metastore;

import static org.junit.Assert.assertEquals;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.testutil.BaseSqoopTestCase;
import org.apache.sqoop.testutil.CommonArgs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.manager.DefaultManagerFactory;
import org.apache.sqoop.tool.JobTool;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


/**
 * Base test class for Incremental Import Metastore data, implemented for specific database services in sub-classes
 */

public abstract class MetaConnectIncrementalImportTestBase extends BaseSqoopTestCase {

    public static final Log LOG = LogFactory
            .getLog(MetaConnectIncrementalImportTestBase.class.getName());

    private String metaConnectString;
    private String metaUser;
    private String metaPass;

    private Connection connMeta;
    private ConnManager cm;

    public MetaConnectIncrementalImportTestBase(String metaConnectString, String metaUser, String metaPass) {
        this.metaConnectString = metaConnectString;
        this.metaUser = metaUser;
        this.metaPass = metaPass;
    }

    @Before
    public void setUp() {
        super.setUp();
        try {
            initMetastoreConnection();
            resetTable();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        resetMetastoreSchema();
    }

    @After
    public void tearDown() {
        super.tearDown();
        resetMetastoreSchema();
        try {
            cm.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
    public void testIncrementalJob() throws SQLException {
        //creates Job
        createJob();

        //Executes the import
        execJob();

        //Ensures the saveIncrementalState saved the right row
        checkIncrementalState(1);

        //Adds rows to the import table
        Statement insertStmt = getConnection().createStatement();
        insertStmt.executeUpdate("INSERT INTO CARLOCATIONS VALUES (2, 'lexus')");
        getConnection().commit();

        //Execute the import again
        execJob();

        //Ensures the last incremental value is updated correctly.
        checkIncrementalState(2);
    }

    private void checkIncrementalState(int expected) throws SQLException {
        Statement getSaveIncrementalState = connMeta.createStatement();
        ResultSet lastCol = getSaveIncrementalState.executeQuery(
                "SELECT propVal FROM " + cm.escapeTableName("SQOOP_SESSIONS") + " WHERE propname = 'incremental.last.value'");
        lastCol.next();
        assertEquals("Last row value differs from expected",
                expected, lastCol.getInt("propVal"));
    }

    private void execJob() {
        JobTool jobToolExec = new JobTool();
        org.apache.sqoop.Sqoop sqoopExec = new org.apache.sqoop.Sqoop(jobToolExec);
        String[] argsExec = getExecJob(metaConnectString, metaUser, metaPass);
        assertEquals("Sqoop Job did not execute properly",
                0, org.apache.sqoop.Sqoop.runSqoop(sqoopExec, argsExec));
    }

    private void createJob() {
        Configuration conf = new Configuration();
        conf.set(org.apache.sqoop.SqoopOptions.METASTORE_PASSWORD_KEY, "true");
        JobTool jobToolCreate = new JobTool();
        org.apache.sqoop.Sqoop sqoopCreate = new org.apache.sqoop.Sqoop(jobToolCreate, conf);
        String[] argsCreate = getIncrementalJob(metaConnectString, metaUser, metaPass);
        org.apache.sqoop.Sqoop.runSqoop(sqoopCreate, argsCreate);
    }

    private void resetTable() throws SQLException {
        //Resets the target table
        dropTableIfExists("CARLOCATIONS");
        setCurTableName("CARLOCATIONS");
        createTableWithColTypesAndNames(
                new String [] {"CARID", "LOCATIONS"},
                new String [] {"INTEGER", "VARCHAR"},
                new String [] {"1", "'Lexus'"});
    }

    private void resetMetastoreSchema() {
        try {
            //Resets the metastore schema
            Statement metastoreStatement = connMeta.createStatement();
            metastoreStatement.execute("DROP TABLE " + cm.escapeTableName("SQOOP_ROOT"));
            metastoreStatement.execute("DROP TABLE " + cm.escapeTableName("SQOOP_SESSIONS"));
            connMeta.commit();
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage());
            try {
                connMeta.rollback();
            } catch (SQLException innerException) {
                LOG.error(innerException.getLocalizedMessage());
            }
        }
    }

    private void initMetastoreConnection() throws SQLException{
        SqoopOptions options = new SqoopOptions();
        options.setConnectString(metaConnectString);
        options.setUsername(metaUser);
        options.setPassword(metaPass);
        org.apache.sqoop.metastore.JobData jd =
                new org.apache.sqoop.metastore.JobData(options, new JobTool());
        DefaultManagerFactory dmf = new DefaultManagerFactory();
        cm = dmf.accept(jd);
        connMeta= cm.getConnection();
    }
}