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

package com.cloudera.sqoop.metastore;

import static org.junit.Assert.assertEquals;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.testutil.BaseSqoopTestCase;
import com.cloudera.sqoop.testutil.CommonArgs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.manager.DefaultManagerFactory;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.tool.JobTool;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Base test class for JobTool, implemented for specific database services in sub-classes
 */

public abstract class JobToolTestBase extends BaseSqoopTestCase {

    public static final Log LOG = LogFactory
            .getLog(MetaConnectIncrementalImportTestBase.class.getName());

    private String metaConnectString;
    private String metaUser;
    private String metaPass;
    private ConnManager cm;


    public JobToolTestBase(String metaConnectString, String metaUser, String metaPass) {
        this.metaConnectString = metaConnectString;
        this.metaUser = metaUser;
        this.metaPass = metaPass;
    }

    @Before
    public void setUp() {
        super.setUp();

        SqoopOptions options = getSqoopOptions();

        Connection conn = getConnection(options);

        try {
            Statement statement = conn.createStatement();
            statement.execute("DROP TABLE " + cm.escapeTableName("SQOOP_ROOT"));
            statement.execute("DROP TABLE " + cm.escapeTableName("SQOOP_SESSIONS"));
            conn.commit();
        } catch (Exception e) {
            LOG.error("Failed to clear metastore database");
        }
        //Methods from BaseSqoopTestClass reference the test Hsqldb database, not the metastore
        try{
            dropTableIfExists("CarLocations");
        } catch (SQLException e) {
            LOG.error("Failed to drop table CarLocations");
        }
        setCurTableName("CarLocations");
        createTableWithColTypesAndNames(
                new String [] {"carId", "Locations"},
                new String [] {"INTEGER", "VARCHAR"},
                new String [] {"1", "'Lexus'"});
    }

    private Connection getConnection(SqoopOptions options) {
        try {
            com.cloudera.sqoop.metastore.JobData jd = new com.cloudera.sqoop.metastore.JobData(options, null);
            DefaultManagerFactory dmf = new DefaultManagerFactory();
            cm = dmf.accept(jd);
            return cm.getConnection();
        } catch (SQLException e) {
            LOG.error("Failed to create a connection to the Metastore");
            return  null;
        }
    }

    private SqoopOptions getSqoopOptions() {
        SqoopOptions options = new SqoopOptions();
        options.setConnectString(metaConnectString);
        options.setUsername(metaUser);
        options.setPassword(metaPass);
        return options;
    }

    @After
    public void tearDown() {
        super.tearDown();

        try {
            cm.close();
        } catch (SQLException e) {
            LOG.error("Failed to close ConnManager");
        }

    }

    protected String[] getCreateJob(String metaConnectString, String metaUser, String metaPass) {
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
        args.add("list-tables");
        args.add("--connect");
        args.add(getConnectString());

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


    protected String[] getDeleteJob(String metaConnectString, String metaUser, String metaPass) {
        List<String> args = new ArrayList<>();
        CommonArgs.addHadoopFlags(args);
        args.add("--delete");
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
    public void testCreateJob() throws IOException {
        org.apache.sqoop.tool.JobTool jobTool = new org.apache.sqoop.tool.JobTool();
        org.apache.sqoop.Sqoop sqoop = new Sqoop(jobTool);
        String[] args = getCreateJob(metaConnectString, metaUser, metaPass);
        assertEquals("Error creating Sqoop Job", 0, Sqoop.runSqoop(sqoop, args));
    }

    @Test
    public void testExecJob() throws IOException {
        Configuration conf = new Configuration();
        //creates the job
        JobTool jobToolCreate = new JobTool();
        Sqoop sqoopCreate = new Sqoop(jobToolCreate, conf);
        String[] argsCreate = getCreateJob(metaConnectString, metaUser, metaPass);
        Sqoop.runSqoop(sqoopCreate, argsCreate);
        //executes the job
        JobTool jobToolExec = new JobTool();
        Sqoop sqoopExec = new Sqoop(jobToolExec);
        String[] argsExec = getExecJob(metaConnectString, metaUser, metaPass);
        assertEquals("Error executing Sqoop Job", 0, Sqoop.runSqoop(sqoopExec, argsExec));
    }

    @Test
    public void testDeleteJob() throws IOException {
        Configuration conf = new Configuration();
        //Creates the job
        JobTool jobToolCreate = new JobTool();
        Sqoop sqoopCreate = new Sqoop(jobToolCreate, conf);
        String[] argsCreate = getCreateJob(metaConnectString, metaUser, metaPass);
        Sqoop.runSqoop(sqoopCreate, argsCreate);
        //Deletes the job
        JobTool jobToolDelete = new JobTool();
        Sqoop sqoopExec = new Sqoop(jobToolDelete);
        String[] argsDelete = getDeleteJob(metaConnectString, metaUser, metaPass);
        assertEquals("Error deleting Sqoop Job", 0, Sqoop.runSqoop(sqoopExec, argsDelete));
    }
}