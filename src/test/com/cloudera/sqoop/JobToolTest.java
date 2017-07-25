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

import com.cloudera.sqoop.manager.MySQLTestUtils;
import com.cloudera.sqoop.manager.OracleUtils;
import com.cloudera.sqoop.testutil.CommonArgs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.manager.DefaultManagerFactory;
import org.apache.sqoop.manager.sqlserver.MSSQLTestUtils;
import org.apache.sqoop.tool.JobTool;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class JobToolTest {

    public static final Log LOG = LogFactory
            .getLog(MetaConnectIncrementalImportTest.class.getName());

    private static MySQLTestUtils mySQLTestUtils = new MySQLTestUtils();
    private static MSSQLTestUtils msSQLTestUtils = new MSSQLTestUtils();

    @Parameterized.Parameters(name = "metaConnectString = {0}, metaUser = {1}, metaPassword = {2}")
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
    public void setUp() throws Exception {
        SqoopOptions options = new SqoopOptions();
        options.setConnectString(metaConnectString);
        options.setUsername(metaUser);
        options.setPassword(metaPass);

        com.cloudera.sqoop.metastore.JobData jd = new com.cloudera.sqoop.metastore.JobData(options, null);
        DefaultManagerFactory dmf = new DefaultManagerFactory();
        ConnManager cm = dmf.accept(jd);
        Connection conn = cm.getConnection();

        System.setProperty(org.apache.sqoop.SqoopOptions.METASTORE_PASSWORD_KEY, "true");

        try {
            Statement statement = conn.createStatement();
            statement.execute("DROP TABLE SQOOP_ROOT");
            statement.execute("DROP TABLE SQOOP_SESSIONS");
            conn.commit();
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage());
        }

        cm.close();
    }

    private String metaConnectString;
    private String metaUser;
    private String metaPass;


    public JobToolTest(String metaConnectString, String metaUser, String metaPass) {
        this.metaConnectString = metaConnectString;
        this.metaUser = metaUser;
        this.metaPass = metaPass;
    }

    protected String[] getCreateJob(String metaConnectString, String metaUser, String metaPass) {
        ArrayList<String> args = new ArrayList<String>();
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
        args.add("jdbc:mysql://mysql.vpc.cloudera.com:3306/sqoop");
        args.add("--username");
        args.add("sqoop");
        args.add("--password");
        args.add("sqoop");
        args.add("--table");
        args.add("CarLocations");
        args.add("--as-textfile");
        args.add("--delete-target-dir");

        return args.toArray(new String[0]);
    }

    protected String[] getExecJob(String metaConnectString, String metaUser, String metaPass) {
        ArrayList<String> args = new ArrayList<String>();
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
        ArrayList<String> args = new ArrayList<String>();
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
        assertEquals(0, Sqoop.runSqoop(sqoop, args));
    }

    @Test
    public void testExecJob() throws IOException {
        Configuration conf = new Configuration();
        conf.set(org.apache.sqoop.SqoopOptions.METASTORE_PASSWORD_KEY, "true");
        JobTool jobToolCreate = new JobTool();
        Sqoop sqoopCreate = new Sqoop(jobToolCreate, conf);
        String[] argsCreate = getCreateJob(metaConnectString, metaUser, metaPass);
        Sqoop.runSqoop(sqoopCreate, argsCreate);
        JobTool jobToolExec = new JobTool();
        Sqoop sqoopExec = new Sqoop(jobToolExec);
        String[] argsExec = getExecJob(metaConnectString, metaUser, metaPass);
        assertEquals(0, Sqoop.runSqoop(sqoopExec, argsExec));
    }

    @Test
    public void testDeleteJob() throws IOException {
        Configuration conf = new Configuration();
        conf.set(org.apache.sqoop.SqoopOptions.METASTORE_PASSWORD_KEY, "true");
        JobTool jobToolCreate = new JobTool();
        Sqoop sqoopCreate = new Sqoop(jobToolCreate, conf);
        String[] argsCreate = getCreateJob(metaConnectString, metaUser, metaPass);
        Sqoop.runSqoop(sqoopCreate, argsCreate);
        JobTool jobToolDelete = new JobTool();
        Sqoop sqoopExec = new Sqoop(jobToolDelete);
        String[] argsDelete = getDeleteJob(metaConnectString, metaUser, metaPass);
        assertEquals(0, Sqoop.runSqoop(sqoopExec, argsDelete));
    }
}