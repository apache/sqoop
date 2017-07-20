package com.cloudera.sqoop;

import com.cloudera.sqoop.testutil.CommonArgs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.manager.DefaultManagerFactory;
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
      .getLog(JobToolTest.class.getName());

    @Parameterized.Parameters(name = "metaConnectString = {0}, metaUser = {1}, metaPass= {2}")
    public static Iterable<? extends Object> dbConnectParameters() {
        return Arrays.asList(
                new Object[]{
                        "jdbc:mysql://mysql.vpc.cloudera.com/sqoop",
                        "sqoop", "sqoop"
                },
                new Object[]{
                        "jdbc:oracle:thin:@//oracle-ee.vpc.cloudera.com/orcl",
                        "sqoop", "sqoop"
                },
                new Object[]{
                        "jdbc:sqlserver://sqlserver.vpc.cloudera.com:1433;database=sqoop",
                        "sqoop", "sqoop"
                },
                new Object[]{
                        "jdbc:postgresql://postgresql.vpc.cloudera.com/sqoop",
                        "sqoop", "sqoop"
                },
                new Object[]{
                        "jdbc:db2://db2.vpc.cloudera.com:50000/SQOOP",
                        "DB2INST1", "cloudera"
                },
                new Object[]{"jdbc:hsqldb:mem:sqoopmetastore", "SA", ""}
        );
    }

    @Before
    public void setUp () throws Exception {
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
        }
        catch (Exception e) {
            LOG.error( e.getLocalizedMessage() );
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
    args.add("--meta-user");
    args.add(metaUser);
    args.add("--meta-pass");
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
        args.add("--meta-user");
        args.add(metaUser);
        args.add("--meta-pass");
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
      assertEquals( 0, Sqoop.runSqoop(sqoopExec, argsExec));
  }
}