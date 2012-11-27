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

package com.cloudera.sqoop.mapreduce.db;

import java.sql.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.sqoop.mapreduce.DBWritable;

/**
 * Test aspects of DataDrivenDBInputFormat.
 */
public class TestDataDrivenDBInputFormat extends TestCase {

  private static final Log LOG = LogFactory.getLog(
      TestDataDrivenDBInputFormat.class);

  private static final String DB_NAME = "dddbif";
  private static final String DB_URL =
    "jdbc:hsqldb:mem:" + DB_NAME;
  private static final String DRIVER_CLASS = "org.hsqldb.jdbcDriver";

  private Connection connection;

  private static final String OUT_DIR;

  static {
    OUT_DIR = System.getProperty("test.build.data", "/tmp") + "/dddbifout";
  }

  private void createConnection(String driverClassName,
      String url) throws Exception {

    Class.forName(driverClassName);
    connection = DriverManager.getConnection(url);
    connection.setAutoCommit(false);
  }

  private void shutdown() {
    try {
      connection.commit();
      connection.close();
      connection = null;
    } catch (Throwable ex) {
      LOG.warn("Exception occurred while closing connection :"
          + StringUtils.stringifyException(ex));
    }
  }

  private void initialize(String driverClassName, String url)
      throws Exception {
    createConnection(driverClassName, url);
  }

  public void setUp() throws Exception {
    initialize(DRIVER_CLASS, DB_URL);
    super.setUp();
  }

  public void tearDown() throws Exception {
    super.tearDown();
    shutdown();
  }



  /**
   * DBWritable class for a table that holds a single SQL date value.
   */
  public static class DateCol implements DBWritable, WritableComparable {
    private Date d;

    public Date getDate() {
      return d;
    }

    public void setDate(Date dt) {
      this.d = dt;
    }

    public String toString() {
      return d.toString();
    }

    public void readFields(ResultSet rs) throws SQLException {
      d = rs.getDate(1);
    }

    public void write(PreparedStatement ps) {
      // not needed.
    }

    public void readFields(DataInput in) throws IOException {
      long v = in.readLong();
      d = new Date(v);
    }

    public void write(DataOutput out) throws IOException {
      out.writeLong(d.getTime());
    }

    @Override
    /** {@inheritDoc} */
    public int hashCode() {
      return (int) d.getTime();
    }

    @Override
    /** {@inheritDoc} */
    public int compareTo(Object o) {
      if (o instanceof DateCol) {
        Long v = Long.valueOf(d.getTime());
        Long other = Long.valueOf(((DateCol) o).d.getTime());
        return v.compareTo(other);
      } else {
        return -1;
      }
    }

    @Override
    /** {@inheritDoc} */
    public boolean equals(Object o) {
      return (o instanceof DateCol) && compareTo(o) == 0;
    }
  }

  /**
   * Mapper that emits its input value as its output key.
   */
  public static class ValMapper
      extends Mapper<Object, Object, Object, NullWritable> {
    public void map(Object k, Object v, Context c)
        throws IOException, InterruptedException {
      c.write(v, NullWritable.get());
    }
  }

  public void testDateSplits() throws Exception {
    Statement s = connection.createStatement();
    final String DATE_TABLE = "datetable";
    final String COL = "foo";
    try {
      try {
        // delete the table if it already exists.
        s.executeUpdate("DROP TABLE " + DATE_TABLE);
      } catch (SQLException e) {
        // Ignored; proceed regardless of whether we deleted the table;
        // it may have simply not existed.
      }

      // Create the table.
      s.executeUpdate("CREATE TABLE " + DATE_TABLE + "(" + COL + " TIMESTAMP)");
      s.executeUpdate("INSERT INTO " + DATE_TABLE + " VALUES('2010-04-01')");
      s.executeUpdate("INSERT INTO " + DATE_TABLE + " VALUES('2010-04-02')");
      s.executeUpdate("INSERT INTO " + DATE_TABLE + " VALUES('2010-05-01')");
      s.executeUpdate("INSERT INTO " + DATE_TABLE + " VALUES('2011-04-01')");

      // commit this tx.
      connection.commit();

      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", "file:///");
      FileSystem fs = FileSystem.getLocal(conf);
      fs.delete(new Path(OUT_DIR), true);

      // now do a dd import
      Job job = new Job(conf);
      job.setMapperClass(ValMapper.class);
      job.setReducerClass(Reducer.class);
      job.setMapOutputKeyClass(DateCol.class);
      job.setMapOutputValueClass(NullWritable.class);
      job.setOutputKeyClass(DateCol.class);
      job.setOutputValueClass(NullWritable.class);
      job.setNumReduceTasks(1);
      job.getConfiguration().setInt("mapreduce.map.tasks", 2);
      FileOutputFormat.setOutputPath(job, new Path(OUT_DIR));
      DBConfiguration.configureDB(job.getConfiguration(), DRIVER_CLASS,
          DB_URL, (String) null, (String) null);
      DataDrivenDBInputFormat.setInput(job, DateCol.class, DATE_TABLE, null,
          COL, COL);

      boolean ret = job.waitForCompletion(true);
      assertTrue("job failed", ret);

      // Check to see that we imported as much as we thought we did.
      assertEquals("Did not get all the records", 4,
          job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter",
          "REDUCE_OUTPUT_RECORDS").getValue());
    } finally {
      s.close();
    }
  }

}
