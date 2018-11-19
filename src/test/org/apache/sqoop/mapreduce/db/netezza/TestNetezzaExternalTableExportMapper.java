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

package org.apache.sqoop.mapreduce.db.netezza;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.sqoop.mapreduce.db.DBConfiguration;
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Verifier;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNetezzaExternalTableExportMapper {

  // chained rule, see #rules
  private Verifier verifyThatLogsAreUploaded = new Verifier() {
    @Override public void verify() {
      File jobDir = tmpFolder.getRoot().toPath().resolve("job_job001_0001").resolve("job__0000-0-0").toFile();
      assertThat(jobDir.exists(), is(true));
      assertThat(jobDir.listFiles().length, is(equalTo(1)));
      assertThat(jobDir.listFiles()[0].getName(), is(equalTo("TEST.nzlog")));
      try {
        assertThat(FileUtils.readFileToString(jobDir.listFiles()[0]), is(equalTo("test log")));
      } catch (IOException e) {
        e.printStackTrace();
        fail("Failed to read log file.");
      }
    }
  };

  // chained rule, see #rules
  private TemporaryFolder tmpFolder = new TemporaryFolder();

  // need to keep tmpFolder around to verify logs
  @Rule
  public RuleChain rules = RuleChain.outerRule(tmpFolder).around(verifyThatLogsAreUploaded);

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private static final SQLException testException = new SQLException("failed in test");

  private NetezzaExternalTableExportMapper<LongWritable, Text> mapper;
  private Mapper.Context context;

  @Before
  public void setUp() throws Exception {
    mapper = basicMockingOfMapper();
    context = getContext();
  }

  @Test
  public void testPassingJDBC() throws Exception {
    withNoopJDBCOperation(mapper).run(context);
  }

  @Test
  public void testFailingJDBC() throws Exception {
    withFailingJDBCOperation(mapper);

    exception.expect(IOException.class);
    exception.expectCause(is(equalTo(testException)));
    mapper.run(context);
  }

  /**
   * Creates an instance of NetezzaExternalTableExportMapper with the
   * necessary fields mocked to be able to call the run() method without errors.
   * @return
   */
  private NetezzaExternalTableExportMapper<LongWritable, Text> basicMockingOfMapper() {
    NetezzaExternalTableExportMapper<LongWritable, Text> mapper = new NetezzaExternalTableExportMapper<LongWritable, Text>() {
      @Override
      public void map(LongWritable key, Text text, Context context) {
        // no-op. Don't read from context, mock won't be ready to handle that.
      }
    };

    mapper.logDir = tmpFolder.getRoot().getAbsolutePath();

    return mapper;
  }

  /**
   * Mocks mapper's DB connection in a way that leads to SQLException during the JDBC operation.
   * @param mapper will modify this object
   * @return modified mapper
   * @throws Exception
   */
  private NetezzaExternalTableExportMapper<LongWritable, Text> withFailingJDBCOperation(NetezzaExternalTableExportMapper<LongWritable, Text> mapper) throws Exception {
    Connection connectionMock = mock(Connection.class);

    // PreparadStatement mock should imitate loading stuff from FIFO into Netezza
    PreparedStatement psMock = mock(PreparedStatement.class);
    when(psMock.execute()).then(invocation -> {
      // Write log file under taskAttemptDir to be able to check log upload
      File logFile = mapper.taskAttemptDir.toPath().resolve("job__0000-0-0").resolve("TEST.nzlog").toFile();
      FileUtils.writeStringToFile(logFile, "test log");

      // Need to open FIFO for reading, otherwise writing would hang
      FileInputStream fis = new FileInputStream(mapper.fifoFile.getAbsoluteFile());

      // Simulate delay
      Thread.sleep(200);
      throw testException;
    });
    when(connectionMock.prepareStatement(anyString())).thenReturn(psMock);

    DBConfiguration dbcMock = mock(DBConfiguration.class);
    when(dbcMock.getConnection()).thenReturn(connectionMock);
    mapper.dbc = dbcMock;
    return mapper;
  }


  /**
   * Mocks mapper's DB connection to execute a no-op JDBC operation.
   * @param mapper will modify this object
   * @return modified mapper
   * @throws Exception
   */
  private NetezzaExternalTableExportMapper<LongWritable, Text> withNoopJDBCOperation(NetezzaExternalTableExportMapper<LongWritable, Text> mapper) throws Exception {
    Connection connectionMock = mock(Connection.class);

    // PreparadStatement mock should imitate loading stuff from FIFO into Netezza
    PreparedStatement psMock = mock(PreparedStatement.class);
    when(psMock.execute()).then(invocation -> {
      // Write log file under taskAttemptDir to be able to check log upload
      File logFile = mapper.taskAttemptDir.toPath().resolve("job__0000-0-0").resolve("TEST.nzlog").toFile();
      FileUtils.writeStringToFile(logFile, "test log");

      // Need to open FIFO for reading, otherwise writing would hang
      new FileInputStream(mapper.fifoFile.getAbsoluteFile());

      // Simulate delay
      Thread.sleep(200);
      return true;
    });
    when(connectionMock.prepareStatement(anyString())).thenReturn(psMock);

    DBConfiguration dbcMock = mock(DBConfiguration.class);
    when(dbcMock.getConnection()).thenReturn(connectionMock);
    mapper.dbc = dbcMock;
    return mapper;
  }


  /**
   * Creates simple mapreduce context that says it has a single record but won't actually
   * return any records as tests are not expected to read the records.
   * @return
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  private Mapper.Context getContext() throws java.io.IOException, InterruptedException {
    Mapper.Context context = mock(Mapper.Context.class);

    Configuration conf = new Configuration();
    when(context.getConfiguration()).thenReturn(conf);

    TaskAttemptID taskAttemptID = new TaskAttemptID();
    when(context.getTaskAttemptID()).thenReturn(taskAttemptID);

    JobID jobID = new JobID("job001", 1);
    when(context.getJobID()).thenReturn(jobID);

    // Simulate a single record by answering 'true' once
    when(context.nextKeyValue()).thenAnswer(new Answer<Object>() {
      boolean answer = true;

      @Override
      public Object answer(InvocationOnMock invocation) {
        if (answer == true) {
          answer = false;
          return true;
        }
        return false;
      }
    });

    return context;
  }

}