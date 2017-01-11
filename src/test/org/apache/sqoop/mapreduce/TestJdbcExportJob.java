/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.mapreduce;

import java.io.IOException;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.sqoop.mapreduce.ExportJobBase.FileType;
import org.junit.Test;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.ExportJobContext;


/**
 * Test methods of the JdbcExportJob implementation.
 */
public class TestJdbcExportJob {

  @Test
  public void testAvroWithNoColumnsSpecified() throws Exception {
    SqoopOptions opts = new SqoopOptions();
    opts.setExportDir("myexportdir");
    JdbcExportJob jdbcExportJob = stubJdbcExportJob(opts, FileType.AVRO_DATA_FILE);
    Job job = new Job();
    jdbcExportJob.configureInputFormat(job, null, null, null);
    assertEquals(asSetOfText("Age", "Name", "Gender"), DefaultStringifier.load(job.getConfiguration(), AvroExportMapper.AVRO_COLUMN_TYPES_MAP, MapWritable.class).keySet());
  }

  @Test
  public void testAvroWithAllColumnsSpecified() throws Exception {
    SqoopOptions opts = new SqoopOptions();
    opts.setExportDir("myexportdir");
    String[] columns = { "Age", "Name", "Gender" };
    opts.setColumns(columns);
    JdbcExportJob jdbcExportJob = stubJdbcExportJob(opts, FileType.AVRO_DATA_FILE);
    Job job = new Job();
    jdbcExportJob.configureInputFormat(job, null, null, null);
    assertEquals(asSetOfText("Age", "Name", "Gender"), DefaultStringifier.load(job.getConfiguration(), AvroExportMapper.AVRO_COLUMN_TYPES_MAP, MapWritable.class).keySet());
  }

  @Test
  public void testAvroWithOneColumnSpecified() throws Exception {
    SqoopOptions opts = new SqoopOptions();
    opts.setExportDir("myexportdir");
    String[] columns = { "Gender" };
    opts.setColumns(columns);
    JdbcExportJob jdbcExportJob = stubJdbcExportJob(opts, FileType.AVRO_DATA_FILE);
    Job job = new Job();
    jdbcExportJob.configureInputFormat(job, null, null, null);
    assertEquals(asSetOfText("Gender"), DefaultStringifier.load(job.getConfiguration(), AvroExportMapper.AVRO_COLUMN_TYPES_MAP, MapWritable.class).keySet());
  }

  @Test
  public void testAvroWithSomeColumnsSpecified() throws Exception {
    SqoopOptions opts = new SqoopOptions();
    opts.setExportDir("myexportdir");
    String[] columns = { "Age", "Name" };
    opts.setColumns(columns);
    JdbcExportJob jdbcExportJob = stubJdbcExportJob(opts, FileType.AVRO_DATA_FILE);
    Job job = new Job();
    jdbcExportJob.configureInputFormat(job, null, null, null);
    assertEquals(asSetOfText("Age", "Name"), DefaultStringifier.load(job.getConfiguration(), AvroExportMapper.AVRO_COLUMN_TYPES_MAP, MapWritable.class).keySet());
  }

  @Test
  public void testAvroWithMoreColumnsSpecified() throws Exception {
    SqoopOptions opts = new SqoopOptions();
    opts.setExportDir("myexportdir");
    String[] columns = { "Age", "Name", "Gender", "Address" };
    opts.setColumns(columns);
    JdbcExportJob jdbcExportJob = stubJdbcExportJob(opts, FileType.AVRO_DATA_FILE);
    Job job = new Job();
    jdbcExportJob.configureInputFormat(job, null, null, null);
    assertEquals(asSetOfText("Age", "Name", "Gender"), DefaultStringifier.load(job.getConfiguration(), AvroExportMapper.AVRO_COLUMN_TYPES_MAP, MapWritable.class).keySet());
  }

  private JdbcExportJob stubJdbcExportJob(SqoopOptions opts, final FileType inputFileType) throws IOException {
    ExportJobContext mockContext = mock(ExportJobContext.class);
    when(mockContext.getOptions()).thenReturn(opts);
    ConnManager mockConnManager = mock(ConnManager.class);
    Map<String, Integer> columnTypeInts = new HashMap<String, Integer>();
    columnTypeInts.put("Name", Types.VARCHAR);
    columnTypeInts.put("Age", Types.SMALLINT);
    columnTypeInts.put("Gender", Types.CHAR);
    when(mockConnManager.getColumnTypes(anyString(), anyString())).thenReturn(columnTypeInts);
    when(mockConnManager.toJavaType(anyString(), anyString(), anyInt())).thenReturn("String");
    when(mockContext.getConnManager()).thenReturn(mockConnManager);
    JdbcExportJob jdbcExportJob = new JdbcExportJob(mockContext) {
      @Override
      protected FileType getInputFileType() {
        return inputFileType;
      }
    };
    jdbcExportJob.options = opts;
    return jdbcExportJob;
  }

  private Set<Text> asSetOfText(String... strings) {
    Set<Text> setOfText = new HashSet<Text>();
    for (String string : strings) {
      setOfText.add(new Text(string));
    }
    return setOfText;
  }
}