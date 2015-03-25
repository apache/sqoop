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

package org.apache.sqoop.mapreduce.sqlserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.lang.reflect.Constructor;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.sqoop.manager.SQLServerManager;
import org.apache.sqoop.mapreduce.ExportJobBase;
import org.apache.sqoop.mapreduce.sqlserver.SqlServerUpsertOutputFormat.SqlServerUpsertRecordWriter;
import org.junit.Test;

public class SqlServerUpsertOutputFormatTest {

  @SuppressWarnings("unchecked")
  @Test
  public void Merge_statement_is_parameterized_correctly() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY,
      org.hsqldb.jdbcDriver.class.getName());
    conf.set(DBConfiguration.URL_PROPERTY, "jdbc:hsqldb:.");
    conf.set(ExportJobBase.SQOOP_EXPORT_UPDATE_COL_KEY, "");
    conf.set(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, "");
    String tableName = "#myTable";
    String[] columnNames = { "FirstColumn", "SecondColumn", "ThirdColumn" };
    String[] updateKeyColumns = { "FirstColumn" };
    conf.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, tableName);
    conf.set(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY,
      StringUtils.join(columnNames, ','));
    conf.set(ExportJobBase.SQOOP_EXPORT_UPDATE_COL_KEY,
      StringUtils.join(updateKeyColumns, ','));
    conf.set(SQLServerManager.TABLE_HINTS_PROP, "NOLOCK");
    conf.set(SQLServerManager.IDENTITY_INSERT_PROP, "true");
    TaskAttemptContext context = null;
    Class cls = null;
    try {
      cls =
        Class
        .forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
    }
    catch(ClassNotFoundException cnfe) {
      // Not hadoop 2.0
    }
    if (cls == null) {
      try {
        cls =
          Class
          .forName("org.apache.hadoop.mapreduce.task.TaskAttemptContext");
      }
      catch(ClassNotFoundException cnfe) {
        // Something wrong
      }
    }
    assertNotNull(cls);
    Constructor c = cls.getConstructor(Configuration.class,
        TaskAttemptID.class);
     context = (TaskAttemptContext)c.newInstance(conf, new TaskAttemptID());
    SqlServerUpsertOutputFormat outputFormat =
        new SqlServerUpsertOutputFormat();
    SqlServerUpsertRecordWriter recordWriter =
        outputFormat.new SqlServerUpsertRecordWriter(context);
    assertEquals("SET IDENTITY_INSERT #myTable ON "
      + "MERGE INTO #myTable AS _target USING ( VALUES ( ?, ?, ? ) )"
      + " AS _source ( FirstColumn, SecondColumn, ThirdColumn ) ON "
      + "_source.FirstColumn = _target.FirstColumn"
      + "  WHEN MATCHED THEN UPDATE SET _target.SecondColumn = "
      + "_source.SecondColumn, _target.ThirdColumn = _source.ThirdColumn"
      + "  WHEN NOT MATCHED THEN INSERT ( FirstColumn, SecondColumn,"
      + " ThirdColumn ) VALUES "
      + "( _source.FirstColumn, _source.SecondColumn, _source.ThirdColumn ) "
      + "OPTION (NOLOCK);", recordWriter.getUpdateStatement());
  }
}
