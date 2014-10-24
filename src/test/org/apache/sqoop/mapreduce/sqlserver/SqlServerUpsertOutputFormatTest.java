package org.apache.sqoop.mapreduce.sqlserver;

import static org.junit.Assert.assertEquals;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.sqoop.manager.SQLServerManager;
import org.apache.sqoop.mapreduce.ExportJobBase;
import org.apache.sqoop.mapreduce.sqlserver.SqlServerUpsertOutputFormat.SqlServerUpsertRecordWriter;
import org.junit.Test;

public class SqlServerUpsertOutputFormatTest {

  @SuppressWarnings("unchecked")
  @Test
  public void Merge_statement_is_parameterized_correctly() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, org.hsqldb.jdbcDriver.class.getName());
    conf.set(DBConfiguration.URL_PROPERTY, "jdbc:hsqldb:.");
    conf.set(ExportJobBase.SQOOP_EXPORT_UPDATE_COL_KEY, "");
    conf.set(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, "");
    String tableName = "#myTable";
    String[] columnNames = { "FirstColumn", "SecondColumn", "ThirdColumn" };
    String[] updateKeyColumns = { "FirstColumn" };
    conf.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, tableName);
    conf.set(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, StringUtils.join(columnNames, ','));
    conf.set(ExportJobBase.SQOOP_EXPORT_UPDATE_COL_KEY, StringUtils.join(updateKeyColumns, ','));
    conf.set(SQLServerManager.TABLE_HINTS_PROP, "NOLOCK");
    conf.set(SQLServerManager.IDENTITY_INSERT_PROP, "true");
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    SqlServerUpsertOutputFormat outputFormat = new SqlServerUpsertOutputFormat();
    SqlServerUpsertRecordWriter recordWriter = outputFormat.new SqlServerUpsertRecordWriter(context);
    assertEquals("SET IDENTITY_INSERT #myTable ON " +
      "MERGE INTO #myTable AS _target USING ( VALUES ( ?, ?, ? ) ) AS _source ( FirstColumn, SecondColumn, ThirdColumn ) ON _source.FirstColumn = _target.FirstColumn" +
      "  WHEN MATCHED THEN UPDATE SET _target.SecondColumn = _source.SecondColumn, _target.ThirdColumn = _source.ThirdColumn" +
      "  WHEN NOT MATCHED THEN INSERT ( FirstColumn, SecondColumn, ThirdColumn ) VALUES " +
      "( _source.FirstColumn, _source.SecondColumn, _source.ThirdColumn ) " +
      "OPTION (NOLOCK);", recordWriter.getUpdateStatement());
  }
}
