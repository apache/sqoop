package org.apache.sqoop.mapreduce.db;

import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.mapreduce.db.DBConfiguration;
import com.cloudera.sqoop.mapreduce.db.DBInputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.lib.SqoopRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestSQLServerDBRecordReader {

  private static final String SPLIT_BY_COLUMN = "myCol";
  private static final String COL_NAME_SAME_AS_SPLIT_BY = SPLIT_BY_COLUMN;
  private static final String UPPERCASE_COL_NAME = SPLIT_BY_COLUMN.toUpperCase();
  private static final String ANY_VALUE_FOR_COL = "Value";
  private static final String NULL_VALUE_FOR_COL = null;

  private SQLServerDBRecordReader reader;

  @Before
  public void before() throws Exception {
    DBInputFormat.DBInputSplit split = mock(DBInputFormat.DBInputSplit.class);
    Configuration conf = new Configuration();
    conf.set(SQLServerDBInputFormat.IMPORT_FAILURE_HANDLER_CLASS, SQLFailureHandlerStub.class.getName());
    Connection connection = mock(Connection.class);
    DBConfiguration dbConfiguration = mock(DBConfiguration.class);
    when(dbConfiguration.getInputOrderBy()).thenReturn(SPLIT_BY_COLUMN);

    reader = spy(new SQLServerDBRecordReader(split, SqlTableClassStub.class, conf, connection, dbConfiguration, "", new String[]{}, "", ""));


    doAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocationOnMock) throws Throwable {
        return StringUtils.EMPTY;
      }
    }).when(reader).getSelectQuery();

    doAnswer(new Answer<ResultSet>() {
      @Override
      public ResultSet answer(InvocationOnMock invocationOnMock) throws Throwable {
        return mock(ResultSet.class);
      }
    }).when(reader).executeQuery(anyString());

    reader.initialize(mock(InputSplit.class), mock(TaskAttemptContext.class));

  }

  @Test
  public void returnNullIfTheLastRecordValueIsNull() {
    when(reader.currentValue()).thenReturn(new SqlTableClassStub(COL_NAME_SAME_AS_SPLIT_BY, NULL_VALUE_FOR_COL));
    reader.getCurrentValue();
    assertEquals(NULL_VALUE_FOR_COL, reader.getLastRecordValue());
  }

  @Test
  public void returnNullIfTheLastRecordValueIsNullAndColumnNameIsDifferent() {
    when(reader.currentValue()).thenReturn(new SqlTableClassStub(UPPERCASE_COL_NAME, NULL_VALUE_FOR_COL));
    reader.getCurrentValue();
    assertEquals(NULL_VALUE_FOR_COL, reader.getLastRecordValue());
  }

  @Test
  public void returnLastSavedValueWhenColumNameIsTheSameSplitByColumn() {
    when(reader.currentValue()).thenReturn(new SqlTableClassStub(COL_NAME_SAME_AS_SPLIT_BY, ANY_VALUE_FOR_COL));
    reader.getCurrentValue();

    assertEquals(ANY_VALUE_FOR_COL, reader.getLastRecordValue());
  }

  /*
   * This test intended to test if the table name and query parameter wouldn't
   * match (eg.: mycol, MyCol) if the DB is case insensitive
   */
  @Test
  public void returnLastSavedValueWhenColumnNameDifferentFromSplitByColumn() {
    when(reader.currentValue()).thenReturn(new SqlTableClassStub(UPPERCASE_COL_NAME, ANY_VALUE_FOR_COL));
    reader.getCurrentValue();

    assertEquals(ANY_VALUE_FOR_COL, reader.getLastRecordValue());
  }

  private static class SqlTableClassStub extends SqoopRecord {
    private String colName;
    private String colValue;

    public SqlTableClassStub(String colName, String colValue) {
      this.colName = colName;
      this.colValue = colValue;
    }

    @Override
    public Map<String, Object> getFieldMap() {
      return new HashMap<String, Object>() {{
        put(colName, colValue);
      }};
    }

    @Override
    public void parse(CharSequence s) throws RecordParser.ParseError {

    }

    @Override
    public void parse(Text s) throws RecordParser.ParseError {

    }

    @Override
    public void parse(byte[] s) throws RecordParser.ParseError {

    }

    @Override
    public void parse(char[] s) throws RecordParser.ParseError {

    }

    @Override
    public void parse(ByteBuffer s) throws RecordParser.ParseError {

    }

    @Override
    public void parse(CharBuffer s) throws RecordParser.ParseError {

    }

    @Override
    public void loadLargeObjects(LargeObjectLoader objLoader) throws SQLException, IOException, InterruptedException {

    }

    @Override
    public int write(PreparedStatement stmt, int offset) throws SQLException {
      return 0;
    }

    @Override
    public String toString(DelimiterSet delimiters) {
      return null;
    }

    @Override
    public int getClassFormatVersion() {
      return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {

    }


  }

  private static class SQLFailureHandlerStub extends SQLFailureHandler {

    @Override
    public boolean canHandleFailure(Throwable failureCause) {
      return false;
    }

    @Override
    public Connection recover() throws IOException {
      return null;
    }
  }

}