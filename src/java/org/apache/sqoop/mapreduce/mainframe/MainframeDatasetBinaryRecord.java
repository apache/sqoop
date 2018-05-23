package org.apache.sqoop.mapreduce.mainframe;

import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.lib.LargeObjectLoader;
import org.apache.sqoop.lib.RecordParser;
import org.apache.commons.el.IntegerLiteral;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.sqoop.lib.SqoopRecord;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MainframeDatasetBinaryRecord extends SqoopRecord {

  private byte[] field;
  private static final Log LOG = LogFactory.getLog(
    MainframeDatasetBinaryRecord.class.getName());

  public Map<String, Object> getFieldMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("fieldName", field);
    return map;
  }

  public void setField(String fieldName, Object fieldVal) {
    if (fieldVal instanceof byte[]) {
      field = (byte[]) fieldVal;
    }
  }

  public void setField(final byte[] val) {
    this.field = val;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    in.readFully(field);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.write(field);
  }

  @Override
  public void readFields(ResultSet rs) throws SQLException {
    field = rs.getBytes(1);
  }

  @Override
  public void write(PreparedStatement s) throws SQLException {
    s.setBytes(1, field);
  }

  @Override
  public String toString() {
    return field.toString();
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
  public int hashCode() {
    return field.hashCode();
  }

  public void loadLargeObjects(LargeObjectLoader loader) {
  }

  public void parse(CharSequence s) {
  }

  public void parse(Text s) {
  }

  public void parse(byte[] s) {
  }

  public void parse(char[] s) {
  }

  public void parse(ByteBuffer s) {
  }

  public void parse(CharBuffer s) {
  }
}