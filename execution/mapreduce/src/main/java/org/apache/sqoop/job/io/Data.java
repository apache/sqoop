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
package org.apache.sqoop.job.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.core.CoreError;

public class Data implements WritableComparable<Data> {

  // The content is an Object to accommodate different kinds of data.
  // For example, it can be:
  // - Object[] for an array of object record
  // - String for a text of CSV record
  private Object content = null;

  public static final int EMPTY_DATA = 0;
  public static final int CSV_RECORD = 1;
  public static final int ARRAY_RECORD = 2;
  private int type = EMPTY_DATA;

  public static final String CHARSET_NAME = "UTF-8";

  public static final char DEFAULT_RECORD_DELIMITER = '\n';
  public static final char DEFAULT_FIELD_DELIMITER = ',';
  public static final char DEFAULT_STRING_DELIMITER = '\'';
  public static final char DEFAULT_STRING_ESCAPE = '\\';
  private char fieldDelimiter = DEFAULT_FIELD_DELIMITER;
  private char stringDelimiter = DEFAULT_STRING_DELIMITER;
  private char stringEscape = DEFAULT_STRING_ESCAPE;
  private String escapedStringDelimiter = String.valueOf(new char[] {
      stringEscape, stringDelimiter
  });

  public void setFieldDelimiter(char fieldDelimiter) {
    this.fieldDelimiter = fieldDelimiter;
  }

  public void setContent(Object content, int type) {
    switch (type) {
    case EMPTY_DATA:
    case CSV_RECORD:
    case ARRAY_RECORD:
      this.type = type;
      this.content = content;
      break;
    default:
      throw new SqoopException(CoreError.CORE_0012, String.valueOf(type));
    }
  }

  public Object getContent(int targetType) {
    switch (targetType) {
    case CSV_RECORD:
      return format();
    case ARRAY_RECORD:
      return parse();
    default:
      throw new SqoopException(CoreError.CORE_0012, String.valueOf(targetType));
    }
  }

  public int getType() {
    return type;
  }

  public boolean isEmpty() {
    return (type == EMPTY_DATA);
  }

  @Override
  public String toString() {
    return (String)getContent(CSV_RECORD);
  }

  @Override
  public int compareTo(Data other) {
    byte[] myBytes = toString().getBytes(Charset.forName(CHARSET_NAME));
    byte[] otherBytes = other.toString().getBytes(
        Charset.forName(CHARSET_NAME));
    return WritableComparator.compareBytes(
        myBytes, 0, myBytes.length, otherBytes, 0, otherBytes.length);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Data)) {
      return false;
    }

    Data data = (Data)other;
    if (type != data.getType()) {
      return false;
    }

    return toString().equals(data.toString());
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    switch (type) {
    case CSV_RECORD:
      result += 31 * content.hashCode();
      return result;
    case ARRAY_RECORD:
      Object[] array = (Object[])content;
      for (int i = 0; i < array.length; i++) {
        result += 31 * array[i].hashCode();
      }
      return result;
    default:
      throw new SqoopException(CoreError.CORE_0012, String.valueOf(type));
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    type = readType(in);
    switch (type) {
    case CSV_RECORD:
      readCsv(in);
      break;
    case ARRAY_RECORD:
      readArray(in);
      break;
    default:
      throw new SqoopException(CoreError.CORE_0012, String.valueOf(type));
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    writeType(out, type);
    switch (type) {
    case CSV_RECORD:
      writeCsv(out);
      break;
    case ARRAY_RECORD:
      writeArray(out);
      break;
    default:
      throw new SqoopException(CoreError.CORE_0012, String.valueOf(type));
    }
  }

  private int readType(DataInput in) throws IOException {
    return WritableUtils.readVInt(in);
  }

  private void writeType(DataOutput out, int type) throws IOException {
    WritableUtils.writeVInt(out, type);
  }

  private void readCsv(DataInput in) throws IOException {
    content = in.readUTF();
  }

  private void writeCsv(DataOutput out) throws IOException {
    out.writeUTF((String)content);
  }

  private void readArray(DataInput in) throws IOException {
    // read number of columns
    int columns = in.readInt();
    content = new Object[columns];
    Object[] array = (Object[])content;
    // read each column
    for (int i = 0; i < array.length; i++) {
      int type = readType(in);
      switch (type) {
      case FieldTypes.UTF:
        array[i] = in.readUTF();
        break;

      case FieldTypes.BIN:
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        array[i] = bytes;
        break;

      case FieldTypes.DOUBLE:
        array[i] = in.readDouble();
        break;

      case FieldTypes.FLOAT:
        array[i] = in.readFloat();
        break;

      case FieldTypes.LONG:
        array[i] = in.readLong();
        break;

      case FieldTypes.INT:
        array[i] = in.readInt();
        break;

      case FieldTypes.SHORT:
        array[i] = in.readShort();
        break;

      case FieldTypes.CHAR:
        array[i] = in.readChar();
        break;

      case FieldTypes.BYTE:
        array[i] = in.readByte();
        break;

      case FieldTypes.BOOLEAN:
        array[i] = in.readBoolean();
        break;

      case FieldTypes.NULL:
        array[i] = null;
        break;

      default:
        throw new IOException(
          new SqoopException(CoreError.CORE_0012, Integer.toString(type))
        );
      }
    }
  }

  private void writeArray(DataOutput out) throws IOException {
    Object[] array = (Object[])content;
    // write number of columns
    out.writeInt(array.length);
    // write each column
    for (int i = 0; i < array.length; i++) {
      if (array[i] instanceof String) {
        writeType(out, FieldTypes.UTF);
        out.writeUTF((String)array[i]);

      } else if (array[i] instanceof byte[]) {
        writeType(out, FieldTypes.BIN);
        out.writeInt(((byte[])array[i]).length);
        out.write((byte[])array[i]);

      } else if (array[i] instanceof Double) {
        writeType(out, FieldTypes.DOUBLE);
        out.writeDouble((Double)array[i]);

      } else if (array[i] instanceof Float) {
        writeType(out, FieldTypes.FLOAT);
        out.writeFloat((Float)array[i]);

      } else if (array[i] instanceof Long) {
        writeType(out, FieldTypes.LONG);
        out.writeLong((Long)array[i]);

      } else if (array[i] instanceof Integer) {
        writeType(out, FieldTypes.INT);
        out.writeInt((Integer)array[i]);

      } else if (array[i] instanceof Short) {
        writeType(out, FieldTypes.SHORT);
        out.writeShort((Short)array[i]);

      } else if (array[i] instanceof Character) {
        writeType(out, FieldTypes.CHAR);
        out.writeChar((Character)array[i]);

      } else if (array[i] instanceof Byte) {
        writeType(out, FieldTypes.BYTE);
        out.writeByte((Byte)array[i]);

      } else if (array[i] instanceof Boolean) {
        writeType(out, FieldTypes.BOOLEAN);
        out.writeBoolean((Boolean)array[i]);

      } else if (array[i] == null) {
        writeType(out, FieldTypes.NULL);

      } else {
        throw new IOException(
          new SqoopException(
              CoreError.CORE_0012, array[i].getClass().getName()
          )
        );
      }
    }
  }

  private String format() {
    switch (type) {
    case EMPTY_DATA:
      return null;

    case CSV_RECORD:
      if (fieldDelimiter == DEFAULT_FIELD_DELIMITER) {
        return (String)content;
      } else {
        // TODO: need to exclude the case where comma is part of a string.
        return ((String)content).replaceAll(
            String.valueOf(DEFAULT_FIELD_DELIMITER),
            String.valueOf(fieldDelimiter));
      }

    case ARRAY_RECORD:
      StringBuilder sb = new StringBuilder();
      Object[] array = (Object[])content;
      for (int i = 0; i < array.length; i++) {
        if (i != 0) {
          sb.append(fieldDelimiter);
        }

        if (array[i] instanceof String) {
          sb.append(stringDelimiter);
          sb.append(escape((String)array[i]));
          sb.append(stringDelimiter);
        } else if (array[i] instanceof byte[]) {
          sb.append(Arrays.toString((byte[])array[i]));
        } else {
          sb.append(String.valueOf(array[i]));
        }
      }
      return sb.toString();

    default:
      throw new SqoopException(CoreError.CORE_0012, String.valueOf(type));
    }
  }

  private Object[] parse() {
    switch (type) {
    case EMPTY_DATA:
      return null;

    case CSV_RECORD:
      ArrayList<Object> list = new ArrayList<Object>();
      // todo: need to parse CSV into Array
      return list.toArray();

    case ARRAY_RECORD:
      return (Object[])content;

    default:
      throw new SqoopException(CoreError.CORE_0012, String.valueOf(type));
    }
  }

  private String escape(String string) {
    // TODO: Also need to escape those special characters as documented in:
    // https://cwiki.apache.org/confluence/display/SQOOP/Sqoop2+Intermediate+representation#Sqoop2Intermediaterepresentation-Intermediateformatrepresentationproposal
    String regex = String.valueOf(stringDelimiter);
    String replacement = Matcher.quoteReplacement(escapedStringDelimiter);
    return string.replaceAll(regex, replacement);
  }

}
