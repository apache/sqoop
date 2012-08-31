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

  private static final int EMPTY_DATA = 0;
  private static final int CSV_RECORD = 1;
  private static final int ARRAY_RECORD = 2;
  private int type = EMPTY_DATA;

  private static char FIELD_DELIMITER = ',';
  private static char RECORD_DELIMITER = '\n';

  public void setContent(Object content) {
    if (content == null) {
      this.type = EMPTY_DATA;
    } else if (content instanceof String) {
      this.type = CSV_RECORD;
    } else if (content instanceof Object[]) {
      this.type = ARRAY_RECORD;
    } else {
      throw new SqoopException(CoreError.CORE_0012,
          content.getClass().getName());
    }
    this.content = content;
  }

  public Object getContent() {
    return content;
  }

  public int getType() {
    return type;
  }

  public boolean isEmpty() {
    return (type == EMPTY_DATA);
  }

  @Override
  public int compareTo(Data other) {
    byte[] myBytes = toString().getBytes(Charset.forName("UTF-8"));
    byte[] otherBytes = other.toString().getBytes(Charset.forName("UTF-8"));
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
  public String toString() {
    switch (type) {
    case CSV_RECORD:
      return (String)content + RECORD_DELIMITER;
    case ARRAY_RECORD:
      StringBuilder sb = new StringBuilder();
      Object[] array = (Object[])content;
      for (int i = 0; i < array.length; i++) {
        if (i != 0) {
          sb.append(FIELD_DELIMITER);
        }

        if (array[i] instanceof String) {
          sb.append("\'");
          sb.append(((String)array[i]).replaceAll(
              "\'", Matcher.quoteReplacement("\\\'")));
          sb.append("\'");
        } else if (array[i] instanceof byte[]) {
          sb.append(Arrays.toString((byte[])array[i]));
        } else {
          sb.append(array[i].toString());
        }
      }
      sb.append(RECORD_DELIMITER);
      return sb.toString();
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

}
