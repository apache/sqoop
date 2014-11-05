/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sqoop.connector.idf;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;
import org.apache.sqoop.schema.type.Type;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;

public class CSVIntermediateDataFormat extends IntermediateDataFormat<String> {

  public static final char SEPARATOR_CHARACTER = ',';
  public static final char ESCAPE_CHARACTER = '\\';
  public static final char QUOTE_CHARACTER = '\'';
  public static final String NULL_STRING = "NULL";


  private static final char[] originals = {
    0x5C,0x00,0x0A,0x0D,0x1A,0x22,0x27
  };

  private static final String[] replacements = {
    new String(new char[] { ESCAPE_CHARACTER, '\\'}),
    new String(new char[] { ESCAPE_CHARACTER, '0'}),
    new String(new char[] { ESCAPE_CHARACTER, 'n'}),
    new String(new char[] { ESCAPE_CHARACTER, 'r'}),
    new String(new char[] { ESCAPE_CHARACTER, 'Z'}),
    new String(new char[] { ESCAPE_CHARACTER, '\"'}),
    new String(new char[] { ESCAPE_CHARACTER, '\''})
  };

  // ISO-8859-1 is an 8-bit codec that is supported in every java implementation.
  public static final String BYTE_FIELD_CHARSET = "ISO-8859-1";

  private final List<Integer> stringFieldIndices = new ArrayList<Integer>();
  private final List<Integer> byteFieldIndices = new ArrayList<Integer>();

  private Schema schema;

  /**
   * {@inheritDoc}
   */
  @Override
  public String getTextData() {
    return data;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setTextData(String text) {
    this.data = text;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setSchema(Schema schema) {
    if(schema == null) {
      return;
    }
    this.schema = schema;
    List<Column> columns = schema.getColumns();
    int i = 0;
    for(Column col : columns) {
      if(col.getType() == Type.TEXT) {
        stringFieldIndices.add(i);
      } else if(col.getType() == Type.BINARY) {
        byteFieldIndices.add(i);
      }
      i++;
    }
  }

  /**
   * Custom CSV parser that honors quoting and escaped quotes.
   * All other escaping is handled elsewhere.
   *
   * @return String[]
   */
  private String[] getFields() {
    if (data == null) {
      return null;
    }

    boolean quoted = false;
    boolean escaped = false;
    List<String> parsedData = new LinkedList<String>();
    StringBuffer buffer = new StringBuffer();
    for (int i = 0; i < data.length(); ++i) {
      char c = data.charAt(i);
      switch(c) {
        case QUOTE_CHARACTER:
          buffer.append(c);
          if (escaped) {
            escaped = false;
          } else {
            quoted = !quoted;
          }
          break;

        case ESCAPE_CHARACTER:
          buffer.append(ESCAPE_CHARACTER);
          escaped = !escaped;
          break;

        case SEPARATOR_CHARACTER:
          if (quoted) {
            buffer.append(c);
          } else {
            parsedData.add(buffer.toString());
            buffer = new StringBuffer();
          }
          break;

        default:
          if (escaped) {
            escaped = false;
          }
          buffer.append(c);
          break;
      }
    }
    parsedData.add(buffer.toString());

    return parsedData.toArray(new String[parsedData.size()]);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object[] getObjectData() {
    if (schema.isEmpty()) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0006);
    }

    String[] fields = getFields();

    if (fields == null) {
      return null;
    }

    if (fields.length != schema.getColumns().size()) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0005,
          "The data " + getTextData() + " has the wrong number of fields.");
    }

    Object[] out = new Object[fields.length];
    Column[] cols = schema.getColumns().toArray(new Column[fields.length]);
    for (int i = 0; i < fields.length; i++) {
      Type colType = cols[i].getType();
      if (fields[i].equals("NULL")) {
        out[i] = null;
        continue;
      }

      Long byteSize;
      switch(colType) {
        case TEXT:
          out[i] = unescapeStrings(fields[i]);
          break;
        case BINARY:
          out[i] = unescapeByteArray(fields[i]);
          break;
        case FIXED_POINT:
          byteSize = ((FixedPoint) cols[i]).getByteSize();
          if (byteSize != null && byteSize <= Integer.SIZE) {
            out[i] = Integer.valueOf(fields[i]);
          } else {
            out[i] = Long.valueOf(fields[i]);
          }
          break;
        case FLOATING_POINT:
          byteSize = ((FloatingPoint) cols[i]).getByteSize();
          if (byteSize != null && byteSize <= Float.SIZE) {
            out[i] = Float.valueOf(fields[i]);
          } else {
            out[i] = Double.valueOf(fields[i]);
          }
          break;
        case DECIMAL:
          out[i] = new BigDecimal(fields[i]);
          break;
        case DATE:
          out[i] = LocalDate.parse(fields[i]);
          break;
        case DATE_TIME:
          // A datetime string with a space as date-time separator will not be
          // parsed expectedly. The expected separator is "T". See also:
          // https://github.com/JodaOrg/joda-time/issues/11
          String iso8601 = fields[i].replace(" ", "T");
          out[i] = LocalDateTime.parse(iso8601);
          break;
        case BIT:
          out[i] = Boolean.valueOf(fields[i].equals("1")
              || fields[i].toLowerCase().equals("true"));
          break;
        default:
          throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0004, "Column type from schema was not recognized for " + colType);
      }
    }
    return out;
  }


  /**
   * {@inheritDoc}
   */
  @VisibleForTesting
  @Override
  public void setObjectData(Object[] data) {
    escapeArray(data);
    this.data = StringUtils.join(data, SEPARATOR_CHARACTER);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.data);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void read(DataInput in) throws IOException {
    data = in.readUTF();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object other) {
    if(this == other) {
      return true;
    }
    if(other == null || !(other instanceof CSVIntermediateDataFormat)) {
      return false;
    }
    return data.equals(((CSVIntermediateDataFormat)other).data);
  }

  public int compareTo(IntermediateDataFormat<?> o) {
    if(this == o) {
      return 0;
    }
    if(this.equals(o)) {
      return 0;
    }
    if(!(o instanceof CSVIntermediateDataFormat)) {
      throw new IllegalStateException("Expected Data to be instance of " +
        "CSVIntermediateFormat, but was an instance of " + o.getClass()
        .getName());
    }
    return data.compareTo(o.getTextData());
  }

  /**
   * If the incoming data is an array, parse it and return the CSV-ised version
   *
   * @param array
   */
  private void escapeArray(Object[] array) {
    for (int i : stringFieldIndices) {
      array[i] = escapeStrings((String) array[i]);
    }
    for (int i : byteFieldIndices) {
      array[i] = escapeByteArrays((byte[]) array[i]);
    }
  }

  private String escapeByteArrays(byte[] bytes) {
    try {
      return escapeStrings(new String(bytes, BYTE_FIELD_CHARSET));
    } catch (UnsupportedEncodingException e) {
      // We should never hit this case.
      // This character set should be distributed with Java.
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0001, "The character set " + BYTE_FIELD_CHARSET + " is not available.");
    }
  }

  private String getRegExp(char orig) {
    return getRegExp(String.valueOf(orig));
  }

  private String getRegExp(String orig) {
    return orig.replaceAll("\\\\", Matcher.quoteReplacement("\\\\"));
  }

  private String escapeStrings(String orig) {
    if (orig == null) {
      return NULL_STRING;
    }

    int j = 0;
    String replacement = orig;
    try {
      for (j = 0; j < replacements.length; j++) {
        replacement = replacement.replaceAll(getRegExp(originals[j]), Matcher.quoteReplacement(replacements[j]));
      }
    } catch (Exception e) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0002, orig + "  " + replacement + "  " + String.valueOf(j) + "  " + e.getMessage());
    }
    StringBuilder  builder = new StringBuilder();
    builder.append(QUOTE_CHARACTER).append(replacement).append(QUOTE_CHARACTER);
    return builder.toString();
  }

  private String unescapeStrings(String orig) {
    //Remove the trailing and starting quotes.
    orig = orig.substring(1, orig.length() - 1);
    int j = 0;
    try {
      for (j = 0; j < replacements.length; j++) {
        orig = orig.replaceAll(getRegExp(replacements[j]),
          Matcher.quoteReplacement(String.valueOf(originals[j])));
      }
    } catch (Exception e) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0003, orig + "  " + String.valueOf(j) + e.getMessage());
    }

    return orig;
  }

  private byte[] unescapeByteArray(String orig) {
    // Always encoded in BYTE_FIELD_CHARSET.
    try {
      return unescapeStrings(orig).getBytes(BYTE_FIELD_CHARSET);
    } catch (UnsupportedEncodingException e) {
      // Should never hit this case.
      // This character set should be distributed with Java.
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0001, "The character set " + BYTE_FIELD_CHARSET + " is not available.");
    }
  }

  public String toString() {
    return data;
  }
}
