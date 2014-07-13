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
package org.apache.sqoop.mapreduce.hcat;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.InputJobInfo;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.ExportJobBase;
import org.apache.sqoop.mapreduce.ImportJobBase;

/**
 * Helper class for Sqoop HCat Integration export jobs.
 */
public class SqoopHCatExportHelper {
  private InputJobInfo jobInfo;
  private HCatSchema hCatFullTableSchema;
  public static final Log LOG = LogFactory
    .getLog(SqoopHCatExportHelper.class.getName());
  private SqoopRecord sqoopRecord;
  private boolean bigDecimalFormatString;
  private static final String TIMESTAMP_TYPE = "java.sql.Timestamp";
  private static final String TIME_TYPE = "java.sql.Time";
  private static final String DATE_TYPE = "java.sql.Date";
  private static final String BIG_DECIMAL_TYPE = "java.math.BigDecimal";
  private static final String FLOAT_TYPE = "Float";
  private static final String DOUBLE_TYPE = "Double";
  private static final String BYTE_TYPE = "Byte";
  private static final String SHORT_TYPE = "Short";
  private static final String INTEGER_TYPE = "Integer";
  private static final String LONG_TYPE = "Long";
  private static final String BOOLEAN_TYPE = "Boolean";
  private static final String STRING_TYPE = "String";
  private static final String BYTESWRITABLE =
    "org.apache.hadoop.io.BytesWritable";
  private static boolean debugHCatExportMapper = false;
  private MapWritable colTypesJava;
  private MapWritable colTypesSql;

  public SqoopHCatExportHelper(Configuration conf)
    throws IOException, InterruptedException {

    colTypesJava = DefaultStringifier.load(conf,
      SqoopHCatUtilities.HCAT_DB_OUTPUT_COLTYPES_JAVA, MapWritable.class);
    colTypesSql = DefaultStringifier.load(conf,
      SqoopHCatUtilities.HCAT_DB_OUTPUT_COLTYPES_SQL, MapWritable.class);
    // Instantiate a copy of the user's class to hold and parse the record.

    String recordClassName = conf.get(
      ExportJobBase.SQOOP_EXPORT_TABLE_CLASS_KEY);
    if (null == recordClassName) {
      throw new IOException("Export table class name ("
        + ExportJobBase.SQOOP_EXPORT_TABLE_CLASS_KEY
        + ") is not set!");
    }

    bigDecimalFormatString = conf.getBoolean(
      ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT,
      ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT_DEFAULT);

    debugHCatExportMapper = conf.getBoolean(
      SqoopHCatUtilities.DEBUG_HCAT_EXPORT_MAPPER_PROP, false);
    try {
      Class<?> cls = Class.forName(recordClassName, true,
        Thread.currentThread().getContextClassLoader());
      sqoopRecord = (SqoopRecord) ReflectionUtils.newInstance(cls, conf);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    }

    if (null == sqoopRecord) {
      throw new IOException("Could not instantiate object of type "
        + recordClassName);
    }

    String inputJobInfoStr = conf.get(HCatConstants.HCAT_KEY_JOB_INFO);
    jobInfo =
      (InputJobInfo) HCatUtil.deserialize(inputJobInfoStr);
    HCatSchema tableSchema = jobInfo.getTableInfo().getDataColumns();
    HCatSchema partitionSchema =
      jobInfo.getTableInfo().getPartitionColumns();
    hCatFullTableSchema = new HCatSchema(tableSchema.getFields());
    for (HCatFieldSchema hfs : partitionSchema.getFields()) {
      hCatFullTableSchema.append(hfs);
    }
  }

  public SqoopRecord convertToSqoopRecord(HCatRecord hcr)
    throws IOException {
    Text key = new Text();
    for (Map.Entry<String, Object> e : sqoopRecord.getFieldMap().entrySet()) {
      String colName = e.getKey();
      String hfn = colName.toLowerCase();
      key.set(hfn);
      String javaColType = colTypesJava.get(key).toString();
      int sqlType = ((IntWritable) colTypesSql.get(key)).get();
      HCatFieldSchema field =
        hCatFullTableSchema.get(hfn);
      HCatFieldSchema.Type fieldType = field.getType();
      Object hCatVal =
        hcr.get(hfn, hCatFullTableSchema);
      String hCatTypeString = field.getTypeString();
      Object sqlVal = convertToSqoop(hCatVal, fieldType,
        javaColType, hCatTypeString);
      if (debugHCatExportMapper) {
        LOG.debug("hCatVal " + hCatVal + " of type "
          + (hCatVal == null ? null : hCatVal.getClass().getName())
          + ",sqlVal " + sqlVal + " of type "
          + (sqlVal == null ? null : sqlVal.getClass().getName())
          + ",java type " + javaColType + ", sql type = "
          + SqoopHCatUtilities.sqlTypeString(sqlType));
      }
      sqoopRecord.setField(colName, sqlVal);
    }
    return sqoopRecord;
  }

  private Object convertToSqoop(Object val,
    HCatFieldSchema.Type fieldType, String javaColType,
    String hCatTypeString) throws IOException {

    if (val == null) {
      return null;
    }

    switch (fieldType) {
      case INT:
      case TINYINT:
      case SMALLINT:
      case FLOAT:
      case DOUBLE:
        val = convertNumberTypes(val, javaColType);
        if (val != null) {
          return val;
        }
        break;
      case BOOLEAN:
        val = convertBooleanTypes(val, javaColType);
        if (val != null) {
          return val;
        }
        break;
      case BIGINT:
        if (javaColType.equals(DATE_TYPE)) {
          return new Date((Long) val);
        } else if (javaColType.equals(TIME_TYPE)) {
          return new Time((Long) val);
        } else if (javaColType.equals(TIMESTAMP_TYPE)) {
          return new Timestamp((Long) val);
        } else {
          val = convertNumberTypes(val, javaColType);
          if (val != null) {
            return val;
          }
        }
        break;
      case DATE:
        Date date = (Date) val;
        if (javaColType.equals(DATE_TYPE)) {
          return date;
        } else if (javaColType.equals(TIME_TYPE)) {
          return new Time(date.getTime());
        } else if (javaColType.equals(TIMESTAMP_TYPE)) {
          return new Timestamp(date.getTime());
        }
        break;
      case TIMESTAMP:
        Timestamp ts = (Timestamp) val;
        if (javaColType.equals(DATE_TYPE)) {
          return new Date(ts.getTime());
        } else if (javaColType.equals(TIME_TYPE)) {
          return new Time(ts.getTime());
        } else if (javaColType.equals(TIMESTAMP_TYPE)) {
          return ts;
        }
        break;
      case STRING:
      case VARCHAR:
      case CHAR:
        val = convertStringTypes(val, javaColType);
        if (val != null) {
          return val;
        }
        break;
      case BINARY:
        val = convertBinaryTypes(val, javaColType);
        if (val != null) {
          return val;
        }
        break;
      case DECIMAL:
        val = convertDecimalTypes(val, javaColType);
        if (val != null) {
          return val;
        }
        break;
      case ARRAY:
      case MAP:
      case STRUCT:
      default:
        throw new IOException("Cannot convert HCatalog type "
          + fieldType);
    }
    LOG.error("Cannot convert HCatalog object of "
      + " type " + hCatTypeString + " to java object type "
      + javaColType);
    return null;
  }

  private Object convertDecimalTypes(Object val, String javaColType) {
    HiveDecimal hd = (HiveDecimal) val;
    BigDecimal bd = hd.bigDecimalValue();

    if (javaColType.equals(BIG_DECIMAL_TYPE)) {
      return bd;
    } else if (javaColType.equals(STRING_TYPE)) {
      String bdStr = null;
      if (bigDecimalFormatString) {
        bdStr = bd.toPlainString();
      } else {
        bdStr = bd.toString();
      }
      return bdStr;
    }
    return null;
  }
  private Object convertBinaryTypes(Object val, String javaColType) {
    byte[] bb = (byte[]) val;
    if (javaColType.equals(BYTESWRITABLE)) {
      BytesWritable bw = new BytesWritable();
      bw.set(bb, 0, bb.length);
      return bw;
    }
    return null;
  }

  private Object convertStringTypes(Object val, String javaColType) {
    String valStr = val.toString();
    if (javaColType.equals(BIG_DECIMAL_TYPE)) {
      return new BigDecimal(valStr);
    } else if (javaColType.equals(DATE_TYPE)
      || javaColType.equals(TIME_TYPE)
      || javaColType.equals(TIMESTAMP_TYPE)) {
      // Oracle expects timestamps for Date also by default based on version
      // Just allow all date types to be assignment compatible
      if (valStr.length() == 10
          && valStr.matches("^\\d{4}-\\d{2}-\\d{2}$")) {
        // Date in yyyy-mm-dd format
        Date d = Date.valueOf(valStr);
        if (javaColType.equals(DATE_TYPE)) {
          return d;
        } else if (javaColType.equals(TIME_TYPE)) {
          return new Time(d.getTime());
        } else if (javaColType.equals(TIMESTAMP_TYPE)) {
          return new Timestamp(d.getTime());
        }
      } else if (valStr.length() == 8
          && valStr.matches("^\\d{2}:\\d{2}:\\d{2}$")) {
        // time in hh:mm:ss
        Time t = Time.valueOf(valStr);
        if (javaColType.equals(DATE_TYPE)) {
          return new Date(t.getTime());
        } else if (javaColType.equals(TIME_TYPE)) {
          return t;
        } else if (javaColType.equals(TIMESTAMP_TYPE)) {
          return new Timestamp(t.getTime());
        }
      } else if (valStr.length() >= 19
          && valStr.length() <= 26
          && valStr.
          matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(.\\d+)?$")) {
        // timestamp in yyyy-mm-dd hh:mm:ss
        Timestamp ts = Timestamp.valueOf(valStr);
        if (javaColType.equals(DATE_TYPE)) {
          return new Date(ts.getTime());
        } else if (javaColType.equals(TIME_TYPE)) {
          return new Time(ts.getTime());
        } else if (javaColType.equals(TIMESTAMP_TYPE)) {
          return ts;
        }
      } else {
        return null;
      }
    } else if (javaColType.equals(STRING_TYPE)) {
      return valStr;
    } else if (javaColType.equals(BOOLEAN_TYPE)) {
      return Boolean.valueOf(valStr);
    } else if (javaColType.equals(BYTE_TYPE)) {
      return Byte.parseByte(valStr);
    } else if (javaColType.equals(SHORT_TYPE)) {
      return Short.parseShort(valStr);
    } else if (javaColType.equals(INTEGER_TYPE)) {
      return Integer.parseInt(valStr);
    } else if (javaColType.equals(LONG_TYPE)) {
      return Long.parseLong(valStr);
    } else if (javaColType.equals(FLOAT_TYPE)) {
      return Float.parseFloat(valStr);
    } else if (javaColType.equals(DOUBLE_TYPE)) {
      return Double.parseDouble(valStr);
    }
    return null;
  }

  private Object convertBooleanTypes(Object val, String javaColType) {
    Boolean b = (Boolean) val;
    if (javaColType.equals(BOOLEAN_TYPE)) {
      return b;
    } else if (javaColType.equals(BYTE_TYPE)) {
      return (byte) (b ? 1 : 0);
    } else if (javaColType.equals(SHORT_TYPE)) {
      return (short) (b ? 1 : 0);
    } else if (javaColType.equals(INTEGER_TYPE)) {
      return (int) (b ? 1 : 0);
    } else if (javaColType.equals(LONG_TYPE)) {
      return (long) (b ? 1 : 0);
    } else if (javaColType.equals(FLOAT_TYPE)) {
      return (float) (b ? 1 : 0);
    } else if (javaColType.equals(DOUBLE_TYPE)) {
      return (double) (b ? 1 : 0);
    } else if (javaColType.equals(BIG_DECIMAL_TYPE)) {
      return new BigDecimal(b ? 1 : 0);
    } else if (javaColType.equals(STRING_TYPE)) {
      return val.toString();
    }
    return null;
  }

  private Object convertNumberTypes(Object val, String javaColType) {
    Number n = (Number) val;
    if (javaColType.equals(BYTE_TYPE)) {
      return n.byteValue();
    } else if (javaColType.equals(SHORT_TYPE)) {
      return n.shortValue();
    } else if (javaColType.equals(INTEGER_TYPE)) {
      return n.intValue();
    } else if (javaColType.equals(LONG_TYPE)) {
      return n.longValue();
    } else if (javaColType.equals(FLOAT_TYPE)) {
      return n.floatValue();
    } else if (javaColType.equals(DOUBLE_TYPE)) {
      return n.doubleValue();
    } else if (javaColType.equals(BIG_DECIMAL_TYPE)) {
      return new BigDecimal(n.doubleValue());
    } else if (javaColType.equals(BOOLEAN_TYPE)) {
      return n.byteValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
    } else if (javaColType.equals(STRING_TYPE)) {
      return n.toString();
    }
    return null;
  }
}
