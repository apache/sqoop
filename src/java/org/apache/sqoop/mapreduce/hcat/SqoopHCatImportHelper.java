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
import java.math.MathContext;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.InputJobInfo;
import org.apache.hive.hcatalog.mapreduce.StorerInfo;
import org.apache.sqoop.lib.SqoopRecord;
import org.apache.sqoop.mapreduce.ImportJobBase;

import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.LargeObjectLoader;

/**
 * Helper class for Sqoop HCat Integration import jobs.
 */
public class SqoopHCatImportHelper {
  public static final Log LOG = LogFactory.getLog(SqoopHCatImportHelper.class
    .getName());

  private static boolean debugHCatImportMapper = false;

  private InputJobInfo jobInfo;
  private HCatSchema hCatFullTableSchema;
  private int fieldCount;
  private boolean bigDecimalFormatString;
  private LargeObjectLoader lobLoader;
  private HCatSchema partitionSchema = null;
  private HCatSchema dataColsSchema = null;
  private String hiveDelimsReplacement;
  private boolean doHiveDelimsReplacement = false;
  private DelimiterSet hiveDelimiters;
  private String[] staticPartitionKeys;
  private int[] hCatFieldPositions;
  private int colCount;

  public SqoopHCatImportHelper(Configuration conf) throws IOException,
    InterruptedException {

    String inputJobInfoStr = conf.get(HCatConstants.HCAT_KEY_JOB_INFO);
    jobInfo = (InputJobInfo) HCatUtil.deserialize(inputJobInfoStr);
    dataColsSchema = jobInfo.getTableInfo().getDataColumns();
    partitionSchema = jobInfo.getTableInfo().getPartitionColumns();
    StringBuilder storerInfoStr = new StringBuilder(1024);
    StorerInfo storerInfo = jobInfo.getTableInfo().getStorerInfo();
    storerInfoStr.append("HCatalog Storer Info : ").append("\n\tHandler = ")
      .append(storerInfo.getStorageHandlerClass())
      .append("\n\tInput format class = ").append(storerInfo.getIfClass())
      .append("\n\tOutput format class = ").append(storerInfo.getOfClass())
      .append("\n\tSerde class = ").append(storerInfo.getSerdeClass());
    Properties storerProperties = storerInfo.getProperties();
    if (!storerProperties.isEmpty()) {
      storerInfoStr.append("\nStorer properties ");
      for (Map.Entry<Object, Object> entry : storerProperties.entrySet()) {
        String key = (String) entry.getKey();
        Object val = entry.getValue();
        storerInfoStr.append("\n\t").append(key).append('=').append(val);
      }
    }
    storerInfoStr.append("\n");
    LOG.info(storerInfoStr);

    hCatFullTableSchema = new HCatSchema(dataColsSchema.getFields());
    for (HCatFieldSchema hfs : partitionSchema.getFields()) {
      hCatFullTableSchema.append(hfs);
    }
    fieldCount = hCatFullTableSchema.size();
    lobLoader = new LargeObjectLoader(conf, new Path(jobInfo.getTableInfo().getTableLocation()));
    bigDecimalFormatString = conf.getBoolean(
      ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT,
      ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT_DEFAULT);
    debugHCatImportMapper = conf.getBoolean(
      SqoopHCatUtilities.DEBUG_HCAT_IMPORT_MAPPER_PROP, false);
    IntWritable[] delimChars = DefaultStringifier.loadArray(conf,
      SqoopHCatUtilities.HIVE_DELIMITERS_TO_REPLACE_PROP, IntWritable.class);
    hiveDelimiters = new DelimiterSet((char) delimChars[0].get(),
      (char) delimChars[1].get(), (char) delimChars[2].get(),
      (char) delimChars[3].get(), delimChars[4].get() == 1 ? true : false);
    hiveDelimsReplacement = conf
      .get(SqoopHCatUtilities.HIVE_DELIMITERS_REPLACEMENT_PROP);
    if (hiveDelimsReplacement == null) {
      hiveDelimsReplacement = "";
    }
    doHiveDelimsReplacement = Boolean.valueOf(conf
      .get(SqoopHCatUtilities.HIVE_DELIMITERS_REPLACEMENT_ENABLED_PROP));

    IntWritable[] fPos = DefaultStringifier.loadArray(conf,
      SqoopHCatUtilities.HCAT_FIELD_POSITIONS_PROP, IntWritable.class);
    hCatFieldPositions = new int[fPos.length];
    for (int i = 0; i < fPos.length; ++i) {
      hCatFieldPositions[i] = fPos[i].get();
    }

    LOG.debug("Hive delims replacement enabled : " + doHiveDelimsReplacement);
    LOG.debug("Hive Delimiters : " + hiveDelimiters.toString());
    LOG.debug("Hive delimiters replacement : " + hiveDelimsReplacement);
    staticPartitionKeys = conf
      .getStrings(SqoopHCatUtilities.HCAT_STATIC_PARTITION_KEY_PROP);
    String partKeysString = staticPartitionKeys == null ? ""
      : Arrays.toString(staticPartitionKeys);
    LOG.debug("Static partition key used : "  + partKeysString);
  }

  public HCatRecord convertToHCatRecord(SqoopRecord sqr) throws IOException,
    InterruptedException {
    try {
      // Loading of LOBs was delayed until we have a Context.
      sqr.loadLargeObjects(lobLoader);
    } catch (SQLException sqlE) {
      throw new IOException(sqlE);
    }
    if (colCount == -1) {
      colCount = sqr.getFieldMap().size();
    }

    Map<String, Object> fieldMap = sqr.getFieldMap();
    HCatRecord result = new DefaultHCatRecord(fieldCount);

    for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
      String key = entry.getKey();
      Object val = entry.getValue();
      String hfn = key.toLowerCase();
      boolean skip = false;
      if (staticPartitionKeys != null && staticPartitionKeys.length > 0) {
        for (int i = 0; i < staticPartitionKeys.length; ++i) {
          if (staticPartitionKeys[i].equals(hfn)) {
            skip = true;
            break;
          }
        }
      }
      if (skip) {
        continue;
      }
      HCatFieldSchema hfs = hCatFullTableSchema.get(hfn);
      if (debugHCatImportMapper) {
        LOG.debug("SqoopRecordVal: field = " + key + " Val " + val
          + " of type " + (val == null ? null : val.getClass().getName())
          + ", hcattype " + hfs.getTypeString());
      }
      Object hCatVal = toHCat(val, hfs);

      result.set(hfn, hCatFullTableSchema, hCatVal);
    }

    return result;
  }

  private Object toHCat(Object val, HCatFieldSchema hfs) {
    HCatFieldSchema.Type hfsType = hfs.getType();
    if (val == null) {
      return null;
    }

    Object retVal = null;

    if (val instanceof Number) {
      retVal = convertNumberTypes(val, hfs);
    } else if (val instanceof Boolean) {
      retVal = convertBooleanTypes(val, hfs);
    } else if (val instanceof String) {
      retVal = convertStringTypes(val, hfs);
    } else if (val instanceof java.util.Date) {
      retVal = converDateTypes(val, hfs);
    } else if (val instanceof BytesWritable) {
      if (hfsType == HCatFieldSchema.Type.BINARY) {
        BytesWritable bw = (BytesWritable) val;
        retVal = bw.getBytes();
      }
    } else if (val instanceof BlobRef) {
      if (hfsType == HCatFieldSchema.Type.BINARY) {
        BlobRef br = (BlobRef) val;
        byte[] bytes = br.isExternal() ? br.toString().getBytes() : br
          .getData();
        retVal = bytes;
      }
    } else if (val instanceof ClobRef) {
      retVal = convertClobType(val, hfs);
    } else {
      throw new UnsupportedOperationException("Objects of type "
        + val.getClass().getName() + " are not suported");
    }
    if (retVal == null) {
      LOG.error("Unable to convert [" + val
        + "]  of type " + val.getClass().getName()
        + " to HCatalog type " + hfs.getTypeString());
    }
    return retVal;
  }

  private Object convertClobType(Object val, HCatFieldSchema hfs) {
    HCatFieldSchema.Type hfsType = hfs.getType();
    ClobRef cr = (ClobRef) val;
    String s = cr.isExternal() ? cr.toString() : cr.getData();

    if (hfsType == HCatFieldSchema.Type.STRING) {
      return s;
    } else if (hfsType == HCatFieldSchema.Type.VARCHAR) {
      VarcharTypeInfo vti = (VarcharTypeInfo) hfs.getTypeInfo();
      HiveVarchar hvc = new HiveVarchar(s, vti.getLength());
      return hvc;
    } else if (hfsType == HCatFieldSchema.Type.CHAR) {
      CharTypeInfo cti = (CharTypeInfo) hfs.getTypeInfo();
      HiveChar hc = new HiveChar(s, cti.getLength());
      return hc;
    }
    return null;
  }

  private Object converDateTypes(Object val, HCatFieldSchema hfs) {
    HCatFieldSchema.Type hfsType = hfs.getType();
    Date d;
    Time t;
    Timestamp ts;
    if (val instanceof java.sql.Date) {
      d = (Date) val;
      if (hfsType == HCatFieldSchema.Type.DATE) {
        return d;
      } else if (hfsType == HCatFieldSchema.Type.TIMESTAMP) {
        return new Timestamp(d.getTime());
      } else if (hfsType == HCatFieldSchema.Type.BIGINT) {
        return (d.getTime());
      } else if (hfsType == HCatFieldSchema.Type.STRING) {
        return val.toString();
      } else if (hfsType == HCatFieldSchema.Type.VARCHAR) {
        VarcharTypeInfo vti = (VarcharTypeInfo) hfs.getTypeInfo();
        HiveVarchar hvc = new HiveVarchar(val.toString(), vti.getLength());
        return hvc;
      } else if (hfsType == HCatFieldSchema.Type.CHAR) {
        CharTypeInfo cti = (CharTypeInfo) hfs.getTypeInfo();
        HiveChar hChar = new HiveChar(val.toString(), cti.getLength());
        return hChar;
      }
    } else if (val instanceof java.sql.Time) {
      t = (Time) val;
      if (hfsType == HCatFieldSchema.Type.DATE) {
        return new Date(t.getTime());
      } else if (hfsType == HCatFieldSchema.Type.TIMESTAMP) {
        return new Timestamp(t.getTime());
      } else if (hfsType == HCatFieldSchema.Type.BIGINT) {
        return ((Time) val).getTime();
      } else if (hfsType == HCatFieldSchema.Type.STRING) {
        return val.toString();
      } else if (hfsType == HCatFieldSchema.Type.VARCHAR) {
        VarcharTypeInfo vti = (VarcharTypeInfo) hfs.getTypeInfo();
        HiveVarchar hvc = new HiveVarchar(val.toString(), vti.getLength());
        return hvc;
      } else if (hfsType == HCatFieldSchema.Type.CHAR) {
        CharTypeInfo cti = (CharTypeInfo) hfs.getTypeInfo();
        HiveChar hChar = new HiveChar(val.toString(), cti.getLength());
        return hChar;
      }
    } else if (val instanceof java.sql.Timestamp) {
      ts = (Timestamp) val;
      if (hfsType == HCatFieldSchema.Type.DATE) {
        return new Date(ts.getTime());
      } else if (hfsType == HCatFieldSchema.Type.TIMESTAMP) {
        return ts;
      } else if (hfsType == HCatFieldSchema.Type.BIGINT) {
        return ts.getTime();
      } else if (hfsType == HCatFieldSchema.Type.STRING) {
        return val.toString();
      } else if (hfsType == HCatFieldSchema.Type.VARCHAR) {
        VarcharTypeInfo vti = (VarcharTypeInfo) hfs.getTypeInfo();
        HiveVarchar hvc = new HiveVarchar(val.toString(), vti.getLength());
        return hvc;
      } else if (hfsType == HCatFieldSchema.Type.CHAR) {
        CharTypeInfo cti = (CharTypeInfo) hfs.getTypeInfo();
        HiveChar hc = new HiveChar(val.toString(), cti.getLength());
        return hc;
      }
    }
    return null;
  }

  private Object convertStringTypes(Object val, HCatFieldSchema hfs) {
    HCatFieldSchema.Type hfsType = hfs.getType();
    if (hfsType == HCatFieldSchema.Type.STRING
        || hfsType == HCatFieldSchema.Type.VARCHAR
        || hfsType == HCatFieldSchema.Type.CHAR) {
      String str = val.toString();
      if (doHiveDelimsReplacement) {
        str = FieldFormatter.hiveStringReplaceDelims(str,
          hiveDelimsReplacement, hiveDelimiters);
      }
      if (hfsType == HCatFieldSchema.Type.STRING) {
        return str;
      } else if (hfsType == HCatFieldSchema.Type.VARCHAR) {
        VarcharTypeInfo vti = (VarcharTypeInfo) hfs.getTypeInfo();
        HiveVarchar hvc = new HiveVarchar(str, vti.getLength());
        return hvc;
      } else if (hfsType == HCatFieldSchema.Type.CHAR) {
        CharTypeInfo cti = (CharTypeInfo) hfs.getTypeInfo();
        HiveChar hc = new HiveChar(val.toString(), cti.getLength());
        return hc;
      }
    } else if (hfsType == HCatFieldSchema.Type.DECIMAL) {
      BigDecimal bd = new BigDecimal(val.toString(), MathContext.DECIMAL128);
      HiveDecimal hd = HiveDecimal.create(bd);
      return hd;
    }
    return null;
  }

  private Object convertBooleanTypes(Object val, HCatFieldSchema hfs) {
    HCatFieldSchema.Type hfsType = hfs.getType();
    Boolean b = (Boolean) val;
    if (hfsType == HCatFieldSchema.Type.BOOLEAN) {
      return b;
    } else if (hfsType == HCatFieldSchema.Type.TINYINT) {
      return (byte) (b ? 1 : 0);
    } else if (hfsType == HCatFieldSchema.Type.SMALLINT) {
      return (short) (b ? 1 : 0);
    } else if (hfsType == HCatFieldSchema.Type.INT) {
      return (int) (b ? 1 : 0);
    } else if (hfsType == HCatFieldSchema.Type.BIGINT) {
      return (long) (b ? 1 : 0);
    } else if (hfsType == HCatFieldSchema.Type.FLOAT) {
      return (float) (b ? 1 : 0);
    } else if (hfsType == HCatFieldSchema.Type.DOUBLE) {
      return (double) (b ? 1 : 0);
    } else if (hfsType == HCatFieldSchema.Type.STRING) {
      return val.toString();
    } else if (hfsType == HCatFieldSchema.Type.VARCHAR) {
      VarcharTypeInfo vti = (VarcharTypeInfo) hfs.getTypeInfo();
      HiveVarchar hvc = new HiveVarchar(val.toString(), vti.getLength());
      return hvc;
    } else if (hfsType == HCatFieldSchema.Type.CHAR) {
      CharTypeInfo cti = (CharTypeInfo) hfs.getTypeInfo();
      HiveChar hChar = new HiveChar(val.toString(), cti.getLength());
      return hChar;
    }
    return null;
  }

  private Object convertNumberTypes(Object val, HCatFieldSchema hfs) {
    HCatFieldSchema.Type hfsType = hfs.getType();

    if (!(val instanceof Number)) {
      return null;
    }
    if (val instanceof BigDecimal
        && hfsType == HCatFieldSchema.Type.STRING
        || hfsType == HCatFieldSchema.Type.VARCHAR
        || hfsType == HCatFieldSchema.Type.CHAR) {
      BigDecimal bd = (BigDecimal) val;
      String bdStr = null;
      if (bigDecimalFormatString) {
        bdStr = bd.toPlainString();
      } else {
        bdStr = bd.toString();
      }
      if (hfsType == HCatFieldSchema.Type.VARCHAR) {
        VarcharTypeInfo vti = (VarcharTypeInfo) hfs.getTypeInfo();
        HiveVarchar hvc = new HiveVarchar(bdStr, vti.getLength());
        return hvc;
      } else if (hfsType == HCatFieldSchema.Type.VARCHAR) {
        CharTypeInfo cti = (CharTypeInfo) hfs.getTypeInfo();
        HiveChar hChar = new HiveChar(bdStr, cti.getLength());
        return hChar;
      } else {
        return bdStr;
      }
    }
    Number n = (Number) val;
    if (hfsType == HCatFieldSchema.Type.TINYINT) {
      return n.byteValue();
    } else if (hfsType == HCatFieldSchema.Type.SMALLINT) {
      return n.shortValue();
    } else if (hfsType == HCatFieldSchema.Type.INT) {
      return n.intValue();
    } else if (hfsType == HCatFieldSchema.Type.BIGINT) {
      return n.longValue();
    } else if (hfsType == HCatFieldSchema.Type.FLOAT) {
      return n.floatValue();
    } else if (hfsType == HCatFieldSchema.Type.DOUBLE) {
      return n.doubleValue();
    } else if (hfsType == HCatFieldSchema.Type.BOOLEAN) {
      return n.byteValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
    } else if (hfsType == HCatFieldSchema.Type.STRING) {
      return n.toString();
    } else if (hfsType == HCatFieldSchema.Type.VARCHAR) {
      VarcharTypeInfo vti = (VarcharTypeInfo) hfs.getTypeInfo();
      HiveVarchar hvc = new HiveVarchar(val.toString(), vti.getLength());
      return hvc;
    } else if (hfsType == HCatFieldSchema.Type.CHAR) {
      CharTypeInfo cti = (CharTypeInfo) hfs.getTypeInfo();
      HiveChar hChar = new HiveChar(val.toString(), cti.getLength());
      return hChar;
    } else if (hfsType == HCatFieldSchema.Type.DECIMAL) {
      BigDecimal bd = new BigDecimal(n.doubleValue(),
        MathContext.DECIMAL128);
      return HiveDecimal.create(bd);
    }
    return null;
  }

  public void cleanup() throws IOException {
    if (null != lobLoader) {
      lobLoader.close();
    }
  }
}
