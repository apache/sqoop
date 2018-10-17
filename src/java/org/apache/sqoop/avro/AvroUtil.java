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
package org.apache.sqoop.avro;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.sqoop.config.ConfigurationConstants;
import org.apache.sqoop.config.ConfigurationHelper;
import org.apache.sqoop.lib.BlobRef;
import org.apache.sqoop.lib.ClobRef;
import org.apache.sqoop.orm.ClassWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * The service class provides methods for creating and converting Avro objects.
 */
public final class AvroUtil {

  public static final String DECIMAL = "decimal";

  public static boolean isDecimal(Schema.Field field) {
    return isDecimal(field.schema());
  }

  public static boolean isDecimal(Schema schema) {
    if (schema.getType().equals(Schema.Type.UNION)) {
      for (Schema type : schema.getTypes()) {
        if (isDecimal(type)) {
          return true;
        }
      }

      return false;
    } else {
      return DECIMAL.equals(schema.getProp(LogicalType.LOGICAL_TYPE_PROP));
    }
  }

  private static BigDecimal padBigDecimal(BigDecimal bd, Schema schema) {
    Schema schemaContainingScale = getDecimalSchema(schema);
    if(schemaContainingScale != null) {
      int scale = Integer.valueOf(schemaContainingScale.getObjectProp("scale").toString());
      if (bd.scale() != scale) {
        return bd.setScale(scale);
      }
    }
    return bd;
  }

  private static Schema getDecimalSchema(Schema schema) {
    if (schema.getType().equals(Schema.Type.UNION)) {
      for (Schema type : schema.getTypes()) {
        // search for decimal schema
        Schema schemaContainingScale = getDecimalSchema(type);
        if (schemaContainingScale != null) {
          return schemaContainingScale;
        }
      }
    } else {
      if(DECIMAL.equals(schema.getProp(LogicalType.LOGICAL_TYPE_PROP))) {
        return schema;
      }
    }
    return null;
  }

  /**
   * Convert a Sqoop's Java representation to Avro representation.
   */
  public static Object toAvro(Object o, Schema.Field field, boolean bigDecimalFormatString, boolean bigDecimalPaddingEnabled) {

    if (o instanceof BigDecimal) {
      if(bigDecimalPaddingEnabled) {
        o = padBigDecimal((BigDecimal) o, field.schema());
      }
      if (!isDecimal(field)) {
        if (bigDecimalFormatString) {
          // Returns a string representation of this without an exponent field.
          return ((BigDecimal) o).toPlainString();
        } else {
          return o.toString();
        }
      }
    } else if (o instanceof Date) {
      return ((Date) o).getTime();
    } else if (o instanceof Time) {
      return ((Time) o).getTime();
    } else if (o instanceof Timestamp) {
      return ((Timestamp) o).getTime();
    } else if (o instanceof BytesWritable) {
      BytesWritable bw = (BytesWritable) o;
      return ByteBuffer.wrap(bw.getBytes(), 0, bw.getLength());
    } else if (o instanceof BlobRef) {
      BlobRef br = (BlobRef) o;
      // If blob data is stored in an external .lob file, save the ref file
      // as Avro bytes. If materialized inline, save blob data as Avro bytes.
      byte[] bytes = br.isExternal() ? br.toString().getBytes() : br.getData();
      return ByteBuffer.wrap(bytes);
    } else if (o instanceof ClobRef) {
      throw new UnsupportedOperationException("ClobRef not supported");
    }
    // primitive types (Integer, etc) are left unchanged
    return o;
  }

  /**
   * Convert Column name into Avro column name.
   */
  public static String toAvroColumn(String column) {
    String candidate = ClassWriter.toJavaIdentifier(column);
    return toAvroIdentifier(candidate);
  }

  /**
   * Format candidate to avro specifics
   */
  public static String toAvroIdentifier(String candidate) {
    char[] data = candidate.toCharArray();
    boolean skip = false;
    int stringIndex = 0;

    for (char c:data) {
      if (Character.isLetterOrDigit(c) || c == '_') {
        data[stringIndex++] = c;
        skip = false;
      } else if(!skip) {
        data[stringIndex++] = '_';
        skip = true;
      }
    }

    char initial = data[0];
    if (Character.isLetter(initial) || initial == '_') {
      return new String(data, 0, stringIndex);
    } else {
      return "AVRO_".concat(new String(data, 0, stringIndex));
    }
  }

  public static GenericRecord toGenericRecord(Map<String, Object> fieldMap,
                                              Schema schema, boolean bigDecimalFormatString) {
    return toGenericRecord(fieldMap, schema, bigDecimalFormatString, false);
  }

  /**
   * Manipulate a GenericRecord instance.
   */
  public static GenericRecord toGenericRecord(Map<String, Object> fieldMap,
      Schema schema, boolean bigDecimalFormatString, boolean bigDecimalPaddingEnabled) {
    GenericRecord record = new GenericData.Record(schema);
    for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
      String avroColumn = toAvroColumn(entry.getKey());
      Schema.Field field = schema.getField(avroColumn);
      Object avroObject = toAvro(entry.getValue(), field, bigDecimalFormatString, bigDecimalPaddingEnabled);
      record.put(avroColumn, avroObject);
    }
    return record;
  }

  private static final String TIMESTAMP_TYPE = "java.sql.Timestamp";
  private static final String TIME_TYPE = "java.sql.Time";
  private static final String DATE_TYPE = "java.sql.Date";
  private static final String BIG_DECIMAL_TYPE = "java.math.BigDecimal";
  private static final String BLOB_REF_TYPE = "org.apache.sqoop.lib.BlobRef";

  /**
   * Convert from Avro type to Sqoop's java representation of the SQL type
   * see SqlManager#toJavaType
   */
  public static Object fromAvro(Object avroObject, Schema schema, String type) {
    if (avroObject == null) {
      return null;
    }

    switch (schema.getType()) {
      case NULL:
        return null;
      case BOOLEAN:
      case INT:
      case FLOAT:
      case DOUBLE:
        return avroObject;
      case LONG:
        if (type.equals(DATE_TYPE)) {
          return new Date((Long) avroObject);
        } else if (type.equals(TIME_TYPE)) {
          return new Time((Long) avroObject);
        } else if (type.equals(TIMESTAMP_TYPE)) {
          return new Timestamp((Long) avroObject);
        }
        return avroObject;
      case BYTES:
        ByteBuffer bb = (ByteBuffer) avroObject;
        BytesWritable bw = new BytesWritable();
        bw.set(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
        if (type.equals(BLOB_REF_TYPE)) {
          // TODO: Should convert BytesWritable to BlobRef properly. (SQOOP-991)
          throw new UnsupportedOperationException("BlobRef not supported");
        }
        return bw;
      case STRING:
        if (type.equals(BIG_DECIMAL_TYPE)) {
          return new BigDecimal(avroObject.toString());
        } else if (type.equals(DATE_TYPE)) {
          return Date.valueOf(avroObject.toString());
        } else if (type.equals(TIME_TYPE)) {
          return Time.valueOf(avroObject.toString());
        } else if (type.equals(TIMESTAMP_TYPE)) {
          return Timestamp.valueOf(avroObject.toString());
        }
        return avroObject.toString();
      case ENUM:
        return avroObject.toString();
      case UNION:
        List<Schema> types = schema.getTypes();
        if (types.size() != 2) {
          throw new IllegalArgumentException("Only support union with null");
        }
        Schema s1 = types.get(0);
        Schema s2 = types.get(1);
        if (s1.getType() == Schema.Type.NULL) {
          return fromAvro(avroObject, s2, type);
        } else if (s2.getType() == Schema.Type.NULL) {
          return fromAvro(avroObject, s1, type);
        } else {
          throw new IllegalArgumentException("Only support union with null");
        }
      case FIXED:
        if (isDecimal(schema)) {
          // Should automatically be a BigDecimal object.
          return avroObject;
        } else {
          return new BytesWritable(((GenericFixed) avroObject).bytes());
        }
      case RECORD:
      case ARRAY:
      case MAP:
      default:
        throw new IllegalArgumentException("Cannot convert Avro type "
            + schema.getType());
    }
  }

  /**
   * Get the schema of AVRO files stored in a directory
   */
  public static Schema getAvroSchema(Path path, Configuration conf)
      throws IOException {
    Path fileToTest = getFileToTest(path, conf);
    SeekableInput input = new FsInput(fileToTest, conf);
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    FileReader<GenericRecord> fileReader = DataFileReader.openReader(input, reader);

    Schema result = fileReader.getSchema();
    fileReader.close();
    return result;
  }

  /**
   * This method checks if the precision is an invalid value, i.e. smaller than 0 and
   * if so, tries to overwrite precision and scale with the configured defaults from
   * the configuration object. If a default precision is not defined, then throws an Exception.
   *
   * @param precision precision
   * @param scale scale
   * @param conf Configuration that contains the default values if the user specified them
   * @return an avro decimal type, that can be added as a column type in the avro schema generation
   */
  public static LogicalType createDecimalType(Integer precision, Integer scale, Configuration conf) {
    if (precision == null || precision <= 0) {
      // we check if the user configured default precision and scale and use these values instead of invalid ones.
      Integer configuredPrecision = ConfigurationHelper.getIntegerConfigIfExists(conf, ConfigurationConstants.PROP_AVRO_DECIMAL_PRECISION);
      if (configuredPrecision != null) {
        precision = configuredPrecision;
      } else {
        throw new RuntimeException("Invalid precision for Avro Schema. Please specify a default precision with the -D" +
            ConfigurationConstants.PROP_AVRO_DECIMAL_PRECISION + " flag to avoid this issue.");
      }
      Integer configuredScale = ConfigurationHelper.getIntegerConfigIfExists(conf, ConfigurationConstants.PROP_AVRO_DECIMAL_SCALE);
      if (configuredScale != null) {
        scale = configuredScale;
      }
    }

    return LogicalTypes.decimal(precision, scale);
  }
  private static Path getFileToTest(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (!fs.isDirectory(path)) {
      return path;
    }
    FileStatus[] fileStatuses = fs.listStatus(path, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
      }
    });
    if (fileStatuses.length == 0) {
      return null;
    }
    return fileStatuses[0].getPath();
  }

  public static Schema parseAvroSchema(String schemaString) {
    return new Schema.Parser().parse(schemaString);
  }

  public static Schema getAvroSchemaFromParquetFile(Path path, Configuration conf) throws IOException {
    Path fileToTest = getFileToTest(path, conf);
    if (fileToTest == null) {
      return null;
    }
    ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(conf, fileToTest, ParquetMetadataConverter.NO_FILTER);

    MessageType parquetSchema = parquetMetadata.getFileMetaData().getSchema();
    AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter();
    return avroSchemaConverter.convert(parquetSchema);
  }
}
