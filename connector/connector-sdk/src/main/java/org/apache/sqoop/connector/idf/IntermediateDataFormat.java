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

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.IntermediateDataFormatError;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.utils.ClassUtils;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.json.simple.JSONValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Abstract class representing a pluggable intermediate data format Sqoop
 * will use to move data between the FROM and TO connectors. All intermediate
 * data formats are expected to have an internal/native implementation,
 * but also should minimally be able to return CSV text version as specified by
 * Sqoop spec. The data format in addition should also be able to return the data
 * as an object array as represented by the object model - each array represents one row.
 * <p/>
 * Any conversion to the format dictated by the corresponding data source from the native or  CSV text format
 * has to be done by the connector themselves both in FROM and TO
 *
 * NOTE: we cannot use the generic for comparable, since the comparison can be arbitrary for instance,
 * purely based on text format
 * @param <T> - Each data format may have a native representation of the
 *            data, represented by the parameter.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
@SuppressWarnings("rawtypes")
public abstract class IntermediateDataFormat<T> implements Comparable {

  protected volatile T data;

  protected Schema schema;

  /**
   * Get one row of data.
   *
   * @return - One row of data, represented in the internal/native format of
   *         the intermediate data format implementation.
   */
  public T getData() {
    validateSchema(schema);
    return data;
  }

  /**
   * Set one row of data. If validate is set to true, the data is validated
   * against the schema.
   *
   * @param obj - A single row of data to be moved.
   */
  public void setData(T obj) {
    validateSchema(schema);
    this.data = obj;
  }

  /**
   * Get one row of data as CSV text. Use {@link #SqoopIDFUtils} for reading and writing
   * into the sqoop specified CSV text format for each {@link #ColumnType} field in the row
   * Why a "native" internal format and then return CSV text too?
   * Imagine a connector that moves data from a system that stores data as a
   * serialization format called FooFormat. If I also need the data to be
   * written into HDFS as FooFormat, the additional cycles burnt in converting
   * the FooFormat to text and back is useless - so using the sqoop specified
   * CSV text format saves those extra cycles
   * <p/>
   * Most fast access mechanisms, like mysqldump or pgsqldump write the data
   * out as CSV, and most often the source data is also represented as CSV
   * - so having a minimal CSV support is mandated for all IDF, so we can easily read the
   * data out as text and write as text.
   * <p/>
   * @return - String representing the data in CSV text format.
   */
  public abstract String getCSVTextData();

  /**
   * Set one row of data as CSV.
   */
  public abstract void setCSVTextData(String csvText);

  /**
   * Get one row of data as an Object array.
   * Sqoop uses defined object representation
   * for each column type. For instance org.joda.time to represent date.
   * Use {@link #SqoopIDFUtils} for reading and writing into the sqoop
   * specified object format for each {@link #ColumnType} field in the row
   * </p>
   * @return - String representing the data as an Object array
   * If FROM and TO schema exist, we will use SchemaMatcher to get the data according to "TO" schema
   */
  public abstract Object[] getObjectData();

 /**
  * Set one row of data as an Object array.
  * It also should construct the data representation
  * that the IDF represents so that the object is ready to
  * consume when getData is invoked. Custom implementations
  * will override this method to convert form object array
  * to the data format
  */
  public abstract void setObjectData(Object[] data);

  /**
   * Set the schema for serializing/de-serializing data.
   *
   * @param schema
   *          - the schema used for serializing/de-serializing data
   */
  public void setSchema(Schema schema) {
    validateSchema(schema);
    this.schema = schema;
  }

  protected void validateSchema(Schema schema) {
    if (schema == null) {
      throw new SqoopException(IntermediateDataFormatError.INTERMEDIATE_DATA_FORMAT_0002);
    }
  }

  /**
   * Serialize the fields of this object to <code>out</code>.
   *
   * @param out <code>DataOuput</code> to serialize this object into.
   * @throws IOException
   */
  public abstract void write(DataOutput out) throws IOException;

  /**
   * Deserialize the fields of this object from <code>in</code>.
   *
   * <p>For efficiency, implementations should attempt to re-use storage in the
   * existing object where possible.</p>
   *
   * @param in <code>DataInput</code> to deseriablize this object from.
   * @throws IOException
   */
  public abstract void read(DataInput in) throws IOException;
  /**
   * Provide the external jars that the IDF depends on
   * @return set of jars
   */
  public Set<String> getJars() {
    Set<String> jars = new  HashSet<String>();
    // Add JODA classes for IDF date/time handling
    jars.add(ClassUtils.jarForClass(LocalDate.class));
    jars.add(ClassUtils.jarForClass(LocalDateTime.class));
    jars.add(ClassUtils.jarForClass(DateTime.class));
    jars.add(ClassUtils.jarForClass(LocalTime.class));
    // Add JSON parsing jar
    jars.add(ClassUtils.jarForClass(JSONValue.class));
    return jars;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((data == null) ? 0 : data.hashCode());
    result = prime * result + ((schema == null) ? 0 : schema.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    IntermediateDataFormat<?> other = (IntermediateDataFormat<?>) obj;
    if (data == null) {
      if (other.data != null)
        return false;
    } else if (!data.equals(other.data))
      return false;
    if (schema == null) {
      if (other.schema != null)
        return false;
    } else if (!schema.equals(other.schema))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return this.data.toString();
  }

  @Override
  public int compareTo(Object o) {
    IntermediateDataFormat<?> idf = (IntermediateDataFormat<?>) o;
    return toString().compareTo(idf.toString());
  }

}