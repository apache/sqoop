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

import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Column;
import org.apache.sqoop.schema.type.Type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class representing a pluggable intermediate data format the Sqoop
 * framework will use to move data to/from the connector. All intermediate
 * data formats are expected to have an internal/native implementation,
 * but also should minimally be able to return a text (CSV) version of the
 * data. The data format should also be able to return the data as an object
 * array - each array representing one row.
 * <p/>
 * Why a "native" internal format and then return text too?
 * Imagine a connector that moves data from a system that stores data as a
 * serialization format called FooFormat. If I also need the data to be
 * written into HDFS as FooFormat, the additional cycles burnt in converting
 * the FooFormat to text and back is useless - so plugging in an intermediate
 * format that can store the data as FooFormat saves those cycles!
 * <p/>
 * Most fast access mechanisms, like mysqldump or pgsqldump write the data
 * out as CSV, and most often the destination data is also represented as CSV
 * - so having a minimal CSV support is important, so we can easily pull the
 * data out as text.
 * <p/>
 * Any conversion to the final format from the native or text format is to be
 * done by the connector or OutputFormat classes.
 *
 * @param <T> - Each data format may have a native representation of the
 *            data, represented by the parameter.
 */
public abstract class IntermediateDataFormat<T> {

  protected volatile T data;

  public int hashCode() {
    return data.hashCode();
  }

  /**
   * Set one row of data. If validate is set to true, the data is validated
   * against the schema.
   *
   * @param data - A single row of data to be moved.
   */
  public void setData(T data) {
    this.data = data;
  }

  /**
   * Get one row of data.
   *
   * @return - One row of data, represented in the internal/native format of
   *         the intermediate data format implementation.
   */
  public T getData() {
    return data;
  }

  /**
   * Get one row of data as CSV.
   *
   * @return - String representing the data in CSV
   */
  public abstract String getTextData();

  /**
   * Set one row of data as CSV.
   *
   */
  public abstract void setTextData(String text);

  /**
   * Get one row of data as an Object array.
   *
   * @return - String representing the data as an Object array
   */
  public abstract Object[] getObjectData();

  /**
   * Set one row of data as an Object array.
   *
   */
  public abstract void setObjectData(Object[] data);

  /**
   * Set the schema to be used.
   *
   * @param schema - the schema to be used
   */
  public abstract void setSchema(Schema schema);

  /**
   * Get the schema of the data.
   *
   * @return - The schema of the data.
   */
  public abstract Schema getSchema();

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
}
