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
import org.apache.log4j.Logger;
import org.apache.sqoop.connector.common.SqoopIDFUtils;
import org.apache.sqoop.schema.Schema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

/**
 * A concrete implementation for the {@link #IntermediateDataFormat} that
 * represents each row of the data source as a comma separates list. Each
 * element in the CSV represents a specific column value encoded as string using
 * the sqoop specified rules. The methods allow serializing to this string and
 * deserializing the string to its corresponding java object based on the
 * {@link #Schema} and its {@link #Column} types.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class CSVIntermediateDataFormat extends IntermediateDataFormat<String> {

  public static final Logger LOG = Logger.getLogger(CSVIntermediateDataFormat.class);

  // need this default constructor for reflection magic used in execution engine
  public CSVIntermediateDataFormat() {
  }

  public CSVIntermediateDataFormat(Schema schema) {
    super.setSchema(schema);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getCSVTextData() {
    return super.getData();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setCSVTextData(String csvText) {
    super.setData(csvText);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object[] getObjectData() {
    super.validateSchema(schema);
    return SqoopIDFUtils.fromCSV(data, schema);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setObjectData(Object[] data) {
    super.validateSchema(schema);
    // convert object array to csv text
    this.data = toCSV(data);

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(this.data);
  }

  /**
   *
   * {@inheritDoc}
   */
  @Override
  public void read(DataInput in) throws IOException {
    data = in.readUTF();
  }

  /**
   * Encode to the sqoop prescribed CSV String for every element in the object
   * array
   *
   * @param objectArray
   */
  @SuppressWarnings("unchecked")
  private String toCSV(Object[] objectArray) {
    return SqoopIDFUtils.toCSV(objectArray, schema);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<String> getJars() {
    return super.getJars();
  }
}
