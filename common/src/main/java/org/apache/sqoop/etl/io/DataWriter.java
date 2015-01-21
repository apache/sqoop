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
package org.apache.sqoop.etl.io;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 * An intermediate layer for passing data from the ETL framework
 * to the MR framework.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class DataWriter {

  /**
   * Write an array of objects into the execution framework
   * @param array - data to be written
   */
  public abstract void writeArrayRecord(Object[] array);

  /**
   * Write data into execution framework as text. The Intermediate Data Format
   * may choose to convert the data to another format based on how the data
   * format is implemented
   * @param text - data represented as CSV text.
   */
  public abstract void writeStringRecord(String text);

  /**
   * Write data in the intermediate data format's native format.
   * @param obj - data to be written
   */
  public abstract void writeRecord(Object obj);

}
