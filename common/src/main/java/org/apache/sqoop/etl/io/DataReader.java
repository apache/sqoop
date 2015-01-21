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
 * An intermediate layer for passing data from the execution engine
 * to the ETL engine.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class DataReader {

  /**
   * Read data from the execution engine as an object array.
   * @return - array of objects with each column represented as an object
   * @throws Exception
   */
  public abstract Object[] readArrayRecord() throws Exception;

  /**
   * Read data from execution engine as text - as a CSV record.
   * public abstract Object readContent(int type) throws Exception;
   * @return - CSV formatted data.
   * @throws Exception
   */
  public abstract String readTextRecord() throws Exception;

  /**
   * Read data from execution engine as a native format.
   * @return - the content in the native format of the intermediate data
   * format being used.
   * @throws Exception
   */
  public abstract Object readContent() throws Exception;

}
