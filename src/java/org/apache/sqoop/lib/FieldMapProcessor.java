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
package org.apache.sqoop.lib;

import java.io.IOException;

import com.cloudera.sqoop.lib.FieldMappable;
import com.cloudera.sqoop.lib.ProcessingException;

/**
 * Interface implemented by classes that process FieldMappable objects.
 */
public interface FieldMapProcessor {

  /**
   * Allow arbitrary processing of a FieldMappable object.
   * @param record an object which can emit a map of its field names to values.
   * @throws IOException if the processor encounters an IO error when
   * operating on this object.
   * @throws ProcessingException if the FieldMapProcessor encounters
   * a general processing error when operating on this object.
   */
  void accept(FieldMappable record) throws IOException, ProcessingException;
}
