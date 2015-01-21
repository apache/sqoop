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
package org.apache.sqoop.job.etl;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;
import org.apache.sqoop.common.ImmutableContext;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.schema.Schema;

/**
 * Context implementation for Extractor.
 *
 * This class is wrapping writer object.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ExtractorContext extends TransferableContext {

  private final DataWriter writer;

  private final Schema schema;

  public ExtractorContext(ImmutableContext context, DataWriter writer, Schema schema) {
    super(context);
    this.writer = writer;
    this.schema = schema;
  }

  /**
   * Return associated data writer object.
   *
   * @return Data writer object for extract output
   */
  public DataWriter getDataWriter() {
    return writer;
  }
  /**
   * Return schema associated with FROM.
   *
   * @return
   */
  public Schema getSchema() {
    return schema;
  }

}
