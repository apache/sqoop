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

import org.apache.sqoop.common.ImmutableContext;
import org.apache.sqoop.etl.io.DataWriter;

/**
 * Context implementation for Extractor.
 *
 * This class is wrapping writer object.
 */
public class ExtractorContext extends ActorContext {

  private DataWriter writer;

  public ExtractorContext(ImmutableContext context, DataWriter writer) {
    super(context);
    this.writer = writer;
  }

  /**
   * Return associated data writer object.
   *
   * @return Data writer object for extract output
   */
  public DataWriter getDataWriter() {
    return writer;
  }
}
