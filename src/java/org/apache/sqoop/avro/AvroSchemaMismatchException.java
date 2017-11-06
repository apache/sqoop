/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.avro;

import org.apache.avro.Schema;

/**
 * This exception will be thrown when Sqoop tries to write to a dataset
 * and the Avro schema which was used when the dataset was created does not match
 * the actual schema which is used by Sqoop during the write operation.
 */
public class AvroSchemaMismatchException extends RuntimeException {

  static final String MESSAGE_TEMPLATE = "%s%nExpected schema: %s%nActual schema: %s";

  private final Schema writtenWithSchema;

  private final Schema actualSchema;

  public AvroSchemaMismatchException(String message, Schema writtenWithSchema, Schema actualSchema) {
    super(String.format(MESSAGE_TEMPLATE, message, writtenWithSchema.toString(), actualSchema.toString()));
    this.writtenWithSchema = writtenWithSchema;
    this.actualSchema = actualSchema;
  }

  public Schema getWrittenWithSchema() {
    return writtenWithSchema;
  }

  public Schema getActualSchema() {
    return actualSchema;
  }

}
