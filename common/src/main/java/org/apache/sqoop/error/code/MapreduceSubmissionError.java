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
package org.apache.sqoop.error.code;

import org.apache.sqoop.common.ErrorCode;

/**
 *
 */
public enum MapreduceSubmissionError implements ErrorCode {

  MAPREDUCE_0001("Unknown error"),

  MAPREDUCE_0002("Failure on submission engine initialization"),

  MAPREDUCE_0003("Can't get RunningJob instance"),

  MAPREDUCE_0004("Unknown map reduce job status"),

  MAPREDUCE_0005("Failure on submission engine destroy"),

  ;

  private final String message;

  private MapreduceSubmissionError(String message) {
    this.message = message;
  }

  public String getCode() {
    return name();
  }

  public String getMessage() {
    return message;
  }
}
