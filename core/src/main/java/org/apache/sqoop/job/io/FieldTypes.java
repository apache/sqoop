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
package org.apache.sqoop.job.io;

public final class FieldTypes {

  public static final int NULL    = 0;

  public static final int BOOLEAN = 1;

  public static final int BYTE    = 10;
  public static final int CHAR    = 11;

  public static final int SHORT   = 20;
  public static final int INT     = 21;
  public static final int LONG    = 22;

  public static final int FLOAT   = 50;
  public static final int DOUBLE  = 51;

  public static final int BIN     = 100;
  public static final int UTF     = 101;

  private FieldTypes() {
    // Disable explicit object creation
  }
}
