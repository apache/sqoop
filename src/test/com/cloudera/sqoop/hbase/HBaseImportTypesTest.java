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

package com.cloudera.sqoop.hbase;

import java.io.IOException;

import org.junit.Test;

/**
 *
 */
public class HBaseImportTypesTest extends HBaseTestCase {

  @Test
  public void testStrings() throws IOException {
    String [] argv = getArgv(true, "stringT", "stringF", true, null);
    String [] types = { "INT", "VARCHAR(32)" };
    String [] vals = { "0", "'abc'" };
    createTableWithColTypes(types, vals);
    runImport(argv);
    verifyHBaseCell("stringT", "0", "stringF", getColName(1), "abc");
  }

}
