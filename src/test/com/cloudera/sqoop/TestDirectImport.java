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

package com.cloudera.sqoop;

import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;

public class TestDirectImport extends ImportJobTestCase {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  protected String[] getArgv(boolean includeHadoopFlags, String[] colNames, boolean isDirect) {
    String columnsString = "";
    for (String col : colNames) {
      columnsString += col + ",";
    }

    ArrayList<String> args = new ArrayList<String>();

    if (includeHadoopFlags) {
      CommonArgs.addHadoopFlags(args);
    }

    args.add("--table");
    args.add(HsqldbTestServer.getTableName());
    args.add("--columns");
    args.add(columnsString);
    if (isDirect) args.add("--direct");
    args.add("--split-by");
    args.add("INTFIELD1");
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());

    args.add("--delete-target-dir");

    return args.toArray(new String[0]);
  }

  @Test
  public void testDirectFlagWithHSQL() throws IOException {
    String[] columns = HsqldbTestServer.getFieldNames();

    String[] argv = getArgv(true, columns, true);
    exception.expect(IOException.class);
    runImport(argv);
  }

  @Test
  public void testNonDirectFlagWithHSQL() throws IOException {
    String[] columns = HsqldbTestServer.getFieldNames();

    String[] argv = getArgv(true, columns, false);
    runImport(argv);

  }

}
