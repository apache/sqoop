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

package com.cloudera.sqoop;

import org.apache.sqoop.TestExportUsingProcedure;

import com.cloudera.sqoop.hive.TestHiveImport;
import com.cloudera.sqoop.hive.TestTableDefWriter;
import com.cloudera.sqoop.io.TestLobFile;
import com.cloudera.sqoop.io.TestNamedFifo;
import com.cloudera.sqoop.io.TestSplittableBufferedWriter;
import com.cloudera.sqoop.lib.TestBooleanParser;
import com.cloudera.sqoop.lib.TestFieldFormatter;
import com.cloudera.sqoop.lib.TestRecordParser;
import com.cloudera.sqoop.lib.TestBlobRef;
import com.cloudera.sqoop.lib.TestClobRef;
import com.cloudera.sqoop.lib.TestLargeObjectLoader;
import com.cloudera.sqoop.manager.TestHsqldbManager;
import com.cloudera.sqoop.manager.TestSqlManager;
import com.cloudera.sqoop.mapreduce.MapreduceTests;
import com.cloudera.sqoop.metastore.TestSavedJobs;
import com.cloudera.sqoop.orm.TestClassWriter;
import com.cloudera.sqoop.orm.TestParseMethods;

import com.cloudera.sqoop.tool.TestToolPlugin;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Smoke tests for Sqoop (com.cloudera.sqoop).
 */
public final class SmokeTests {

  private SmokeTests() { }

  public static Test suite() {
    TestSuite suite = new TestSuite("Smoke tests for com.cloudera.sqoop");

    suite.addTestSuite(TestAllTables.class);
    suite.addTestSuite(TestHsqldbManager.class);
    suite.addTestSuite(TestSqlManager.class);
    suite.addTestSuite(TestClassWriter.class);
    suite.addTestSuite(TestColumnTypes.class);
    suite.addTestSuite(TestExport.class);
    suite.addTestSuite(TestMultiCols.class);
    suite.addTestSuite(TestMultiMaps.class);
    suite.addTestSuite(TestSplitBy.class);
    suite.addTestSuite(TestQuery.class);
    suite.addTestSuite(TestWhere.class);
    suite.addTestSuite(TestTargetDir.class);
    suite.addTestSuite(TestAppendUtils.class);
    suite.addTestSuite(TestHiveImport.class);
    suite.addTestSuite(TestRecordParser.class);
    suite.addTestSuite(TestFieldFormatter.class);
    suite.addTestSuite(TestSqoopOptions.class);
    suite.addTestSuite(TestParseMethods.class);
    suite.addTestSuite(TestConnFactory.class);
    suite.addTestSuite(TestSplittableBufferedWriter.class);
    suite.addTestSuite(TestTableDefWriter.class);
    suite.addTestSuite(TestBlobRef.class);
    suite.addTestSuite(TestClobRef.class);
    suite.addTestSuite(TestLargeObjectLoader.class);
    suite.addTestSuite(TestLobFile.class);
    suite.addTestSuite(TestExportUpdate.class);
    suite.addTestSuite(TestSavedJobs.class);
    suite.addTestSuite(TestNamedFifo.class);
    suite.addTestSuite(TestBooleanParser.class);
    suite.addTestSuite(TestMerge.class);
    suite.addTestSuite(TestToolPlugin.class);
    suite.addTestSuite(TestExportUsingProcedure.class);
    suite.addTest(MapreduceTests.suite());

    return suite;
  }

}

