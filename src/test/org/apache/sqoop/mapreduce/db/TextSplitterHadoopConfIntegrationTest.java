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
package org.apache.sqoop.mapreduce.db;

import java.sql.ResultSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.sqoop.validation.ValidationException;

import com.cloudera.sqoop.testutil.MockResultSet;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertFalse;

public class TextSplitterHadoopConfIntegrationTest {
  private static final String TEXT_COL_NAME = "text_col_name";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testDefaultValueOfUnsetBooleanParam() throws Exception {
    Configuration conf = Job.getInstance().getConfiguration();
    TextSplitter splitter = new TextSplitter();
    ResultSet rs = new MockResultSet();

    String containedByExpectedExceptionMessage = TextSplitter.ALLOW_TEXT_SPLITTER_PROPERTY;

    thrown.expect(ValidationException.class);
    thrown.expectMessage(containedByExpectedExceptionMessage);
    splitter.split(conf, rs, TEXT_COL_NAME);
  }

  @Test
  public void testBooleanParamValue() throws Exception {
    Configuration conf = Job.getInstance().getConfiguration();
    conf.set(TextSplitter.ALLOW_TEXT_SPLITTER_PROPERTY, "true");
    TextSplitter splitter = new TextSplitter();
    ResultSet rs = new MockResultSet();
    List<InputSplit> splits = splitter.split(conf, rs, TEXT_COL_NAME);
    assertFalse(splits.isEmpty());
  }

}

