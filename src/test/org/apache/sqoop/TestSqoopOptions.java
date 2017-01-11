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

package org.apache.sqoop;

import java.util.Properties;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestSqoopOptions {

  @Test
  public void testParseColumnParsing() {
    new SqoopOptions() {
  @Test
      public void testParseColumnMapping() {
        Properties result = new Properties();
        parseColumnMapping("test=INTEGER,test1=DECIMAL(1%2C1),test2=NUMERIC(1%2C%202)", result);
        assertEquals("INTEGER", result.getProperty("test"));
        assertEquals("DECIMAL(1,1)", result.getProperty("test1"));
        assertEquals("NUMERIC(1, 2)", result.getProperty("test2"));
      }
    }.testParseColumnMapping();
  }

  @Test
  public void testColumnNameCaseInsensitive() {
    SqoopOptions opts = new SqoopOptions();
    opts.setColumns(new String[]{ "AAA", "bbb" });
    assertEquals("AAA", opts.getColumnNameCaseInsensitive("aAa"));
    assertEquals("bbb", opts.getColumnNameCaseInsensitive("BbB"));
    assertEquals(null, opts.getColumnNameCaseInsensitive("notFound"));
    opts.setColumns(null);
    assertEquals(null, opts.getColumnNameCaseInsensitive("noColumns"));
  }
}
