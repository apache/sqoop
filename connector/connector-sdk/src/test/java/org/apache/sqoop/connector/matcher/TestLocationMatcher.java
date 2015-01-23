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
package org.apache.sqoop.connector.matcher;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.schema.NullSchema;
import org.apache.sqoop.schema.Schema;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class TestLocationMatcher {

  private LocationMatcher matcher;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    matcher = null;
  }

  /**
   * FROM and TO schemas are identical, fields should be copied directly.
   */
  @Test
  public void testPerfectMatch() {
    matcher = new LocationMatcher(
        SchemaFixture.createSchema1("from"),
        SchemaFixture.createSchema1("to"));
    Object[] fields = SchemaFixture.createNotNullRecordForSchema1();

    Object[] actual = matcher.getMatchingData(fields);
    assertArrayEquals(fields, actual);
  }

  /**
   * When no FROM schema is specified, fields should be copied directly.
   */
  @Test
  public void testDirectFieldsCopy() {
    matcher = new LocationMatcher(
        NullSchema.getInstance(),
        SchemaFixture.createSchema1("to"));
    Object[] fields = SchemaFixture.createNotNullRecordForSchema1();

    Object[] actual = matcher.getMatchingData(fields);
    assertArrayEquals(fields, actual);
  }

  /**
   * If TO schema has more fields than FROM schema, and all of the extra fields
   * are "nullable", their values will be set to null.
   */
  @Test
  public void testConvertWhenToSchemaIsLongerThanFromSchema() {
    matcher = new LocationMatcher(
        SchemaFixture.createSchema("from", 2),
        SchemaFixture.createSchema("to", 3));
    Object[] fields = new Object[] {"t1", "t2"};

    Object[] actual = matcher.getMatchingData(fields);
    assertArrayEquals(new Object[] {"t1", "t2", null}, actual);
  }

  /**
   * If TO schema has more fields than FROM schema, and NOT all of the extra
   * fields are "nullable", a SqoopException is expected.
   */
  @Test (expectedExceptions = SqoopException.class)
  public void testConvertWhenToSchemaIsLongerThanFromSchemaFail() {
    Schema from = SchemaFixture.createSchema("from", 2);
    Schema to = SchemaFixture.createSchema("to", 4);
    to.getColumnsList().get(2).setNullable(true);
    to.getColumnsList().get(3).setNullable(false);
    matcher = new LocationMatcher(from, to);
    Object[] fields = new Object[] {"t1", "t2"};

    matcher.getMatchingData(fields);
  }

}