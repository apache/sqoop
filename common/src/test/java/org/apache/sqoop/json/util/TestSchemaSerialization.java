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
package org.apache.sqoop.json.util;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.json.JSONUtils;
import org.apache.sqoop.schema.ByteArraySchema;
import org.apache.sqoop.schema.NullSchema;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.schema.type.Array;
import org.apache.sqoop.schema.type.Binary;
import org.apache.sqoop.schema.type.Bit;
import org.apache.sqoop.schema.type.Date;
import org.apache.sqoop.schema.type.DateTime;
import org.apache.sqoop.schema.type.Decimal;
import org.apache.sqoop.schema.type.Enum;
import org.apache.sqoop.schema.type.FixedPoint;
import org.apache.sqoop.schema.type.FloatingPoint;
import org.apache.sqoop.schema.type.Map;
import org.apache.sqoop.schema.type.Set;
import org.apache.sqoop.schema.type.Text;
import org.apache.sqoop.schema.type.Time;
import org.apache.sqoop.schema.type.Unknown;
import org.json.simple.JSONObject;
import org.testng.annotations.Test;

/**
 *
 */
public class TestSchemaSerialization {

  @Test
  public void testSchemaNull() {
    // a null schema is treated as a NullSchema
    JSONObject extractJson = SchemaSerialization.extractSchema(null);
    JSONObject restoreJson = JSONUtils.parse(extractJson.toJSONString());
    assertEquals(NullSchema.getInstance(), SchemaSerialization.restoreSchema(restoreJson));
  }

  @Test
  public void testNullSchemaObject() {
    transferAndAssert(NullSchema.getInstance());
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SqoopException.class)
  public void testEmptySchemaName() {
    Schema schema = new Schema("");
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SqoopException.class)
  public void testNullSchemaName() {
    Schema schema = new Schema(null);
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SqoopException.class)
  public void testSchemaWithNullColumnName() {
    Schema schema = new Schema("test").addColumn(new Text(null));
  }

  @SuppressWarnings("unused")
  @Test(expectedExceptions = SqoopException.class)
  public void testSchemaWithEmptyColumnName() {
    Schema schema = new Schema("test").addColumn(new Text(""));
  }

  @Test
  public void testByteArraySchemaObject() {
    transferAndAssert(ByteArraySchema.getInstance());
  }

  @Test
  public void testArray() {
    // create an array type containing decimals
    Schema array = new Schema("array").addColumn(new Array("a", new Decimal("a1", 5, 2)).setSize(1L));
    transferAndAssert(array);
  }

  @Test
  public void testBinary() {
    Schema binary = new Schema("b").addColumn(new Binary("A", 100L));
    transferAndAssert(binary);
  }

  @Test
  public void testBit() {
    Schema bit = new Schema("b").addColumn(new Bit("B"));
    transferAndAssert(bit);
  }

  @Test
  public void testDate() {
    Schema date = new Schema("d").addColumn(new Date("d"));
    transferAndAssert(date);
  }

  @Test
  public void testDateTime() {
    Schema dateTime = new Schema("dt").addColumn(new DateTime("dt", Boolean.FALSE, Boolean.TRUE));
    transferAndAssert(dateTime);
  }

  @Test
  public void testDecimal() {
    Schema decimal = new Schema("d").addColumn(new Decimal("d", 5, 2));
    transferAndAssert(decimal);
  }

  @Test
  public void testEnum() {
    Schema e = new Schema("e").addColumn(new Enum("e", Collections
        .unmodifiableSet(new HashSet<String>(Arrays.asList(new String[] { "A", "B" })))));
    transferAndAssert(e);
  }

  @Test
  public void testFixedPoint() {
    Schema f = new Schema("f").addColumn(new FixedPoint("fp", 4L, Boolean.FALSE));
    transferAndAssert(f);
  }

  @Test
  public void testFloatingPoint() {
    Schema fp = new Schema("fp").addColumn(new FloatingPoint("k", 4L));
    transferAndAssert(fp);
  }

  @Test
  public void testMap() {
    Schema m = new Schema("m").addColumn(new Map("m", new Text("m1"), new Decimal("m2", 5, 2)));
    transferAndAssert(m);
  }

  @Test
  public void testSet() {
    Schema s = new Schema("s").addColumn(new Set("b", new Binary("b1")));
    transferAndAssert(s);
  }

  @Test
  public void testText() {
    Schema t = new Schema("t").addColumn(new Text("x", 10L));
    transferAndAssert(t);
  }

  @Test
  public void testTime() {
    Schema t = new Schema("t").addColumn(new Time("t", Boolean.FALSE));
    transferAndAssert(t);
  }

  @Test
  public void testUnknown() {
    Schema t = new Schema("t").addColumn(new Unknown("u", 4L));
    transferAndAssert(t);
  }
  @Test
  public void testNullable() {
    Schema nullable = new Schema("n").addColumn(new Text("x", Boolean.FALSE));
    transferAndAssert(nullable);
  }

  @Test
  public void testAllTypes() {
    Schema allTypes = new Schema("all-types")
      .addColumn(new Array("a", new Text("a1")))
      .addColumn(new Binary("b"))
      .addColumn(new Bit("c"))
      .addColumn(new Date("d"))
      .addColumn(new DateTime("e", true, true))
      .addColumn(new Decimal("f", 5, 2))
      .addColumn(new Enum("g", Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(new String[] { "X", "Y" })))))
      .addColumn(new FixedPoint("h", 2L, false))
      .addColumn(new FloatingPoint("i", 4L))
      .addColumn(new Map("j", new Text("j1"), new Text("j2")))
      .addColumn(new Set("k", new Text("k1")))
      .addColumn(new Text("l"))
      .addColumn(new Time("m", true))
      .addColumn(new Unknown("u"))
    ;
    transferAndAssert(allTypes);
  }

  @Test
  public void testComplex() {
    Schema complex = new Schema("complex")
      .addColumn(new Map("a", new Text("a1"), new Set("a2", new Array("a3", new Text("a4")))));
    transferAndAssert(complex);
  }

  private void transferAndAssert(Schema schema) {
    Schema transferred = transfer(schema);
    assertEquals(schema, transferred);
  }

  protected Schema transfer(Schema schema) {
    JSONObject extractJson = SchemaSerialization.extractSchema(schema);
    String transferredString = extractJson.toJSONString();
    JSONObject restoreJson = JSONUtils.parse(transferredString);
    return SchemaSerialization.restoreSchema(restoreJson);
  }
}
