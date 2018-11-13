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

package org.apache.sqoop.testutil;

import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertArrayEquals;

@Category(UnitTest.class)
public class TestArgumentArrayBuilder {

  @Test
  public void testArgumentArrayBuilder() {
    String[] expectedArray = { "-D", "property=value3", "--option", "value2", "--", "--toolOption", "value1" };
    ArgumentArrayBuilder builder = new ArgumentArrayBuilder();
    builder.withToolOption("toolOption", "value1")
        .withOption("option", "value2")
        .withProperty("property", "value3");
    String[] argArray = builder.build();
    assertArrayEquals(expectedArray, argArray);
  }

  @Test
  public void testArgumentArrayBuilderOverriddenValues() {
    String[] expectedArray = { "-D", "property=modifiedProperty", "--option", "modifiedOption", "--", "--toolOption", "modifiedToolOption" };

    ArgumentArrayBuilder builder = new ArgumentArrayBuilder();
    builder.withToolOption("toolOption", "originalToolOption")
        .withOption("option", "originalOption")
        .withProperty("property", "originalProperty");
    builder.withProperty("property", "modifiedProperty")
        .withOption("option", "modifiedOption")
        .withToolOption("toolOption", "modifiedToolOption");
    String[] argArray = builder.build();

    assertArrayEquals(expectedArray, argArray);
  }

  @Test
  public void testArgumentArrayBuilderCopiesEverything() {
    String[] expectedArray = { "-D", "property=value3", "--option", "value2", "--", "--toolOption", "value1" };
    ArgumentArrayBuilder builder = new ArgumentArrayBuilder();
    builder.withToolOption("toolOption", "value1")
        .withOption("option", "value2")
        .withProperty("property", "value3");

    ArgumentArrayBuilder otherBuilder = new ArgumentArrayBuilder();
    otherBuilder.with(builder);
    String[] argArray = otherBuilder.build();
    assertArrayEquals(expectedArray, argArray);
  }

}
