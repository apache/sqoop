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

package org.apache.sqoop.shell.utils;

import static org.apache.sqoop.shell.utils.ConfigFiller.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.ResourceBundle;

import jline.console.ConsoleReader;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.model.InputEditable;
import org.apache.sqoop.model.MBooleanInput;
import org.apache.sqoop.model.MDateTimeInput;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MListInput;
import org.apache.sqoop.model.MLongInput;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.shell.ShellEnvironment;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestConfigFiller {
  CommandLine line;
  ConsoleReader reader;
  ResourceBundle resourceBundle;
  ByteArrayInputStream in;
  byte[] data;

  @BeforeTest(alwaysRun = true)
  public void setup() throws IOException {
    Groovysh shell = new Groovysh();
    ShellEnvironment.setIo(shell.getIo());
    line = mock(CommandLine.class);
    data = new byte[1000];
    in = new ByteArrayInputStream(data);
    reader = new ConsoleReader(in, System.out);
    resourceBundle = new ResourceBundle() {
      @Override
      protected Object handleGetObject(String key) {
        return "fake_translated_value";
      }

      @Override
      public Enumeration<String> getKeys() {
        return Collections.emptyEnumeration();
      }
    };
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputString() throws IOException {
    MStringInput input = new MStringInput("String", false, InputEditable.ANY, StringUtils.EMPTY, (short)30, Collections.EMPTY_LIST);
    when(line.hasOption("prefix-String")).thenReturn(false);
    assertTrue(fillInputString("prefix", input, line));
    assertNull(input.getValue());

    input.setEmpty();
    when(line.hasOption("prefix-String")).thenReturn(true);
    when(line.getOptionValue("prefix-String")).thenReturn("abc");
    assertTrue(fillInputString("prefix", input, line));
    assertEquals(input.getValue(), "abc");

    input.setEmpty();
    String lengthExceeds30 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
    when(line.getOptionValue("prefix-String")).thenReturn(lengthExceeds30);
    assertFalse(fillInputString("prefix", input, line));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputStringWithBundle() throws IOException {
    initEnv();
    // End of the process
    MStringInput input = new MStringInput("String", false, InputEditable.ANY, StringUtils.EMPTY, (short)30, Collections.EMPTY_LIST);
    assertFalse(fillInputStringWithBundle(input, reader, resourceBundle));

    // Empty input
    initData("\r");
    input.setEmpty();
    assertTrue(fillInputStringWithBundle(input, reader, resourceBundle));
    assertNull(input.getValue());

    // Normal input
    initData("abc\r");
    input.setEmpty();
    assertTrue(fillInputStringWithBundle(input, reader, resourceBundle));
    assertEquals(input.getValue(), "abc");

    // Retry when the given input exceeds maximal allowance
    String lengthExceeds30 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
    String remove30characters = "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b";
    String lengthBelowLimit = "abcdefg";
    initData(lengthExceeds30 + "\r" + remove30characters + lengthBelowLimit + "\r");
    input.setEmpty();
    assertTrue(fillInputStringWithBundle(input, reader, resourceBundle));
    assertEquals(input.getValue(), lengthBelowLimit);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputInteger() throws IOException {
    MIntegerInput input = new MIntegerInput("Integer", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    when(line.hasOption("prefix-Integer")).thenReturn(false);
    assertTrue(fillInputInteger("prefix", input, line));
    assertNull(input.getValue());

    // Valid integer number
    input.setEmpty();
    when(line.hasOption("prefix-Integer")).thenReturn(true);
    when(line.getOptionValue("prefix-Integer")).thenReturn("12345");
    assertTrue(fillInputInteger("prefix", input, line));
    assertEquals(input.getValue().intValue(), 12345);

    // Invalid integer number
    input.setEmpty();
    when(line.hasOption("prefix-Integer")).thenReturn(true);
    when(line.getOptionValue("prefix-Integer")).thenReturn("abc");
    assertFalse(fillInputInteger("prefix", input, line));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputIntegerWithBundle() throws IOException {
    initEnv();
    // End of the process
    MIntegerInput input = new MIntegerInput("Integer", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    assertFalse(fillInputIntegerWithBundle(input, reader, resourceBundle));

    // Empty input
    initData("\r");
    input.setEmpty();
    assertTrue(fillInputIntegerWithBundle(input, reader, resourceBundle));
    assertNull(input.getValue());

    // Normal input
    initData("12345\r");
    input.setEmpty();
    assertTrue(fillInputIntegerWithBundle(input, reader, resourceBundle));
    assertEquals(input.getValue().intValue(), 12345);

    // Retry when the given input is not a valid integer number
    initData("abc\r12345\r");
    input.setEmpty();
    assertTrue(fillInputIntegerWithBundle(input, reader, resourceBundle));
    assertEquals(input.getValue().intValue(), 12345);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputLong() throws IOException {
    MLongInput input = new MLongInput("Long", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    when(line.hasOption("prefix-Long")).thenReturn(false);
    assertTrue(fillInputLong("prefix", input, line));
    assertNull(input.getValue());

    // Valid long number
    input.setEmpty();
    when(line.hasOption("prefix-Long")).thenReturn(true);
    when(line.getOptionValue("prefix-Long")).thenReturn("12345");
    assertTrue(fillInputLong("prefix", input, line));
    assertEquals(input.getValue().longValue(), 12345);

    // Invalid long number
    input.setEmpty();
    when(line.hasOption("prefix-Long")).thenReturn(true);
    when(line.getOptionValue("prefix-Long")).thenReturn("abc");
    assertFalse(fillInputLong("prefix", input, line));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputLongWithBundle() throws IOException {
    initEnv();
    // End of the process
    MLongInput input = new MLongInput("Long", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    assertFalse(fillInputLongWithBundle(input, reader, resourceBundle));

    // Empty input
    initData("\r");
    input.setEmpty();
    assertTrue(fillInputLongWithBundle(input, reader, resourceBundle));
    assertNull(input.getValue());

    // Normal input
    initData("12345\r");
    input.setEmpty();
    assertTrue(fillInputLongWithBundle(input, reader, resourceBundle));
    assertEquals(input.getValue().intValue(), 12345);

    // Retry when the given input is not a valid long number
    initData("abc\r12345\r");
    input.setEmpty();
    assertTrue(fillInputLongWithBundle(input, reader, resourceBundle));
    assertEquals(input.getValue().intValue(), 12345);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputBoolean() throws IOException {
    MBooleanInput input = new MBooleanInput("Boolean", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    when(line.hasOption("prefix-Boolean")).thenReturn(false);
    assertTrue(fillInputBoolean("prefix", input, line));
    assertNull(input.getValue());

    // true
    input.setEmpty();
    when(line.hasOption("prefix-Boolean")).thenReturn(true);
    when(line.getOptionValue("prefix-Boolean")).thenReturn("true");
    assertTrue(fillInputBoolean("prefix", input, line));
    assertTrue(input.getValue());

    // false
    input.setEmpty();
    when(line.hasOption("prefix-Boolean")).thenReturn(true);
    when(line.getOptionValue("prefix-Boolean")).thenReturn("false");
    assertTrue(fillInputBoolean("prefix", input, line));
    assertFalse(input.getValue());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputBooleanWithBundle() throws IOException {
    initEnv();
    // End of the process
    MBooleanInput input = new MBooleanInput("Boolean", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    assertFalse(fillInputBooleanWithBundle(input, reader, resourceBundle));

    // true
    initData("true\r");
    input.setEmpty();
    assertTrue(fillInputBooleanWithBundle(input, reader, resourceBundle));
    assertTrue(input.getValue());

    // false
    initData("false\r");
    input.setEmpty();
    assertTrue(fillInputBooleanWithBundle(input, reader, resourceBundle));
    assertFalse(input.getValue());

    // Retry when the given input is not a valid boolean number
    initData("abc\rfalse\r");
    input.setEmpty();
    assertTrue(fillInputBooleanWithBundle(input, reader, resourceBundle));
    assertFalse(input.getValue());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputMap() throws IOException {
    MMapInput input = new MMapInput("Map", false, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    when(line.hasOption("prefix-Map")).thenReturn(false);
    assertTrue(fillInputMap("prefix", input, line));

    // Normal input
    input.setEmpty();
    when(line.hasOption("prefix-Map")).thenReturn(true);
    when(line.getOptionValue("prefix-Map")).thenReturn("k1=v1&k2=v2");
    assertTrue(fillInputMap("prefix", input, line));
    HashMap<String, String> map = new HashMap<String, String>();
    map.put("k1", "v1");
    map.put("k2", "v2");
    assertEquals(input.getValue(), map);

    // Invalid input
    input.setEmpty();
    when(line.hasOption("prefix-Map")).thenReturn(true);
    when(line.getOptionValue("prefix-Map")).thenReturn("k1=v1&k2");
    assertFalse(fillInputMap("prefix", input, line));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputMapWithBundle() throws IOException {
    initEnv();
    // End of the process
    MMapInput input = new MMapInput("Map", false, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    assertFalse(fillInputMapWithBundle(input, reader, resourceBundle));

    // empty
    initData("\r");
    input.setEmpty();
    assertTrue(fillInputMapWithBundle(input, reader, resourceBundle));

    // Add k1=v1 and k2=v2
    initData("k1=v1\rk2=v2\r\r");
    input.setEmpty();
    assertTrue(fillInputMapWithBundle(input, reader, resourceBundle));
    HashMap<String, String> map = new HashMap<String, String>();
    map.put("k1", "v1");
    map.put("k2", "v2");
    assertEquals(input.getValue(), map);

    // Remove k2
    initData("k2\r\r");
    assertTrue(fillInputMapWithBundle(input, reader, resourceBundle));
    map.remove("k2");
    assertEquals(input.getValue(), map);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputEnum() throws IOException {
    MEnumInput input = new MEnumInput("Enum", false, InputEditable.ANY, StringUtils.EMPTY, new String[] {"YES", "NO"}, Collections.EMPTY_LIST);
    when(line.hasOption("prefix-Enum")).thenReturn(false);
    assertTrue(fillInputEnum("prefix", input, line));
    assertNull(input.getValue());

    // Normal input
    input.setEmpty();
    when(line.hasOption("prefix-Enum")).thenReturn(true);
    when(line.getOptionValue("prefix-Enum")).thenReturn("YES");
    assertTrue(fillInputEnum("prefix", input, line));
    assertEquals(input.getValue(), "YES");

    // Invalid input
    input.setEmpty();
    when(line.hasOption("prefix-Enum")).thenReturn(true);
    when(line.getOptionValue("prefix-Enum")).thenReturn("NONEXISTVALUE");
    assertFalse(fillInputEnum("prefix", input, line));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputEnumWithBundle() throws IOException {
    initEnv();
    // End of the process
    MEnumInput input = new MEnumInput("Enum", false, InputEditable.ANY, StringUtils.EMPTY, new String[] {"YES", "NO"}, Collections.EMPTY_LIST);
    assertFalse(fillInputEnumWithBundle(input, reader, resourceBundle));

    // empty
    initData("\r");
    input.setEmpty();
    assertTrue(fillInputEnumWithBundle(input, reader, resourceBundle));

    // YES
    initData("0\r");
    input.setEmpty();
    assertTrue(fillInputEnumWithBundle(input, reader, resourceBundle));
    assertEquals(input.getValue(), "YES");

    // Retry when the given input is not a valid boolean number
    initData("a\r1\r");
    input.setEmpty();
    assertTrue(fillInputEnumWithBundle(input, reader, resourceBundle));
    assertEquals(input.getValue(), "NO");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputList() throws IOException {
    MListInput input = new MListInput("List", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    when(line.hasOption("prefix-List")).thenReturn(false);
    assertTrue(fillInputList("prefix", input, line));
    assertNull(input.getValue());

    // Normal input
    input.setEmpty();
    when(line.hasOption("prefix-List")).thenReturn(true);
    when(line.getOptionValue("prefix-List")).thenReturn("l1&l2&l3");
    assertTrue(fillInputList("prefix", input, line));
    assertEquals(StringUtils.join(input.getValue(), "&"), "l1&l2&l3");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputListWithBundle() throws IOException {
    initEnv();
    // End of the process
    MListInput input = new MListInput("List", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    assertFalse(fillInputListWithBundle(input, reader, resourceBundle));

    // empty
    initData("\r");
    input.setEmpty();
    assertTrue(fillInputListWithBundle(input, reader, resourceBundle));

    // Normal input
    initData("l1\rl2\rl3\r\r");
    input.setEmpty();
    assertTrue(fillInputListWithBundle(input, reader, resourceBundle));
    assertEquals(StringUtils.join(input.getValue(), "&"), "l1&l2&l3");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputDateTime() throws IOException {
    MDateTimeInput input = new MDateTimeInput("DateTime", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    when(line.hasOption("prefix-DateTime")).thenReturn(false);
    assertTrue(fillInputDateTime("prefix", input, line));
    assertNull(input.getValue());

    // Normal input
    input.setEmpty();
    when(line.hasOption("prefix-DateTime")).thenReturn(true);
    when(line.getOptionValue("prefix-DateTime")).thenReturn("123456789");
    assertTrue(fillInputDateTime("prefix", input, line));
    assertEquals(input.getValue().getMillis(), 123456789);

    // Invalid input
    input.setEmpty();
    when(line.hasOption("prefix-DateTime")).thenReturn(true);
    when(line.getOptionValue("prefix-DateTime")).thenReturn("abcd");
    assertFalse(fillInputDateTime("prefix", input, line));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFillInputDateTimeWithBundle() throws IOException {
    initEnv();
    // End of the process
    MDateTimeInput input = new MDateTimeInput("DateTime", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST);
    assertFalse(fillInputDateTimeWithBundle(input, reader, resourceBundle));

    // empty
    initData("\r");
    input.setEmpty();
    assertTrue(fillInputDateTimeWithBundle(input, reader, resourceBundle));

    // Normal input
    initData("123456789\r");
    input.setEmpty();
    assertTrue(fillInputDateTimeWithBundle(input, reader, resourceBundle));
    assertEquals(input.getValue().getMillis(), 123456789);

    // Retry when the given input is not valid datetime
    initData("abc\r123456789\r");
    input.setEmpty();
    assertTrue(fillInputDateTimeWithBundle(input, reader, resourceBundle));
    assertEquals(input.getValue().getMillis(), 123456789);
  }

  private void initData(String destData) {
    byte[] destDataBytes = destData.getBytes();
    System.arraycopy(destDataBytes, 0, data, 0, destDataBytes.length);
    in.reset();
  }

  private void initEnv() {
    in.reset();
    for (int i = 0; i < data.length; i++) {
      data[i] = '\0';
    }
  }
}
