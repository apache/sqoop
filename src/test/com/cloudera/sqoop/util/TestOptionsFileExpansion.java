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
package com.cloudera.sqoop.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import junit.framework.TestCase;

import org.junit.Assert;

import com.cloudera.sqoop.Sqoop;
/**
 * Tests various options file loading scenarios.
 */
public class TestOptionsFileExpansion extends TestCase {

  /**
   * Text from options file 1. Each string represents a new line.
   */
  private static final String[] OPTIONS_FILE_TEXT1 = new String[] {
    "--foo",
    "-bar",
    "--",
    "--XYZ",
  };

  /**
   * Expected options parsed out from options file 1.
   */
  private static final String[] OPTIONS_FILE_TEXT1_OUTPUT = new String[] {
    "--foo",
    "-bar",
    "--",
    "--XYZ",
  };

  /**
   * Text for options file 2. Each string represents a new line. This
   * contains empty lines, comments and optinos that extend to multiple lines.
   */
  private static final String[] OPTIONS_FILE_TEXT2 = new String[] {
    "--archives",
    "tools.jar,archive.jar,test.jar,\\",
    "ldap.jar,sasl.jar",
    "--connect",
    "jdbc:jdbcspy:localhost:1521:test",
    "--username",
    "superman",
    "--password",
    "",
    "# Ironic password.",
    "# No one will ever guess.",
    "kryptonite",
  };

  /**
   * Expected options parsed out from file 2.
   */
  private static final String[] OPTIONS_FILE_TEXT2_OUTPUT = new String[] {
    "--archives",
    "tools.jar,archive.jar,test.jar,ldap.jar,sasl.jar",
    "--connect",
    "jdbc:jdbcspy:localhost:1521:test",
    "--username",
    "superman",
    "--password",
    "kryptonite",
  };

  /**
   * Text for options file 4. This contains options that represent empty
   * strings or strings that have leading and trailing spaces.
   */
  private static final String[] OPTIONS_FILE_TEXT3 = new String[] {
    "-",
    "\"     leading spaces\"",
    "'        leading and trailing spaces    '",
    "\"\"",
    "''",
  };

  /**
   * Expected options parsed out from file 3.
   */
  private static final String[] OPTIONS_FILE_TEXT3_OUTPUT = new String[] {
    "-",
    "     leading spaces",
    "        leading and trailing spaces    ",
    "",
    "",
  };

  /**
   * Text for options file 4. This file has an invalid entry in the last line
   * which will cause it to fail to load.
   */
  private static final String[] OPTIONS_FILE_TEXT4 = new String[] {
    "--abcd",
    "--efgh",
    "# foo",
    "# bar",
    "XYZ\\",
  };

  /**
   * Text for options file 5. This file has an invalid entry in the second line
   * where there is a starting single quote character that is not terminating.
   */
  private static final String[] OPTIONS_FILE_TEXT5 = new String[] {
    "-abcd",
    "\'",
    "--foo",
  };

  /**
   * Text for options file 6. This file has an invalid entry in the second line
   * where a quoted string extends into the following line.
   */
  private static final String[] OPTIONS_FILE_TEXT6 = new String[] {
    "--abcd",
    "' the quick brown fox \\",
    "jumped over the lazy dog'",
    "--efgh",
  };

  public void testOptionsFiles() throws Exception {
    checkOptionsFile(OPTIONS_FILE_TEXT1, OPTIONS_FILE_TEXT1_OUTPUT);
    checkOptionsFile(OPTIONS_FILE_TEXT2, OPTIONS_FILE_TEXT2_OUTPUT);
    checkOptionsFile(OPTIONS_FILE_TEXT3, OPTIONS_FILE_TEXT3_OUTPUT);
  }

  public void testInvalidOptionsFile() {
    checkInvalidOptionsFile(OPTIONS_FILE_TEXT4);
    checkInvalidOptionsFile(OPTIONS_FILE_TEXT5);
  }

  public void testMultilineQuotedText() {
    try {
      checkOptionsFile(OPTIONS_FILE_TEXT6, new String[] {});
      Assert.assertTrue(false);
    } catch (Exception ex) {
      Assert.assertTrue(
          ex.getMessage().startsWith("Multiline quoted strings not supported"));
    }
  }

  private void checkInvalidOptionsFile(String[] fileContents) {
    try {
      checkOptionsFile(fileContents, new String[] {});
      Assert.assertTrue(false);
    } catch (Exception ex) {
      Assert.assertTrue(ex.getMessage().startsWith("Malformed option"));
    }
  }

  private void checkOptionsFile(String[] fileContent, String[] expectedOptions)
    throws Exception {
    String[] prefix0 = new String[] { };
    String[] suffix0 = new String[] { };

    checkOutput(prefix0, suffix0, fileContent, expectedOptions);

    String[] prefix1 = new String[] { "--nomnom" };
    String[] suffix1 = new String[] { };

    checkOutput(prefix1, suffix1,
        fileContent, expectedOptions);

    String[] prefix2 = new String[] { };
    String[] suffix2 = new String[] { "yIkes" };

    checkOutput(prefix2, suffix2,
        fileContent, expectedOptions);

    String[] prefix3 = new String[] { "foo", "bar" };
    String[] suffix3 = new String[] { "xyz", "abc" };

    checkOutput(prefix3, suffix3,
        fileContent, expectedOptions);
  }


  /**
   * Uses the given prefix and suffix to create the original args array which
   * contains two entries between the prefix and suffix entries that specify
   * the options file. The options file is dynamically created using the
   * contents of the third array - fileContent. Once this is expanded, the
   * expanded arguments are compared to see if they are same as prefix entries
   * followed by parsed arguments from the options file, followed by suffix
   * entries.
   * @param prefix
   * @param suffix
   * @param fileContent
   * @param expectedContent
   * @throws Exception
   */
  private void checkOutput(String[] prefix, String[] suffix,
      String[] fileContent, String[] expectedContent) throws Exception {

    String[] args = new String[prefix.length + 2 + suffix.length];

    for (int i = 0; i < prefix.length; i++) {
      args[i] = prefix[i];
    }
    args[prefix.length] = Sqoop.SQOOP_OPTIONS_FILE_SPECIFIER;
    args[prefix.length + 1] =  createOptionsFile(fileContent);
    for (int j = 0; j < suffix.length; j++) {
      args[j + 2 + prefix.length] = suffix[j];
    }

    String[] expandedArgs = OptionsFileUtil.expandArguments(args);

    assertSame(prefix, expectedContent, suffix, expandedArgs);

  }

  private void assertSame(String[] prefix, String[] content, String[] suffix,
      String[] actual) {
    Assert.assertTrue(prefix.length + content.length + suffix.length
        == actual.length);

    for (int i = 0; i < prefix.length; i++) {
      Assert.assertTrue(actual[i].equals(prefix[i]));
    }

    for (int i = 0; i < content.length; i++) {
      Assert.assertTrue(actual[i + prefix.length].equals(content[i]));
    }

    for (int i = 0; i < suffix.length; i++) {
      Assert.assertTrue(actual[i + prefix.length + content.length].equals(
          suffix[i]));
    }
  }

  private String createOptionsFile(String[] data) throws Exception {
    File file = File.createTempFile("options", ".opf");
    file.deleteOnExit();

    BufferedWriter writer = null;
    try {
      writer = new BufferedWriter(new FileWriter(file));
      for (String datum : data) {
        writer.write(datum);
        writer.newLine();
      }
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException ex) {
          // No handling required
        }
      }
    }

    return file.getAbsolutePath();
  }
}
