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

import org.apache.commons.lang.StringUtils;

import java.util.LinkedList;
import java.util.List;

import static org.apache.sqoop.shell.ShellEnvironment.*;

/**
 * Display table based data
 */
public class TableDisplayer {

  /**
   * Display given columns in nice table structure to given IO object.
   *
   * @param headers List of headers
   * @param columns Array of columns
   */
  public static void display(List<String> headers, List<String> ...columns) {
    assert headers != null;
    assert columns != null;
    assert headers.size() == columns.length;

    // Count of columns
    int columnCount = headers.size();

    // List of all maximal widths of each column
    List<Integer> widths = new LinkedList<Integer>();
    for(int i = 0; i < columnCount; i++) {
      widths.add(getMaximalWidth(headers.get(i), columns[i]));
    }

    // First line is border
    drawLine(widths);

    // Print out header (text is centralised)
    print("| ");
    for(int i = 0 ; i < columnCount; i++) {
      print(StringUtils.center(headers.get(i), widths.get(i), ' '));
      print((i == columnCount -1) ? " |" : " | ");
    }
    println();

    // End up header by border
    drawLine(widths);

    // Number of rows in the table
    int rows = getMaximalRows(columns);

    // Print out each row
    for(int row = 0 ; row < rows; row++) {
      print("| ");
      for(int i = 0 ; i < columnCount; i++) {
        print(StringUtils.rightPad(columns[i].get(row), widths.get(i), ' '));
        print((i == columnCount -1) ? " |" : " | ");
      }
      println();
    }

    // End table by final border
    drawLine(widths);
  }

  /**
   * Draw border line
   *
   * @param widths List of widths of each column
   */
  private static void drawLine(List<Integer> widths) {
    int last = widths.size() - 1;
    print("+-");
    for(int i = 0; i < widths.size(); i++) {
      print(StringUtils.repeat("-", widths.get(i)));
      print((i == last) ? "-+" : "-+-");
    }
    println();
  }

  /**
   * Get maximal width for given column with it's associated header.
   *
   * @param header Associated header
   * @param column All column values
   * @return Maximal
   */
  private static int getMaximalWidth(String header, List<String> column) {
    assert header != null;
    assert column != null;

    int max = header.length();

    for(String value : column) {
      if(value != null && value.length() > max) {
        max = value.length();
      }
    }

    return max;
  }

  /**
   * Get maximal number of rows available in the column list
   *
   * @param columns Array with all column values
   * @return
   */
  private static int getMaximalRows(List<String>... columns) {
    int max = 0;

    for(List<String> column : columns) {
      if(column.size() > max) {
        max = column.size();
      }
    }

    return max;
  }

  private TableDisplayer() {
    // Instantiation is prohibited
  }
}
