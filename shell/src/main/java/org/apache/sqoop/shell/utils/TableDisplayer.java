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

/**
 * Display table based data
 */
public class TableDisplayer {

  public static class Style {
    // Various characters that should be used to "draw" the table's columns, lines, ...
    String headerLineSeparator;
    String lineSeparator;
    boolean linesAfterEachRow;

    // Generated ones
    String columnLeft;
    String columnRight;
    String columnMiddle;

    String lineLeft;
    String lineRight;
    String lineMiddle;

    String headerLeft;
    String headerRight;
    String headerMiddle;

    public Style(String headerLineSeparator, String lineSeparator, String columnSeparator, String lineColumnSeparator, boolean linesAfterEachRow) {
      this.headerLineSeparator = headerLineSeparator;
      this.lineSeparator = lineSeparator;
      this.linesAfterEachRow = linesAfterEachRow;

      this.columnLeft = columnSeparator + " ";
      this.columnRight = " " + columnSeparator;
      this.columnMiddle = " " + columnSeparator + " ";

      this.lineLeft = "+" + lineSeparator;
      this.lineRight = lineSeparator + "+";
      this.lineMiddle = lineSeparator + "+" + lineSeparator;

      this.headerLeft = "+" + headerLineSeparator;
      this.headerRight = headerLineSeparator + "+";
      this.headerMiddle = headerLineSeparator + "+" + headerLineSeparator;
    }
  }

  public static final Style DEFAULT_STYLE = new Style("-", "-", "|", "+", false);
  public static final Style RST_STYLE = new Style("=", "-", "|", "+", true);

  /**
   * Interface that this displayer will use to write out formatted text.
   */
  public interface TableDisplayerWriter {
    /**
     * Print out addition formatted text to the output
     *
     * @param text
     */
    void append(String text);

    /**
     * Flush and print new line
     */
    void newLineAndFlush();
  }

  public TableDisplayer(TableDisplayerWriter writer, Style style) {
    setWriter(writer);
    this.style = style;
  }

  /**
   * Writer instance that we should use to write something out
   */
  private TableDisplayerWriter writer;

  /**
   * Style that should be used to the generated tables
   */
  private Style style;

  /**
   * Reset the writer if needed.
   *
   * @param writer
   */
  public void setWriter(TableDisplayerWriter writer) {
    this.writer = writer;
  }

  /**
   * Display given columns in nice table structure to given IO object.
   *
   * @param headers List of headers
   * @param columns Array of columns
   */
  public void display(List<String> headers, List<String> ...columns) {
    assert columns != null;
    assert columns.length >= 1;
    assert writer != null;
    if(headers != null) {
      assert headers.size() == columns.length;
    }

    // Count of columns
    int columnCount = columns.length;

    // List of all maximal widths of each column
    List<Integer> widths = new LinkedList<Integer>();
    for(int i = 0; i < columnCount; i++) {
      widths.add(getMaximalWidth(headers != null ? headers.get(i) : null, columns[i]));
    }

    // First line is border
    drawLine(widths);

    if(headers != null) {
      // Print out header (text is centralised)
      print(style.columnLeft);
      for (int i = 0; i < columnCount; i++) {
        print(StringUtils.center(headers.get(i), widths.get(i), ' '));
        print((i == columnCount - 1) ? style.columnRight : style.columnMiddle);
      }
      println();

      // End up header by border
      drawHeaderLine(widths);
    }

    // Number of rows in the table
    int rows = getMaximalRows(columns);

    // Print out each row
    for(int row = 0 ; row < rows; row++) {
      print(style.columnLeft);
      for(int i = 0 ; i < columnCount; i++) {
        print(StringUtils.rightPad(columns[i].get(row), widths.get(i), ' '));
        print((i == columnCount - 1) ? style.columnRight : style.columnMiddle);

      }
      println();

      if(style.linesAfterEachRow) {
        drawLine(widths);
      }
    }

    // End table by final border
    if(!style.linesAfterEachRow) {
      drawLine(widths);
    }
  }

  /**
   * Draw border line
   *
   * @param widths List of widths of each column
   */
  private void drawLine(List<Integer> widths) {
    drawLine(widths, style.lineLeft, style.lineRight, style.lineMiddle, style.lineSeparator);
  }
  private void drawHeaderLine(List<Integer> widths) {
    drawLine(widths, style.headerLeft, style.headerRight, style.headerMiddle, style.headerLineSeparator);
  }

  private void drawLine(List<Integer> widths, String left, String right, String middle, String line) {
    int last = widths.size() - 1;
    print(left);
    for(int i = 0; i < widths.size(); i++) {
      print(StringUtils.repeat(line, widths.get(i)));
      print((i == last) ? right : middle);
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
  private int getMaximalWidth(String header, List<String> column) {
    assert column != null;

    int max = header != null ? header.length() : 0;

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
  private int getMaximalRows(List<String>... columns) {
    int max = 0;

    for(List<String> column : columns) {
      if(column.size() > max) {
        max = column.size();
      }
    }

    return max;
  }

  private void print(String text) {
    writer.append(text);
  }

  private void println() {
    writer.newLineAndFlush();
  }

}
