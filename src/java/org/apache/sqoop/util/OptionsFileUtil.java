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

package org.apache.sqoop.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.cloudera.sqoop.Sqoop;

/**
 * Provides utility functions to read in options file. An options file is a
 * regular text file with each line specifying a separate option. An option
 * may continue into a following line by using a back-slash separator character
 * at the end of the non-terminating line. Options file also allow empty lines
 * and comment lines which are disregarded. Comment lines must begin with the
 * hash character as the first character. Leading and trailing white-spaces are
 * ignored for any options read from the Options file.
 */
public final class OptionsFileUtil {

  public static final Log LOG = LogFactory.getLog(
                                  OptionsFileUtil.class.getName());

  private OptionsFileUtil() { }

  /**
   * Expands any options file that may be present in the given set of arguments.
   *
   * @param args the given arguments
   * @return a new string array that contains the expanded arguments.
   * @throws Exception
   */
  public static String[] expandArguments(String[] args) throws Exception {
    List<String> options = new ArrayList<String>();

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals(Sqoop.SQOOP_OPTIONS_FILE_SPECIFIER)) {
        if (i == args.length - 1) {
          throw new Exception("Missing options file");
        }

        String fileName = args[++i];
        File optionsFile = new File(fileName);
        BufferedReader reader = null;
        StringBuilder buffer = new StringBuilder();
        try {
          reader = new BufferedReader(new FileReader(optionsFile));
          String nextLine = null;
          while ((nextLine = reader.readLine()) != null) {
            nextLine = nextLine.trim();
            if (nextLine.length() == 0 || nextLine.startsWith("#")) {
              // empty line or comment
              continue;
            }

            buffer.append(nextLine);
            if (nextLine.endsWith("\\")) {
              if (buffer.charAt(0) == '\'' || buffer.charAt(0) == '"') {
                throw new Exception(
                    "Multiline quoted strings not supported in file("
                      + fileName + "): " + buffer.toString());
              }
              // Remove the trailing back-slash and continue
              buffer.deleteCharAt(buffer.length()  - 1);
            } else {
              // The buffer contains a full option
              options.add(
                  removeQuotesEncolosingOption(fileName, buffer.toString()));
              buffer.delete(0, buffer.length());
            }
          }

          // Assert that the buffer is empty
          if (buffer.length() != 0) {
            throw new Exception("Malformed option in options file("
                + fileName + "): " + buffer.toString());
          }
        } catch (IOException ex) {
          throw new Exception("Unable to read options file: " + fileName, ex);
        } finally {
          if (reader != null) {
            try {
              reader.close();
            } catch (IOException ex) {
              LOG.info("Exception while closing reader", ex);
            }
          }
        }
      } else {
        // Regular option. Parse it and put it on the appropriate list
        options.add(args[i]);
      }
    }

    return options.toArray(new String[options.size()]);
  }

  /**
   * Removes the surrounding quote characters as needed. It first attempts to
   * remove surrounding double quotes. If successful, the resultant string is
   * returned. If no surrounding double quotes are found, it attempts to remove
   * surrounding single quote characters. If successful, the resultant string
   * is returned. If not the original string is returnred.
   * @param fileName
   * @param option
   * @return
   * @throws Exception
   */
  private static String removeQuotesEncolosingOption(
      String fileName, String option) throws Exception {

    // Attempt to remove double quotes. If successful, return.
    String option1 = removeQuoteCharactersIfNecessary(fileName, option, '"');
    if (!option1.equals(option)) {
      // Quotes were successfully removed
      return option1;
    }

    // Attempt to remove single quotes.
    return removeQuoteCharactersIfNecessary(fileName, option, '\'');
  }

  /**
   * Removes the surrounding quote characters from the given string. The quotes
   * are identified by the quote parameter, the given string by option. The
   * fileName parameter is used for raising exceptions with relevant message.
   * @param fileName
   * @param option
   * @param quote
   * @return
   * @throws Exception
   */
  private static String removeQuoteCharactersIfNecessary(String fileName,
      String option, char quote) throws Exception {
    boolean startingQuote = (option.charAt(0) == quote);
    boolean endingQuote = (option.charAt(option.length() - 1) == quote);

    if (startingQuote && endingQuote) {
      if (option.length() == 1) {
        throw new Exception("Malformed option in options file("
            + fileName + "): " + option);
      }
      return option.substring(1, option.length() - 1);
    }

    if (startingQuote || endingQuote) {
       throw new Exception("Malformed option in options file("
           + fileName + "): " + option);
    }

    return option;
  }

}
