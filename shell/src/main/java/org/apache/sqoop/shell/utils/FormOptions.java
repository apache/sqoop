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

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MInputType;
import org.apache.sqoop.shell.core.ShellError;

/**
 * Utilities for automatically creating org.apache.commons.cli.Option objects.
 */
public class FormOptions {
  /**
   * This method is used to automatically generate keys
   * for a particular input.
   *
   * @param prefix Prefix to prepend to CLI option keys
   * @param input
   * @return
   */
  @SuppressWarnings("rawtypes")
  public static String getOptionKey(String prefix, MInput input) {
    return prefix + "-" + input.getName().replace('.', '-');
  }

  /**
   * This method is used to automatically generate CLI options
   * for a list of forms.
   *
   * @param prefix Prefix to prepend to CLI option keys
   * @param forms Forms to get options for
   * @return
   */
  public static List<Option> getFormsOptions(String prefix, List<MForm> forms) {
    List<Option> options = new LinkedList<Option>();
    for (MForm form : forms) {
      List<Option> formOptions = getFormOptions(prefix, form);
      options.addAll(formOptions);
    }
    return options;
  }

  /**
   * This method is used to automatically generate CLI options
   * for a particular form.
   *
   * @param prefix Prefix to prepend to CLI option keys
   * @param form Form to get options for
   * @return List<Option>
   */
  @SuppressWarnings({ "rawtypes", "static-access" })
  public static List<Option> getFormOptions(String prefix, MForm form) {
    List<Option> options = new LinkedList<Option>();
    for (MInput input : form.getInputs()) {
      if (input.getType().equals(MInputType.BOOLEAN)) {
        options.add(OptionBuilder
                    .withLongOpt(getOptionKey(prefix, input))
                    .create());
      } else {
        options.add(OptionBuilder
                    .withLongOpt(getOptionKey(prefix, input))
                    .hasArg()
                    .create());
      }
    }
    return options;
  }

  /**
   * Parses command line options.
   *
   * @param options parse arglist against these.
   * @param start beginning index in arglist.
   * @param arglist arguments to parse.
   * @param stopAtNonOption stop parsing when nonoption found in arglist.
   * @return CommandLine object
   */
  public static CommandLine parseOptions(Options options, int start, List<String> arglist, boolean stopAtNonOption) {
    String[] args = arglist.subList(start, arglist.size()).toArray(new String[arglist.size() - start]);

    CommandLineParser parser = new GnuParser();
    CommandLine line;
    try {
      line = parser.parse(options, args, stopAtNonOption);
    } catch (ParseException e) {
      throw new SqoopException(ShellError.SHELL_0003, e.getMessage(), e);
    }
    return line;
  }
}
