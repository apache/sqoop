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
package org.apache.sqoop.client.shell;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.sqoop.client.core.Environment;
import org.codehaus.groovy.tools.shell.IO;

import java.util.List;

/**
 *
 */
public class SetOptionFunction extends SqoopFunction {

  public static final String NAME = "name";
  public static final String VALUE = "value";

  private IO io;

  @SuppressWarnings("static-access")
  protected SetOptionFunction(IO io) {
    this.io = io;

    this.addOption(OptionBuilder.hasArg()
      .withDescription("Client option name")
      .withLongOpt(NAME)
      .create(NAME.charAt(0)));
    this.addOption(OptionBuilder.hasArg()
      .withDescription("New option value")
      .withLongOpt(VALUE)
      .create(VALUE.charAt(0)));
  }

  public Object execute(List<String> args) {
    CommandLine line = parseOptions(this, 1, args);
    if (!line.hasOption(NAME)) {
      io.out.println("Required argument --name is missing.");
      return null;
    }
    if (!line.hasOption(VALUE)) {
      io.out.println("Required argument --value is missing.");
      return null;
    }

    handleOptionSetting(line.getOptionValue(NAME), line.getOptionValue(VALUE));

    io.out.println();
    return null;
  }

  private void handleOptionSetting(String name, String value) {
    if(name.equals("verbose")) {
      boolean newValue = false;

      if(value.equals("1") || value.equals("true")) {
        newValue = true;
      }

      Environment.setVerbose(newValue);
      io.out.println("Verbose option was changed to " + newValue);
      return;
    }

    io.out.println("Unknown option " + name + ". Ignoring...");
  }
}
