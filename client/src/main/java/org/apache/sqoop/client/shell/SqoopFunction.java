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

import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.sqoop.client.core.ClientError;
import org.apache.sqoop.common.SqoopException;

import static org.apache.sqoop.client.shell.ShellEnvironment.*;

@SuppressWarnings("serial")
abstract public class SqoopFunction extends Options {

  public void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printOptions(getIo().out, formatter.getWidth(), this, 0, 4);
  }

  public abstract Object executeFunction(CommandLine line);

  public Object execute(List<String> args) {
    CommandLine line = parseOptions(this, 1, args);
    return executeFunction(line);
  }

  protected CommandLine parseOptions(Options options, int start, List<String> arglist) {
    Iterator<String> iterator = arglist.iterator();
    int i = 0;
    for (; i < start; i ++) {
      iterator.next();
    }

    String[] args = new String[arglist.size() - start];
    for (; i < arglist.size(); i ++) {
      args[i - start] = iterator.next();
    }

    CommandLineParser parser = new GnuParser();
    CommandLine line;
    try {
      line = parser.parse(options, args);
    } catch (ParseException e) {
      throw new SqoopException(ClientError.CLIENT_0003, e.getMessage(), e);
    }
    return line;
  }

  protected long getLong(CommandLine line, String parameterName) {
    return Long.parseLong(line.getOptionValue(parameterName));
  }
}
