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
package org.apache.sqoop.shell;

import java.text.MessageFormat;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.shell.core.Constants;
import org.codehaus.groovy.tools.shell.Command;
import org.codehaus.groovy.tools.shell.CommandSupport;
import org.codehaus.groovy.tools.shell.Shell;
import org.codehaus.groovy.tools.shell.util.SimpleCompletor;

import static org.apache.sqoop.shell.ShellEnvironment.*;

public class HelpCommand extends CommandSupport {

  protected HelpCommand(Shell shell) {
    super(shell, Constants.CMD_HELP, Constants.CMD_HELP_SC);
  }

  @Override
  public String getDescription() {
    return resourceString(Constants.RES_HELP_DESCRIPTION);
  }

  @Override
  public String getUsage() {
    return resourceString(Constants.RES_HELP_USAGE);
  }

  @Override
  public String getHelp() {
    return resourceString(Constants.RES_HELP_MESSAGE);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Object execute(List args) {
    if (args.size() == 0) {
      list();
    }
    else {
      help((String)args.get(0));
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private void list() {
    Iterator<Command> iterator;

    // Figure out the max command name and shortcut length dynamically
    int maxName = 0;
    int maxShortcut = 0;
    iterator = shell.getRegistry().commands().iterator();
    while (iterator.hasNext()) {
      Command command = iterator.next();
      if (command.getHidden()) {
        continue;
      }

      if (command.getName().length() > maxName) {
        maxName = command.getName().length();
      }
        
      if (command.getShortcut().length() > maxShortcut) {
        maxShortcut = command.getShortcut().length();
      }
    }

    printlnResource(Constants.RES_HELP_INFO);
    println();

    // List the commands we know about
    printlnResource(Constants.RES_HELP_AVAIL_COMMANDS);

    iterator = shell.getRegistry().commands().iterator();
    while (iterator.hasNext()) {
      Command command = iterator.next();
      if (command.getHidden()) {
        continue;
      }

      String paddedName = StringUtils.rightPad(command.getName(), maxName);
      String paddedShortcut = StringUtils.rightPad(command.getShortcut(), maxShortcut);

      String description = command.getDescription();

      StringBuilder sb = new StringBuilder();
      sb.append("  ")
         .append(MessageFormat.format(resource.getString(Constants
             .RES_HELP_CMD_DESCRIPTION), paddedName,
             paddedShortcut, description));
      println(sb.toString());
    }

    println();
    printlnResource(Constants.RES_HELP_SPECIFIC_CMD_INFO);
    println();
  }

  private void help(String name) {
    Command command = shell.getRegistry().find(name);
    if (command == null) {
      String msg = MessageFormat.format(resource.getString(Constants
          .RES_UNRECOGNIZED_CMD), name);
      throw new SqoopException(ShellError.SHELL_0001, msg);
    }
    printlnResource(Constants.RES_HELP_CMD_USAGE, command.getName(), command.getUsage());
    println();
    println(command.getHelp());
    println();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected List createCompletors() {
    SimpleCompletor completor = new SimpleCompletor();
    Iterator<Command> iterator = registry.iterator();
    while (iterator.hasNext()) {
      Command command = iterator.next();
      if (command.getHidden()) {
        continue;
      }
            
      completor.add(command.getName());
    }

    List completors = new LinkedList();
    completors.add(completor);
    return completors;
  }
}
