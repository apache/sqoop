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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Iterator;

import org.codehaus.groovy.tools.shell.Command;
import org.codehaus.groovy.tools.shell.CommandRegistry;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO.Verbosity;

public class SqoopShell
{
  private static final String banner =
      "@|green Sqoop Shell:|@ Type '@|bold help|@' or '@|bold \\h|@' for help.";

  public static HashSet<String> commandsToKeep;
  static {
    commandsToKeep = new HashSet<String>();
    commandsToKeep.add("exit");
    commandsToKeep.add("history");
  }

  public static void main (String[] args) throws Exception
  {
    System.setProperty("groovysh.prompt", "sqoop");
    Groovysh shell = new Groovysh();

    CommandRegistry registry = shell.getRegistry();
    @SuppressWarnings("unchecked")
    Iterator<Command> iterator = registry.iterator();
    while (iterator.hasNext()) {
      Command command = iterator.next();
      if (!commandsToKeep.contains(command.getName())) {
        iterator.remove();
        // remove from "names" set to avoid duplicate error.
        registry.remove(command);
      }
    }

    shell.register(new HelpCommand(shell));
    shell.register(new SetCommand(shell));
    shell.register(new ShowCommand(shell));
    shell.register(new CreateCommand(shell));
    shell.register(new DeleteCommand(shell));
    shell.register(new UpdateCommand(shell));

    if (args.length == 0) {
      // Interactive mode:
      shell.getIo().setVerbosity(Verbosity.QUIET);
      shell.getIo().out.println(banner);
      shell.getIo().out.println();
      shell.run(args);

    } else {
      // Batch mode (with a script file):
      File script = new File(args[0]);
      if (!script.isAbsolute()) {
        String userDir = System.getProperty("user.dir");
        script = new File(userDir, args[0]);
      }

      BufferedReader in = new BufferedReader(new FileReader(script));
      String line;
      while ((line = in.readLine()) != null) {
        shell.getIo().out.print(shell.renderPrompt());
        shell.getIo().out.println(line);
        Object result = shell.execute(line);
        if (result != null) {
          shell.getIo().out.println(result);
        }
      }
    }
  }
}
