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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.utils.ThrowableDisplayer;
import org.codehaus.groovy.runtime.MethodClosure;
import org.codehaus.groovy.tools.shell.Command;
import org.codehaus.groovy.tools.shell.CommandRegistry;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.codehaus.groovy.tools.shell.IO.Verbosity;

import static org.apache.sqoop.shell.ShellEnvironment.*;

/**
 * Main entry point to Sqoop client.
 *
 * Sqoop shell is implemented on top of Groovy shell.
 */
public final class SqoopShell {

  /**
   * Location of resource file that can contain few initial commands that will
   * be loaded during each client execution.
   */
  private static final String RC_FILE = ".sqoop2rc";

  /**
   * Banner message that is displayed in interactive mode after client start.
   */

  /**
   * Hash of commands that we want to have in history in all cases.
   */
  public final static HashSet<String> commandsToKeep;

  static {
    commandsToKeep = new HashSet<String>();
    commandsToKeep.add("exit");
    commandsToKeep.add("history");
  }

  /**
   * Main entry point to the client execution.
   *
   * @param args Command line arguments
   * @throws Exception
   */
  public static void main (String[] args) throws Exception {
    System.setProperty("groovysh.prompt", Constants.SQOOP_PROMPT);
    Groovysh shell = new Groovysh();

    // Install our error hook (exception handling)
    shell.setErrorHook(new MethodClosure(ThrowableDisplayer.class, "errorHook"));

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
    shell.register(new CloneCommand(shell));
    shell.register(new StartCommand(shell));
    shell.register(new StopCommand(shell));
    shell.register(new StatusCommand(shell));
    shell.register(new EnableCommand(shell));
    shell.register(new DisableCommand(shell));
    shell.register(new GrantCommand(shell));
    shell.register(new RevokeCommand(shell));

    // Configure shared shell io object
    setIo(shell.getIo());

    // We're running in batch mode by default
    setInteractive(false);

    // Let's see if user do have resource file with initial commands that he
    // would like to apply.
    String homeDir = System.getProperty(Constants.PROP_HOMEDIR);
    File rcFile = new File(homeDir, RC_FILE);

    if(rcFile.exists()) {
      printlnResource(Constants.RES_SQOOP_PROMPT_SHELL_LOADRC, RC_FILE);
      interpretFileContent(rcFile, shell);
      printlnResource(Constants.RES_SQOOP_PROMPT_SHELL_LOADEDRC);
    }

    if (args.length == 0) {
      // Interactive mode:
      getIo().setVerbosity(Verbosity.QUIET);
      printlnResource(Constants.RES_SQOOP_SHELL_BANNER);
      println();

      // Switch to interactive mode
      setInteractive(true);
      shell.run(args);

    } else {
      // Batch mode (with a script file):
      File script = new File(args[0]);
      if (!script.isAbsolute()) {
        String userDir = System.getProperty(Constants.PROP_CURDIR);
        script = new File(userDir, args[0]);
      }

      interpretFileContent(script, shell);
    }
  }

  /**
   * Interpret file content in given shell.
   *
   * @param script Script file that should be interpreted
   * @param shell Shell where the script should be interpreted
   * @throws IOException
   */
  private static void interpretFileContent(File script, Groovysh shell) throws IOException {
    BufferedReader in = new BufferedReader(new FileReader(script));
    String line;

    // Iterate over all lines and executed them one by one
    while ((line = in.readLine()) != null) {

      // Skip comments and empty lines as we don't need to interpret those
      if(line.isEmpty() || line.startsWith("#")) {
        continue;
      }

      // Render shell and command to get user perception that it was run as usual
      print(shell.renderPrompt());
      println(line);

      // Manually trigger command line parsing
      Object result = shell.execute(line);

      if (result == null) {
        break;
      }
    }
  }

  private SqoopShell() {
    // Instantiation of this class is prohibited
  }
}
