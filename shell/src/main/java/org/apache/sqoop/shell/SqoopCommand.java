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

import groovy.lang.GroovyShell;
import groovy.lang.MissingPropertyException;
import groovy.lang.Script;

import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.utils.ClassUtils;
import org.codehaus.groovy.tools.shell.ComplexCommandSupport;
import org.codehaus.groovy.tools.shell.Shell;

import static org.apache.sqoop.shell.ShellEnvironment.*;

/**
 * Sqoop shell command.
 *
 * Every command should define following resource properties:
 *
 * $command.description
 *    One sentence describing purpose of the command, displayed on "help" command.
 */
public abstract class SqoopCommand extends ComplexCommandSupport {

  /**
   * Command name
   */
  private String name;

  /**
   * Function map given by concrete implementation.
   *
   * Key: Name of the function as is present in the shell
   * Value: Class name implementing the function
   */
  private final Map<String, Class<? extends SqoopFunction>> functionNames;

  /**
   * Instantiated functions for reuse. Built lazily.
   */
  private final Map<String, SqoopFunction> functionInstances;

  protected SqoopCommand(Shell shell,
                         String name,
                         String shortcut) {
    this(shell, name, shortcut, null);
  }

  protected SqoopCommand(Shell shell,
                         String name,
                         String shortcut,
                         Map<String, Class<? extends SqoopFunction>> funcs) {
    super(shell, name, shortcut);

    this.name = name;
    this.functionNames = funcs;
    this.functionInstances = new HashMap<String, SqoopFunction>();

    this.functions = new LinkedList<String>();
    if (funcs != null) {
      this.functions.addAll(funcs.keySet());
    }
  }

  @Override
  public String getDescription() {
    return resourceString(name + ".description");
  }

  @Override
  public String getUsage() {
    return new StringBuilder()
      .append("[")
      .append(StringUtils.join(functionNames.keySet(), "|"))
      .append("]")
      .toString();
  }

  @Override
  public String getHelp() {
    return getDescription() + ".";
  }

  /**
   * Override execute method
   */
  @Override
  public Object execute(List args) {
    resolveVariables(args);
    return executeCommand(args);
  }

  /**
   * Abstract executeCommand
   * @param args list
   * @return Object
   */
  public Object executeCommand(List args) {
    if (args.size() == 0) {
      printlnResource(Constants.RES_SHARED_USAGE, name, getUsage());
      return null;
    }

    String func = (String)args.get(0);

    // Unknown function
    if(!functionNames.containsKey(func)) {
      printlnResource(Constants.RES_SHARED_UNKNOWN_FUNCTION, func);
      return null;
    }

    // If we already do have the instance, execute it
    if(functionInstances.containsKey(func)) {
      return functionInstances.get(func).execute(args);
    }

    // Otherwise create new instance
    Class klass = functionNames.get(func);
    SqoopFunction instance = (SqoopFunction) ClassUtils.instantiate(klass);
    if(instance == null) {
      // This is pretty much a developer error as it shouldn't happen without changing and testing code
      throw new SqoopException(ShellError.SHELL_0000, "Can't instantiate class " + klass);
    }

    functionInstances.put(func, instance);

    // And return the function execution
    return instance.execute(args);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected void resolveVariables(List arg) {
    List temp = new ArrayList();
    GroovyShell gs = new GroovyShell(getBinding());
    for(Object obj:arg) {
      Script scr = gs.parse("\""+(String)obj+"\"");
      try {
        temp.add(scr.run().toString());
      }
      catch(MissingPropertyException e) {
        throw new SqoopException(ShellError.SHELL_0004, e.getMessage(), e);
      }
    }
    Collections.copy(arg, temp);
  }
}
