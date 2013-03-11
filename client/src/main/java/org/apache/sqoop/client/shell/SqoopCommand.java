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

import groovy.lang.GroovyShell;
import groovy.lang.MissingPropertyException;
import groovy.lang.Script;

import java.util.*;

import org.apache.sqoop.client.core.ClientError;
import org.apache.sqoop.common.SqoopException;
import org.codehaus.groovy.tools.shell.ComplexCommandSupport;
import org.codehaus.groovy.tools.shell.Shell;

public abstract class SqoopCommand extends ComplexCommandSupport
{
  private String descriptionPrefix;
  private String descriptionPostfix;

  private String description;
  private String usage;
  private String help;

  @SuppressWarnings("unchecked")
  protected SqoopCommand(Shell shell, String name, String shortcut,
      String[] funcs, String descriptionPrefix, String descriptionPostfix) {
    super(shell, name, shortcut);

    this.functions = new LinkedList<String>();
    for (String func : funcs) {
      this.functions.add(func);
    }

    this.descriptionPrefix = descriptionPrefix;
    this.descriptionPostfix = descriptionPostfix;
  }

  @Override
  public String getDescription() {
    if (description == null) {
      StringBuilder sb = new StringBuilder();

      if (descriptionPrefix != null) {
        sb.append(descriptionPrefix);
        sb.append(" ");
      }

      @SuppressWarnings("unchecked")
      Iterator<String> iterator = functions.iterator();
      int size = functions.size();
      sb.append(iterator.next());
      if (size > 1) {
        for (int i = 1; i < (size - 1); i++) {
          sb.append(", ");
          sb.append(iterator.next());
        }
        sb.append(" or ");
        sb.append(iterator.next());
      }

      if (descriptionPostfix != null) {
        sb.append(" ");
        sb.append(descriptionPostfix);
      }

      description = sb.toString();
    }

    return description;
  }

  @Override
  public String getUsage() {
    if (usage == null) {
      StringBuilder sb = new StringBuilder();

      sb.append("[");

      @SuppressWarnings("unchecked")
      Iterator<String> iterator = functions.iterator();
      int size = functions.size();
      sb.append(iterator.next());
      for (int i = 1; i < size; i++) {
        sb.append("|");
        sb.append(iterator.next());
      }

      sb.append("]");

      usage = sb.toString();
    }

    return usage;
  }

  @Override
  public String getHelp() {
    if (help == null) {
      help = getDescription() + ".";
    }

    return help;
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
  public abstract Object executeCommand(List args);

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
        throw new SqoopException(ClientError.CLIENT_0004, e.getMessage(), e);
      }
    }
    Collections.copy(arg, temp);
  }
}
