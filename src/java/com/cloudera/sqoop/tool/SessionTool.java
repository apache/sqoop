/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.sqoop.tool;

import java.io.IOException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.cli.ToolOptions;

import com.cloudera.sqoop.metastore.hsqldb.HsqldbSessionStorage;
import com.cloudera.sqoop.metastore.SessionData;
import com.cloudera.sqoop.metastore.SessionStorage;
import com.cloudera.sqoop.metastore.SessionStorageFactory;

/**
 * Tool that creates and executes saved sessions.
 */
public class SessionTool extends BaseSqoopTool {

  public static final Log LOG = LogFactory.getLog(
      SessionTool.class.getName());

  private enum SessionOp {
    SessionCreate,
    SessionDelete,
    SessionExecute,
    SessionList,
    SessionShow,
  };

  private Map<String, String> sessionDescriptor;
  private String sessionName;
  private SessionOp operation;
  private SessionStorage storage;

  public SessionTool() {
    super("session");
  }

  /**
   * Given an array of strings, return all elements of this
   * array up to (but not including) the first instance of "--".
   */
  private String [] getElementsUpToDoubleDash(String [] array) {
    String [] parseableChildArgv = null;
    for (int i = 0; i < array.length; i++) {
      if ("--".equals(array[i])) {
        parseableChildArgv = Arrays.copyOfRange(array, 0, i);
        break;
      }
    }

    if (parseableChildArgv == null) {
      // Didn't find any nested '--'.
      parseableChildArgv = array;
    }

    return parseableChildArgv;
  }

  /**
   * Given an array of strings, return the first instance
   * of "--" and all following elements.
   * If no "--" exists, return null.
   */
  private String [] getElementsAfterDoubleDash(String [] array) {
    String [] extraChildArgv = null;
    for (int i = 0; i < array.length; i++) {
      if ("--".equals(array[i])) {
        extraChildArgv = Arrays.copyOfRange(array, i, array.length);
        break;
      }
    }

    return extraChildArgv;
  }

  private int configureChildTool(SqoopOptions childOptions,
      SqoopTool childTool, String [] childArgv) {
    // Within the child arguments there may be a '--' followed by
    // dependent args. Stash them off to the side.

    // Everything up to the '--'.
    String [] parseableChildArgv = getElementsUpToDoubleDash(childArgv);

    // The '--' and any subsequent args.
    String [] extraChildArgv = getElementsAfterDoubleDash(childArgv);
   
    // Now feed the arguments into the tool itself.
    try {
      childOptions = childTool.parseArguments(parseableChildArgv,
          null, childOptions, false);
      childTool.appendArgs(extraChildArgv);
      childTool.validateOptions(childOptions);
    } catch (ParseException pe) {
      LOG.error("Error parsing arguments to the session-specific tool.");
      LOG.error("See 'sqoop help <tool>' for usage.");
      return 1;
    } catch (SqoopOptions.InvalidOptionsException e) {
      System.err.println(e.getMessage());
      return 1;
    }

    return 0; // Success.
  }

  private int createSession(SqoopOptions options) throws IOException {
    // In our extraArguments array, we should have a '--' followed by
    // a tool name, and any tool-specific arguments.
    // Create an instance of the named tool and then configure it to
    // get a SqoopOptions out which we will serialize into a session.
    int dashPos = getDashPosition(extraArguments);
    int toolArgPos = dashPos + 1;
    if (null == extraArguments || toolArgPos < 0
        || toolArgPos >= extraArguments.length) {
      LOG.error("No tool specified; cannot create a session.");
      LOG.error("Use: sqoop create-session [session-args] "
          + "-- <tool-name> [tool-args]");
      return 1;
    }

    String sessionToolName = extraArguments[toolArgPos];
    SqoopTool sessionTool = SqoopTool.getTool(sessionToolName);
    if (null == sessionTool) {
      LOG.error("No such tool available: " + sessionToolName);
      return 1;
    }

    // Create a SqoopOptions and Configuration based on the current one,
    // but deep-copied. This will be populated within the session.
    SqoopOptions sessionOptions = new SqoopOptions();
    sessionOptions.setConf(new Configuration(options.getConf()));

    // Get the arguments to feed to the child tool.
    String [] childArgs = Arrays.copyOfRange(extraArguments, toolArgPos + 1,
        extraArguments.length);

    int confRet = configureChildTool(sessionOptions, sessionTool, childArgs);
    if (0 != confRet) {
      // Error.
      return confRet;
    }

    // Now that the tool is fully configured, materialize the session.
    SessionData sessionData = new SessionData(sessionOptions, sessionTool);
    this.storage.create(sessionName, sessionData);
    return 0; // Success.
  }

  private int listSessions(SqoopOptions opts) throws IOException {
    List<String> sessionNames = storage.list();
    System.out.println("Available sessions:");
    for (String name : sessionNames) {
      System.out.println("  " + name);
    }
    return 0;
  }

  private int deleteSession(SqoopOptions opts) throws IOException {
    this.storage.delete(sessionName);
    return 0;
  }

  private int execSession(SqoopOptions opts) throws IOException {
    SessionData data = this.storage.read(sessionName);
    if (null == data) {
      LOG.error("No such session: " + sessionName);
      return 1;
    }

    SqoopOptions childOpts = data.getSqoopOptions();
    SqoopTool childTool = data.getSqoopTool();

    int dashPos = getDashPosition(extraArguments);
    String [] childArgv;
    if (dashPos >= extraArguments.length) {
      childArgv = new String[0];
    } else {
      childArgv = Arrays.copyOfRange(extraArguments, dashPos + 1,
          extraArguments.length);
    }

    int confRet = configureChildTool(childOpts, childTool, childArgv);
    if (0 != confRet) {
      // Error.
      return confRet;
    }

    return childTool.run(childOpts);
  }

  private int showSession(SqoopOptions opts) throws IOException {
    SessionData data = this.storage.read(sessionName);
    if (null == data) {
      LOG.error("No such session: " + sessionName);
      return 1;
    }

    SqoopOptions childOpts = data.getSqoopOptions();
    SqoopTool childTool = data.getSqoopTool();

    System.out.println("Session: " + sessionName);
    System.out.println("Tool: " + childTool.getToolName());

    System.out.println("Options:");
    System.out.println("----------------------------");
    Properties props = childOpts.writeProperties();
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      System.out.println(entry.getKey().toString() + " = " + entry.getValue());
    }

    // TODO: This does not show entries in the Configuration
    // (SqoopOptions.getConf()) which were stored as different from the
    // default. 

    return 0;
  }

  @Override
  /** {@inheritDoc} */
  public int run(SqoopOptions options) {
    // Get a SessionStorage instance to use to materialize this session.
    SessionStorageFactory ssf = new SessionStorageFactory(options.getConf());
    this.storage = ssf.getSessionStorage(sessionDescriptor);
    if (null == this.storage) {
      LOG.error("There is no SessionStorage implementation available");
      LOG.error("that can read your specified session descriptor.");
      LOG.error("Don't know where to save this session info! You may");
      LOG.error("need to specify the connect string with --meta-connect.");
      return 1;
    }

    try {
      // Open the storage layer.
      this.storage.open(this.sessionDescriptor);

      // And now determine what operation to perform with it.
      switch (operation) {
      case SessionCreate:
        return createSession(options);
      case SessionDelete:
        return deleteSession(options);
      case SessionExecute:
        return execSession(options);
      case SessionList:
        return listSessions(options);
      case SessionShow:
        return showSession(options);
      default:
        LOG.error("Undefined session operation: " + operation);
        return 1;
      }
    } catch (IOException ioe) {
      LOG.error("I/O error performing session operation: "
          + StringUtils.stringifyException(ioe));
      return 1;
    } finally {
      if (null != this.storage) {
        try {
          storage.close();
        } catch (IOException ioe) {
          LOG.warn("IOException closing SessionStorage: "
              + StringUtils.stringifyException(ioe));
        }
      }
    }
  }

  @Override
  /** Configure the command-line arguments we expect to receive */
  public void configureOptions(ToolOptions toolOptions) {
    toolOptions.addUniqueOptions(getSessionOptions());
  }

  @Override
  /** {@inheritDoc} */
  public void applyOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {

    if (in.hasOption(VERBOSE_ARG)) {
      // Immediately switch into DEBUG logging.
      Category sqoopLogger = Logger.getLogger(
          Sqoop.class.getName()).getParent();
      sqoopLogger.setLevel(Level.DEBUG);
      LOG.debug("Enabled debug logging.");
    }

    if (in.hasOption(HELP_ARG)) {
      ToolOptions toolOpts = new ToolOptions();
      configureOptions(toolOpts);
      printHelp(toolOpts);
      throw new InvalidOptionsException("");
    }

    this.sessionDescriptor = new TreeMap<String, String>();

    if (in.hasOption(SESSION_METASTORE_ARG)) {
      this.sessionDescriptor.put(HsqldbSessionStorage.META_CONNECT_KEY,
          in.getOptionValue(SESSION_METASTORE_ARG));
    }

    // These are generated via an option group; exactly one
    // of this exhaustive list will always be selected.
    if (in.hasOption(SESSION_CMD_CREATE_ARG)) {
      this.operation = SessionOp.SessionCreate;
      this.sessionName = in.getOptionValue(SESSION_CMD_CREATE_ARG);
    } else if (in.hasOption(SESSION_CMD_DELETE_ARG)) {
      this.operation = SessionOp.SessionDelete;
      this.sessionName = in.getOptionValue(SESSION_CMD_DELETE_ARG);
    } else if (in.hasOption(SESSION_CMD_EXEC_ARG)) {
      this.operation = SessionOp.SessionExecute;
      this.sessionName = in.getOptionValue(SESSION_CMD_EXEC_ARG);
    } else if (in.hasOption(SESSION_CMD_LIST_ARG)) {
      this.operation = SessionOp.SessionList;
    } else if (in.hasOption(SESSION_CMD_SHOW_ARG)) {
      this.operation = SessionOp.SessionShow;
      this.sessionName = in.getOptionValue(SESSION_CMD_SHOW_ARG);
    }
  }

  @Override
  /** {@inheritDoc} */
  public void validateOptions(SqoopOptions options)
      throws InvalidOptionsException {

    if (null == operation
        || (null == this.sessionName && operation != SessionOp.SessionList)) {
      throw new InvalidOptionsException("No session operation specified"
          + HELP_STR);
    }

    if (operation == SessionOp.SessionCreate) {
      // Check that we have a '--' followed by at least a tool name.
      if (extraArguments == null || extraArguments.length == 0) {
        throw new InvalidOptionsException(
            "Expected: -- <tool-name> [tool-args] "
            + HELP_STR);
      }
    }

    int dashPos = getDashPosition(extraArguments);
    if (hasUnrecognizedArgs(extraArguments, 0, dashPos)) {
      throw new InvalidOptionsException(HELP_STR);
    }
  }

  @Override
  /** {@inheritDoc} */
  public void printHelp(ToolOptions opts) {
    System.out.println("usage: sqoop " + getToolName()
        + " [GENERIC-ARGS] [SESSION-ARGS] [-- [<tool-name>] [TOOL-ARGS]]");
    System.out.println("");
    
    opts.printHelp();
  
    System.out.println("");
    System.out.println("Generic Hadoop command-line arguments:");
    System.out.println("(must preceed any tool-specific arguments)");
    ToolRunner.printGenericCommandUsage(System.out);
  }
}

