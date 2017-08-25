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

package org.apache.sqoop.tool;

import static org.apache.sqoop.manager.JdbcDrivers.DB2;
import static org.apache.sqoop.manager.JdbcDrivers.HSQLDB;
import static org.apache.sqoop.manager.JdbcDrivers.MYSQL;
import static org.apache.sqoop.manager.JdbcDrivers.ORACLE;
import static org.apache.sqoop.manager.JdbcDrivers.POSTGRES;
import static org.apache.sqoop.manager.JdbcDrivers.SQLSERVER;

import java.io.IOException;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.cli.ToolOptions;
import com.cloudera.sqoop.metastore.GenericJobStorage;
import com.cloudera.sqoop.metastore.JobData;
import com.cloudera.sqoop.metastore.JobStorage;
import com.cloudera.sqoop.metastore.JobStorageFactory;
import org.apache.sqoop.manager.JdbcDrivers;
import org.apache.sqoop.util.LoggingUtils;

/**
 * Tool that creates and executes saved jobs.
 */
public class JobTool extends com.cloudera.sqoop.tool.BaseSqoopTool {

  public static final Log LOG = LogFactory.getLog(
      JobTool.class.getName());
  private static final String DASH_STR  = "--";

  private static Set<JdbcDrivers> SUPPORTED_DRIVERS = EnumSet.of(HSQLDB, MYSQL, ORACLE, POSTGRES, DB2, SQLSERVER);

  private enum JobOp {
    JobCreate,
    JobDelete,
    JobExecute,
    JobList,
    JobShow,
  };

  private Map<String, String> storageDescriptor;
  private String jobName;
  private JobOp operation;
  private JobStorage storage;

  public JobTool() {
    super("job");
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
      LOG.error("Error parsing arguments to the job-specific tool: ", pe);
      LOG.error("See 'sqoop help <tool>' for usage.");
      return 1;
    } catch (SqoopOptions.InvalidOptionsException e) {
      System.err.println(e.getMessage());
      return 1;
    }

    return 0; // Success.
  }

  private int createJob(SqoopOptions options) throws IOException {
    // In our extraArguments array, we should have a '--' followed by
    // a tool name, and any tool-specific arguments.
    // Create an instance of the named tool and then configure it to
    // get a SqoopOptions out which we will serialize into a job.
    int dashPos = getDashPosition(extraArguments);
    int toolArgPos = dashPos + 1;
    if (null == extraArguments || toolArgPos < 0
        || toolArgPos >= extraArguments.length) {
      LOG.error("No tool specified; cannot create a job.");
      LOG.error("Use: sqoop job --create <job-name> "
          + "-- <tool-name> [tool-args]");
      return 1;
    }

    String jobToolName = extraArguments[toolArgPos];
    SqoopTool jobTool = SqoopTool.getTool(jobToolName);
    if (null == jobTool) {
      LOG.error("No such tool available: " + jobToolName);
      return 1;
    }

    // Create a SqoopOptions and Configuration based on the current one,
    // but deep-copied. This will be populated within the job.
    SqoopOptions jobOptions = new SqoopOptions();
    jobOptions.setConf(new Configuration(options.getConf()));

    // Get the arguments to feed to the child tool.
    String [] childArgs = Arrays.copyOfRange(extraArguments, toolArgPos + 1,
        extraArguments.length);

    int confRet = configureChildTool(jobOptions, jobTool, childArgs);
    if (0 != confRet) {
      // Error.
      return confRet;
    }

    // Now that the tool is fully configured, materialize the job.
    //TODO(jarcec): Remove the cast when JobData will be moved to apache package
    JobData jobData = new JobData(jobOptions,
            (com.cloudera.sqoop.tool.SqoopTool)jobTool);
    this.storage.create(jobName, jobData);
    return 0; // Success.
  }

  private int listJobs(SqoopOptions opts) throws IOException {
    List<String> jobNames = storage.list();
    System.out.println("Available jobs:");
    for (String name : jobNames) {
      System.out.println("  " + name);
    }
    return 0;
  }

  private int deleteJob(SqoopOptions opts) throws IOException {
    this.storage.delete(jobName);
    return 0;
  }

  private int execJob(SqoopOptions opts) throws IOException {
    JobData data = this.storage.read(jobName);
    if (null == data) {
      LOG.error("No such job: " + jobName);
      return 1;
    }

    SqoopOptions childOpts = data.getSqoopOptions();
    SqoopTool childTool = data.getSqoopTool();

    // Don't overwrite the original SqoopOptions with the
    // arguments; make a child options.

    SqoopOptions clonedOpts = (SqoopOptions) childOpts.clone();
    clonedOpts.setParent(childOpts);

    String [] childArgv = childOpts.getExtraArgs();
    if (childArgv == null || childArgv.length == 0) {
        childArgv = new String[0];
    }

    int dashPos = getDashPosition(extraArguments);
    if (dashPos < extraArguments.length) {
        String[] extraArgs = Arrays.copyOfRange(extraArguments, dashPos + 1,
                extraArguments.length);
        if (childArgv == null || childArgv.length == 0) {
            childArgv = extraArgs;
        } else {
            // Find the second dash pos
            int dashPos2 = getDashPosition(extraArgs);
            if (dashPos2 >= extraArgs.length) {
                // if second dash is not present add it
                extraArgs = ArrayUtils.addAll(extraArgs, DASH_STR);
            }
            childArgv = ArrayUtils.addAll(extraArgs, childArgv);
        }
    }

    int confRet = configureChildTool(clonedOpts, childTool, childArgv);
    if (0 != confRet) {
        // Error.
        return confRet;
    }

      return childTool.run(clonedOpts);
  }

  private int showJob(SqoopOptions opts) throws IOException {
    JobData data = this.storage.read(jobName);
    if (null == data) {
      LOG.error("No such job: " + jobName);
      return 1;
    }

    SqoopOptions childOpts = data.getSqoopOptions();
    SqoopTool childTool = data.getSqoopTool();

    System.out.println("Job: " + jobName);
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
    // Get a JobStorage instance to use to materialize this job.
    JobStorageFactory ssf = new JobStorageFactory(options.getConf());
    this.storage = ssf.getJobStorage(storageDescriptor);
    if (null == this.storage) {
      LOG.error("There is no JobStorage implementation available");
      LOG.error("that can read your specified storage descriptor.");
      LOG.error("Don't know where to save this job info! You may");
      LOG.error("need to specify the connect string with --meta-connect.");
      return 1;
    }

    try {
      // Open the storage layer.
      this.storage.open(this.storageDescriptor);

      // And now determine what operation to perform with it.
      switch (operation) {
      case JobCreate:
        return createJob(options);
      case JobDelete:
        return deleteJob(options);
      case JobExecute:
        return execJob(options);
      case JobList:
        return listJobs(options);
      case JobShow:
        return showJob(options);
      default:
        LOG.error("Undefined job operation: " + operation);
        return 1;
      }
    } catch (IOException ioe) {
      LOG.error("I/O error performing job operation: "
          + StringUtils.stringifyException(ioe));
      return 1;
    } finally {
      if (null != this.storage) {
        try {
          storage.close();
        } catch (IOException ioe) {
          LOG.warn("IOException closing JobStorage: "
              + StringUtils.stringifyException(ioe));
        }
      }
    }
  }

  @Override
  /** Configure the command-line arguments we expect to receive */
  public void configureOptions(ToolOptions toolOptions) {
    toolOptions.addUniqueOptions(getJobOptions());
  }

  @Override
  /** {@inheritDoc} */
  public void applyOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {

    if (in.hasOption(VERBOSE_ARG)) {
      LoggingUtils.setDebugLevel();
      LOG.debug("Enabled debug logging.");
    }

    if (in.hasOption(HELP_ARG)) {
      ToolOptions toolOpts = new ToolOptions();
      configureOptions(toolOpts);
      printHelp(toolOpts);
      throw new InvalidOptionsException("");
    }

    this.storageDescriptor = new TreeMap<String, String>();

    applyMetastoreOptions(in, out);
    // These are generated via an option group; exactly one
    // of this exhaustive list will always be selected.
    if (in.hasOption(JOB_CMD_CREATE_ARG)) {
      this.operation = JobOp.JobCreate;
      this.jobName = in.getOptionValue(JOB_CMD_CREATE_ARG);
    } else if (in.hasOption(JOB_CMD_DELETE_ARG)) {
      this.operation = JobOp.JobDelete;
      this.jobName = in.getOptionValue(JOB_CMD_DELETE_ARG);
    } else if (in.hasOption(JOB_CMD_EXEC_ARG)) {
      this.operation = JobOp.JobExecute;
      this.jobName = in.getOptionValue(JOB_CMD_EXEC_ARG);
    } else if (in.hasOption(JOB_CMD_LIST_ARG)) {
      this.operation = JobOp.JobList;
    } else if (in.hasOption(JOB_CMD_SHOW_ARG)) {
      this.operation = JobOp.JobShow;
      this.jobName = in.getOptionValue(JOB_CMD_SHOW_ARG);
    }
  }

  private void applyMetastoreOptions(CommandLine in, SqoopOptions out) throws InvalidOptionsException {
    String metaConnectString;
    String metaUsernameString;
    String metaPasswordString;
    if (in.hasOption(STORAGE_METASTORE_ARG)) {
      metaConnectString = in.getOptionValue(STORAGE_METASTORE_ARG);
      this.storageDescriptor.put(GenericJobStorage.META_DRIVER_KEY, chooseDriverType(metaConnectString));
      this.storageDescriptor.put(GenericJobStorage.META_CONNECT_KEY, metaConnectString);
    } else {
      metaConnectString = out.getMetaConnectStr();
      this.storageDescriptor.put(GenericJobStorage.META_DRIVER_KEY, chooseDriverType(metaConnectString));
      this.storageDescriptor.put(GenericJobStorage.META_CONNECT_KEY, metaConnectString);
    }
    if (in.hasOption(METASTORE_USER_ARG)) {
      metaUsernameString = in.getOptionValue(METASTORE_USER_ARG);
      this.storageDescriptor.put(GenericJobStorage.META_USERNAME_KEY, metaUsernameString);
    } else {
      metaUsernameString = out.getMetaUsername();
      this.storageDescriptor.put(GenericJobStorage.META_USERNAME_KEY, metaUsernameString);
    }
    if (in.hasOption(METASTORE_PASS_ARG)) {
      metaPasswordString = in.getOptionValue(METASTORE_PASS_ARG);
      this.storageDescriptor.put(GenericJobStorage.META_PASSWORD_KEY, metaPasswordString);
    } else {
      metaPasswordString = out.getMetaPassword();
      this.storageDescriptor.put(GenericJobStorage.META_PASSWORD_KEY, metaPasswordString);
    }
  }

  private String chooseDriverType(String metaConnectString) throws InvalidOptionsException {
    for (JdbcDrivers driver : SUPPORTED_DRIVERS) {
      if (metaConnectString.startsWith(driver.getSchemePrefix())) {
        return driver.getDriverClass();
      }
    }
    throw new InvalidOptionsException("current meta-connect scheme not compatible with metastore");
  }

  @Override
  /** {@inheritDoc} */
  public void validateOptions(SqoopOptions options)
      throws InvalidOptionsException {

    if (null == operation
        || (null == this.jobName && operation != JobOp.JobList)) {
      throw new InvalidOptionsException("No job operation specified"
          + HELP_STR);
    }

    if (operation == JobOp.JobCreate) {
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
        + " [GENERIC-ARGS] [JOB-ARGS] [-- [<tool-name>] [TOOL-ARGS]]");
    System.out.println("");

    opts.printHelp();

    System.out.println("");
    System.out.println("Generic Hadoop command-line arguments:");
    System.out.println("(must preceed any tool-specific arguments)");
    ToolRunner.printGenericCommandUsage(System.out);
  }
}

