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

package org.apache.sqoop;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.cli.ToolOptions;
import com.cloudera.sqoop.tool.SqoopTool;
import com.cloudera.sqoop.util.OptionsFileUtil;

/**
 * Main entry-point for Sqoop
 * Usage: hadoop jar (this_jar_name) com.cloudera.sqoop.Sqoop (options)
 * See the SqoopOptions class for options.
 */
public class Sqoop extends Configured implements Tool {

  public static final Log SQOOP_LOG = LogFactory.getLog("com.cloudera.sqoop");
  public static final Log LOG = LogFactory.getLog(Sqoop.class.getName());

  /**
   * If this System property is set, always throw an exception, do not just
   * exit with status 1.
   */
  public static final String SQOOP_RETHROW_PROPERTY = "sqoop.throwOnError";

  /**
   * The option to specify an options file from which other options to the
   * tool are read.
   */
  public static final String SQOOP_OPTIONS_FILE_SPECIFIER = "--options-file";

  static {
    Configuration.addDefaultResource("sqoop-site.xml");
  }

  private SqoopTool tool;
  private SqoopOptions options;
  private String [] childPrgmArgs;

  /**
   * Creates a new instance of Sqoop set to run the supplied SqoopTool
   * with the default configuration.
   * @param tool the SqoopTool to run in the main body of Sqoop.
   */
  public Sqoop(SqoopTool tool) {
    this(tool, (Configuration) null);
  }

  /**
   * Creates a new instance of Sqoop set to run the supplied SqoopTool
   * with the provided configuration.
   * @param tool the SqoopTool to run in the main body of Sqoop.
   * @param conf the Configuration to use (e.g., from ToolRunner).
   */
  public Sqoop(SqoopTool tool, Configuration conf) {
    this(tool, conf, new SqoopOptions());
  }

  /**
   * Creates a new instance of Sqoop set to run the supplied SqoopTool
   * with the provided configuration and SqoopOptions.
   * @param tool the SqoopTool to run in the main body of Sqoop.
   * @param conf the Configuration to use (e.g., from ToolRunner).
   * @param opts the SqoopOptions which control the tool's parameters.
   */
  public Sqoop(SqoopTool tool, Configuration conf, SqoopOptions opts) {
    LOG.info("Running Sqoop version: " + new SqoopVersion().VERSION);

    if (null != conf) {
      setConf(conf);
    }

    this.options = opts;
    this.options.setConf(getConf());

    this.tool = tool;
  }

  /**
   * @return the SqoopOptions used in this Sqoop instance.
   */
  public SqoopOptions getOptions() {
    return this.options;
  }

  /**
   * @return the SqoopTool used in this Sqoop instance.
   */
  public SqoopTool getTool() {
    return this.tool;
  }

  @Override
  /**
   * Actual main entry-point for the program
   */
  public int run(String [] args) {
    if (options.getConf() == null) {
      // Configuration wasn't initialized until after the ToolRunner
      // got us to this point. ToolRunner gave Sqoop itself a Conf
      // though.
      options.setConf(getConf());
    }

    try {
      options = tool.parseArguments(args, null, options, false);
      tool.appendArgs(this.childPrgmArgs);
      tool.validateOptions(options);
    } catch (Exception e) {
      // Couldn't parse arguments.
      // Log the stack trace for this exception
      LOG.debug(e.getMessage(), e);
      // Print exception message.
      System.err.println(e.getMessage());
      return 1; // Exit on exception here.
    }

    return tool.run(options);
  }

  /**
   * SqoopTools sometimes pass arguments to a child program (e.g., mysqldump).
   * Users can specify additional args to these programs by preceeding the
   * additional arguments with a standalone '--'; but
   * ToolRunner/GenericOptionsParser will cull out this argument. We remove
   * the child-program arguments in advance, and store them to be readded
   * later.
   * @param argv the argv in to the SqoopTool
   * @return the argv with a "--" and any subsequent arguments removed.
   */
  private String [] stashChildPrgmArgs(String [] argv) {
    for (int i = 0; i < argv.length; i++) {
      if ("--".equals(argv[i])) {
        this.childPrgmArgs = Arrays.copyOfRange(argv, i, argv.length);
        return Arrays.copyOfRange(argv, 0, i);
      }
    }

    // Didn't find child-program arguments.
    return argv;
  }

  /**
   * Given a Sqoop object and a set of arguments to deliver to
   * its embedded SqoopTool, run the tool, wrapping the call to
   * ToolRunner.
   * This entry-point is preferred to ToolRunner.run() because
   * it has a chance to stash child program arguments before
   * GenericOptionsParser would remove them.
   */
  public static int runSqoop(Sqoop sqoop, String [] args) {
    try {
      String [] toolArgs = sqoop.stashChildPrgmArgs(args);
      return ToolRunner.run(sqoop.getConf(), sqoop, toolArgs);
    } catch (Exception e) {
      LOG.error("Got exception running Sqoop: " + e.toString());
      e.printStackTrace();
      if (System.getProperty(SQOOP_RETHROW_PROPERTY) != null) {
        throw new RuntimeException(e);
      }
      return 1;
    }

  }

  /**
   * Entry-point that parses the correct SqoopTool to use from the args,
   * but does not call System.exit() as main() will.
   */
  public static int runTool(String [] args, Configuration conf) {
    // Expand the options
    String[] expandedArgs = null;
    try {
      expandedArgs = OptionsFileUtil.expandArguments(args);
    } catch (Exception ex) {
      LOG.error("Error while expanding arguments", ex);
      System.err.println(ex.getMessage());
      System.err.println("Try 'sqoop help' for usage.");
      return 1;
    }

    String toolName = expandedArgs[0];
    Configuration pluginConf = SqoopTool.loadPlugins(conf);
    SqoopTool tool = SqoopTool.getTool(toolName);
    if (null == tool) {
      System.err.println("No such sqoop tool: " + toolName
          + ". See 'sqoop help'.");
      return 1;
    }


    Sqoop sqoop = new Sqoop(tool, pluginConf);
    return runSqoop(sqoop,
        Arrays.copyOfRange(expandedArgs, 1, expandedArgs.length));
  }

  /**
   * Entry-point that parses the correct SqoopTool to use from the args,
   * but does not call System.exit() as main() will.
   */
  public static int runTool(String [] args) {
    return runTool(args, new Configuration());
  }

  public static void main(String [] args) {
    if (args.length == 0) {
      System.err.println("Try 'sqoop help' for usage.");
      System.exit(1);
    }

    int ret = runTool(args);
    System.exit(ret);
  }
}

