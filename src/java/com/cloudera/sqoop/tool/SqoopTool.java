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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;
import com.cloudera.sqoop.cli.SqoopParser;
import com.cloudera.sqoop.cli.ToolOptions;
import com.cloudera.sqoop.shims.ShimLoader;

/**
 * Base class for Sqoop subprograms (e.g., SqoopImport, SqoopExport, etc.)
 * Allows subprograms to configure the arguments they accept and
 * provides an entry-point to the subprogram.
 */
public abstract class SqoopTool {

  public static final Log LOG = LogFactory.getLog(SqoopTool.class.getName());

  private static final Map<String, Class<? extends SqoopTool>> TOOLS;
  private static final Map<String, String> DESCRIPTIONS;

  static {
    // All SqoopTool instances should be registered here so that
    // they can be found internally.
    TOOLS = new TreeMap<String, Class<? extends SqoopTool>>();
    DESCRIPTIONS = new TreeMap<String, String>();

    registerTool("codegen", CodeGenTool.class,
        "Generate code to interact with database records");
    registerTool("create-hive-table", CreateHiveTableTool.class,
        "Import a table definition into Hive");
    registerTool("eval", EvalSqlTool.class,
        "Evaluate a SQL statement and display the results");
    registerTool("export", ExportTool.class,
        "Export an HDFS directory to a database table");
    registerTool("import", ImportTool.class,
        "Import a table from a database to HDFS");
    registerTool("import-all-tables", ImportAllTablesTool.class,
        "Import tables from a database to HDFS");
    registerTool("help", HelpTool.class, "List available commands");
    registerTool("list-databases", ListDatabasesTool.class,
        "List available databases on a server");
    registerTool("list-tables", ListTablesTool.class,
        "List available tables in a database");
    registerTool("merge", MergeTool.class,
        "Merge results of incremental imports");
    registerTool("metastore", MetastoreTool.class,
        "Run a standalone Sqoop metastore");
    registerTool("job", JobTool.class,
        "Work with saved jobs");
    registerTool("version", VersionTool.class,
        "Display version information");
  }

  /**
   * Add a tool to the available set of SqoopTool instances.
   * @param toolName the name the user access the tool through.
   * @param cls the class providing the tool.
   * @param description a user-friendly description of the tool's function.
   */
  private static void registerTool(String toolName,
      Class<? extends SqoopTool> cls, String description) {
    TOOLS.put(toolName, cls);
    DESCRIPTIONS.put(toolName, description);
  }

  /**
   * @return the list of available tools.
   */
  public static final Set<String> getToolNames() {
    return TOOLS.keySet();
  }

  /**
   * @return the SqoopTool instance with the provided name, or null
   * if no such tool exists.
   */
  public static final SqoopTool getTool(String toolName) {
    Class<? extends SqoopTool> cls = TOOLS.get(toolName);
    try {
      if (null != cls) {
        SqoopTool tool = cls.newInstance();
        tool.setToolName(toolName);
        return tool;
      }
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      return null;
    }

    return null;
  }

  /**
   * @return the user-friendly description for a tool, or null if the tool
   * cannot be found.
   */
  public static final String getToolDescription(String toolName) {
    return DESCRIPTIONS.get(toolName);
  }

  /** The name of the current tool. */
  private String toolName;

  /** Arguments that remained unparsed after parseArguments. */
  protected String [] extraArguments;

  public SqoopTool() {
    this.toolName = "<" + this.getClass().getName() + ">";
  }

  public SqoopTool(String name) {
    this.toolName = name;
  }

  public String getToolName() {
    return this.toolName;
  }

  protected void setToolName(String name) {
    this.toolName = name;
  }

  /**
   * Main body of code to run the tool.
   * @param options the SqoopOptions configured via
   * configureOptions()/applyOptions().
   * @return an integer return code for external programs to consume. 0
   * represents success; nonzero means failure.
   */  
  public abstract int run(SqoopOptions options);

  /**
   * Configure the command-line arguments we expect to receive.
   * @param opts a ToolOptions that should be populated with sets of
   * RelatedOptions for the tool.
   */
  public void configureOptions(ToolOptions opts) {
    // Default implementation does nothing.
  }

  /**
   * Print the help message for this tool.
   * @param opts the configured tool options
   */
  public void printHelp(ToolOptions opts) {
    System.out.println("usage: sqoop " + getToolName()
        + " [GENERIC-ARGS] [TOOL-ARGS]");
    System.out.println("");

    opts.printHelp();

    System.out.println("");
    System.out.println("Generic Hadoop command-line arguments:");
    System.out.println("(must preceed any tool-specific arguments)");
    ToolRunner.printGenericCommandUsage(System.out);
  }

  /** Generate the SqoopOptions containing actual argument values from
   * the extracted CommandLine arguments.
   * @param in the CLI CommandLine that contain the user's set Options.
   * @param out the SqoopOptions with all fields applied.
   * @throws InvalidOptionsException if there's a problem.
   */
  public void applyOptions(CommandLine in, SqoopOptions out)
      throws InvalidOptionsException {
    // Default implementation does nothing.
  }

  /**
   * Validates options and ensures that any required options are
   * present and that any mutually-exclusive options are not selected.
   * @throws InvalidOptionsException if there's a problem.
   */
  public void validateOptions(SqoopOptions options)
      throws InvalidOptionsException {
    // Default implementation does nothing.
  }

  /**
   * Configures a SqoopOptions according to the specified arguments.
   * Reads a set of arguments and uses them to configure a SqoopOptions
   * and its embedded configuration (i.e., through GenericOptionsParser.)
   * Stores any unparsed arguments in the extraArguments field.
   *
   * @param args the arguments to parse.
   * @param conf if non-null, set as the configuration for the returned
   * SqoopOptions.
   * @param in a (perhaps partially-configured) SqoopOptions. If null,
   * then a new SqoopOptions will be used. If this has a null configuration
   * and conf is null, then a new Configuration will be inserted in this.
   * @param useGenericOptions if true, will also parse generic Hadoop
   * options into the Configuration.
   * @return a SqoopOptions that is fully configured by a given tool.
   */
  public SqoopOptions parseArguments(String [] args,
      Configuration conf, SqoopOptions in, boolean useGenericOptions)
      throws ParseException, SqoopOptions.InvalidOptionsException {
    SqoopOptions out = in;

    if (null == out) {
      out = new SqoopOptions();
    }

    if (null != conf) {
      // User specified a configuration; use it and override any conf
      // that may have been in the SqoopOptions.
      out.setConf(conf);
    } else if (null == out.getConf()) {
      // User did not specify a configuration, but neither did the
      // SqoopOptions. Fabricate a new one.
      out.setConf(new Configuration());
    }

    String [] toolArgs = args; // args after generic parser is done.
    if (useGenericOptions) {
      try {
        toolArgs = ShimLoader.getHadoopShim().parseGenericOptions(
            out.getConf(), args);
      } catch (IOException ioe) {
        ParseException pe = new ParseException(
            "Could not parse generic arguments");
        pe.initCause(ioe);
        throw pe;
      }
    }

    // Parse tool-specific arguments.
    ToolOptions toolOptions = new ToolOptions();
    configureOptions(toolOptions);
    CommandLineParser parser = new SqoopParser();
    CommandLine cmdLine = parser.parse(toolOptions.merge(), toolArgs, true);
    applyOptions(cmdLine, out);
    this.extraArguments = cmdLine.getArgs();
    return out;
  }

  /**
   * Append 'extra' to extraArguments.
   */
  public void appendArgs(String [] extra) {
    int existingLen =
        (this.extraArguments == null) ? 0 : this.extraArguments.length;
    int newLen = (extra == null) ? 0 : extra.length;
    String [] newExtra = new String[existingLen + newLen];

    if (null != this.extraArguments) {
      System.arraycopy(this.extraArguments, 0, newExtra, 0, existingLen);
    }

    if (null != extra) {
      System.arraycopy(extra, 0, newExtra, existingLen, newLen);
    }

    this.extraArguments = newExtra;
  }

  @Override
  public String toString() {
    return getToolName();
  }
}

