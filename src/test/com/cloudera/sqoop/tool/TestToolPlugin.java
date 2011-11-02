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

package com.cloudera.sqoop.tool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.StringUtils;

import com.cloudera.sqoop.Sqoop;
import com.cloudera.sqoop.SqoopOptions;

import com.cloudera.sqoop.SqoopOptions.InvalidOptionsException;

import com.cloudera.sqoop.cli.ToolOptions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import junit.framework.TestCase;

/**
 * Test that tool plugins work.
 */
public class TestToolPlugin extends TestCase {

  public static final Log LOG = LogFactory
      .getLog(TestToolPlugin.class.getName());

  /**
   * The plugin that registers the tool.
   */
  public static class PluginClass extends ToolPlugin {
    public List<ToolDesc> getTools() {
      return Collections.singletonList(new ToolDesc("fooTool",
          FooTool.class, "does foo things"));
    }
  }

  /**
   * The dummy tool itself.
   */
  public static class FooTool extends BaseSqoopTool {
    /** Holds the name of the last user we "operated" as.  */
    private static String lastUser;
    static String getLastUser() {
      return lastUser;
    }

    private static void setLastUser(String last) {
      lastUser = last;
    }

    public FooTool() {
      super("fooTool");
    }

    /** Just save the username and call it a day. */
    @Override
    public int run(SqoopOptions opts) {
      setLastUser(opts.getUsername());
      LOG.info("FooTool operating on user: " + lastUser);
      return 0;
    }

    @Override
    public void configureOptions(ToolOptions toolOptions) {
      toolOptions.addUniqueOptions(getCommonOptions());
    }

    @Override
    public void applyOptions(CommandLine in, SqoopOptions out)
        throws InvalidOptionsException {
      applyCommonOptions(in, out);
    }

    @Override
    public void validateOptions(SqoopOptions options)
        throws InvalidOptionsException {
      validateCommonOptions(options);
    }
  }

  public void testPlugin() {
    // Register the plugin with SqoopTool.
    Configuration pluginConf = new Configuration();
    pluginConf.set(SqoopTool.TOOL_PLUGINS_KEY, PluginClass.class.getName());
    SqoopTool.loadPlugins(pluginConf);

    ArrayList<String> args = new ArrayList<String>();
    args.add("fooTool");
    args.add("--username");
    args.add("bob");
    args.add("--connect");
    args.add("anywhere");

    int ret = Sqoop.runTool(args.toArray(new String[0]));
    assertEquals("Expected tool run success", 0, ret);

    String actualUser = FooTool.getLastUser();
    assertEquals("Failed to set username correctly.", "bob", actualUser);
  }

  /**
   * Plugin class that tries to override an existing tool definition.
   */
  public static class OverridePlugin extends ToolPlugin {
    public List<ToolDesc> getTools() {
      return Collections.singletonList(new ToolDesc("import",
          FooTool.class, "replaces 'import' with foo"));
    }
  }

  public void testNoOverrideTools() {
    // Test that you can't override an existing tool definition. First
    // registration of a tool name wins.
    Configuration pluginConf = new Configuration();
    pluginConf.set(SqoopTool.TOOL_PLUGINS_KEY, OverridePlugin.class.getName());
    try {
      SqoopTool.loadPlugins(pluginConf);
      fail("Successfully loaded a plugin that overrides 'import' tool.");
    } catch (RuntimeException re) {
      LOG.info("Got runtime exception registering plugin (expected; ok): "
          + StringUtils.stringifyException(re));
    }
  }
}
