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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;

import jline.console.ConsoleReader;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.InputEditable;
import org.apache.sqoop.model.MBooleanInput;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDateTimeInput;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MEnumInput;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MIntegerInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MListInput;
import org.apache.sqoop.model.MLongInput;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.model.MValidator;
import org.apache.sqoop.shell.core.Constants;
import org.apache.sqoop.shell.core.ShellError;
import org.apache.sqoop.utils.MapResourceBundle;
import org.apache.sqoop.validation.Status;
import org.codehaus.groovy.tools.shell.Groovysh;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TestUpdateCommand {
  UpdateCommand updateCmd;
  SqoopClient client;
  ConsoleReader reader;
  ResourceBundle resourceBundle;
  ByteArrayInputStream in;
  byte[] data;

  @BeforeTest(alwaysRun = true)
  public void setup() throws IOException {
    Groovysh shell = new Groovysh();
    updateCmd = new UpdateCommand(shell);
    ShellEnvironment.setIo(shell.getIo());
    client = mock(SqoopClient.class);
    ShellEnvironment.setClient(client);

    data = new byte[1000];
    in = new ByteArrayInputStream(data);
    reader = new ConsoleReader(in, System.out);
    ShellEnvironment.setConsoleReader(reader);
    resourceBundle = new ResourceBundle() {
      @Override
      protected Object handleGetObject(String key) {
        return "fake_translated_value";
      }

      @Override
      public Enumeration<String> getKeys() {
        return Collections.emptyEnumeration();
      }
    };
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testUpdateLink() throws InterruptedException {
    ShellEnvironment.setInteractive(false);
    MLink link = new MLink(1L, new MLinkConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()));
    when(client.getLink("link_test")).thenReturn(link);
    when(client.getConnectorConfigBundle(1L)).thenReturn(new MapResourceBundle(new HashMap()));
    when(client.updateLink(link)).thenReturn(Status.OK);

    // update link -lid link_test
    Status status = (Status) updateCmd.execute(Arrays.asList(Constants.FN_LINK, "-lid", "link_test"));
    assertTrue(status != null && status == Status.OK);

    // Missing argument for option lid
    try {
      updateCmd.execute(Arrays.asList(Constants.FN_LINK, "-lid"));
      fail("Update link should fail as parameters aren't complete!");
    } catch (SqoopException e) {
      assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      assertTrue(e.getMessage().contains("Missing argument for option"));
    }

    // Missing option lid
    try {
      updateCmd.execute(Arrays.asList(Constants.FN_LINK));
      fail("Update link should fail as option lid is missing");
    } catch (SqoopException e) {
      assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      assertTrue(e.getMessage().contains("Missing required option"));
    }
  }

  @Test
  public void testUpdateLinkInteractive() {
    ShellEnvironment.setInteractive(true);
    initEnv();
    when(client.getConnector("connector_test")).thenReturn(new MConnector("", "", "", null, null, null));
    MLink link = new MLink(1, new MLinkConfig(getConfig("CONFIGFROMNAME"), new ArrayList<MValidator>()));
    when(client.getLink("link_test")).thenReturn(link);
    when(client.updateLink(link)).thenReturn(Status.OK);
    when(client.getConnectorConfigBundle(any(Long.class))).thenReturn(resourceBundle);

    // update link -lid link_test
    initData("linkname\r" +         // link name
        "abc\r" +                   // for input with name "String"
        "12345\r" +                 // for input with name "Integer"
        "56789\r" +                 // for input with name "Long"
        "true\r" +                  // for input with name "Boolean"
        "k1=v1\rk2=v2\r\r" +        // for input with name "Map"
        "0\r" +                     // for input with name "Enum"
        "l1\rl2\rl3\r\r" +          // for input with name "List"
        "12345678\r");              // for input with name "DateTime"
    Status status = (Status) updateCmd.execute(Arrays.asList(Constants.FN_LINK, "-lid", "link_test"));
    assertTrue(status != null && status == Status.OK);
    assertEquals(link.getName(), "linkname");
    assertEquals(link.getConnectorLinkConfig("CONFIGFROMNAME").getStringInput("CONFIGFROMNAME.String").getValue(), "abc");
    assertEquals(link.getConnectorLinkConfig("CONFIGFROMNAME").getIntegerInput("CONFIGFROMNAME.Integer").getValue().intValue(), 12345);
    assertEquals(link.getConnectorLinkConfig("CONFIGFROMNAME").getLongInput("CONFIGFROMNAME.Long").getValue().longValue(), 56789);
    assertTrue((link.getConnectorLinkConfig("CONFIGFROMNAME").getBooleanInput("CONFIGFROMNAME.Boolean").getValue()));
    HashMap<String, String> map = new HashMap<String, String>();
    map.put("k1", "v1");
    map.put("k2", "v2");
    assertEquals(link.getConnectorLinkConfig("CONFIGFROMNAME").getMapInput("CONFIGFROMNAME.Map").getValue(), map);
    assertEquals(link.getConnectorLinkConfig("CONFIGFROMNAME").getEnumInput("CONFIGFROMNAME.Enum").getValue(), "YES");
    assertEquals(StringUtils.join(link.getConnectorLinkConfig("CONFIGFROMNAME").getListInput("CONFIGFROMNAME.List").getValue(), "&"), "l1&l2&l3");
    assertEquals(link.getConnectorLinkConfig("CONFIGFROMNAME").getDateTimeInput("CONFIGFROMNAME.DateTime").getValue().getMillis(), 12345678);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testUpdateJob() throws InterruptedException {
    ShellEnvironment.setInteractive(false);
    MJob job = new MJob(1L, 2L, 1L, 2L,
        new MFromConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
        new MToConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()),
        new MDriverConfig(new ArrayList<MConfig>(), new ArrayList<MValidator>()));
    when(client.getJob("job_test")).thenReturn(job);
    when(client.getConnectorConfigBundle(any(Long.class))).thenReturn(new MapResourceBundle(new HashMap()));
    when(client.getDriverConfigBundle()).thenReturn(new MapResourceBundle(new HashMap()));
    when(client.updateJob(job)).thenReturn(Status.OK);

    // update job -jid job_test
    Status status = (Status) updateCmd.execute(Arrays.asList(Constants.FN_JOB, "-jid", "job_test"));
    assertTrue(status != null && status == Status.OK);

    // Missing argument for option jid
    try {
      updateCmd.execute(Arrays.asList(Constants.FN_JOB, "-jid"));
      fail("Update job should fail as parameters aren't complete!");
    } catch (SqoopException e) {
      assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      assertTrue(e.getMessage().contains("Missing argument for option"));
    }

    // Missing option jid
    try {
      updateCmd.execute(Arrays.asList(Constants.FN_JOB));
      fail("Update job should fail as option jid is missing");
    } catch (SqoopException e) {
      assertEquals(ShellError.SHELL_0003, e.getErrorCode());
      assertTrue(e.getMessage().contains("Missing required option"));
    }
  }

  @Test
  public void testUpdateJobInteractive() {
    ShellEnvironment.setInteractive(true);
    initEnv();
    MJob job = new MJob(1, 2, 1, 2, new MFromConfig(getConfig("fromJobConfig"), new ArrayList<MValidator>()),
        new MToConfig(getConfig("toJobConfig"), new ArrayList<MValidator>()),
        new MDriverConfig(getConfig("driverConfig"), new ArrayList<MValidator>()));
    when(client.getJob("job_test")).thenReturn(job);
    when(client.getConnectorConfigBundle(any(Long.class))).thenReturn(resourceBundle);
    when(client.getDriverConfigBundle()).thenReturn(resourceBundle);
    when(client.updateJob(job)).thenReturn(Status.OK);

    // update job -jid job_test
    initData("jobname\r" +          // job name
        // From job config
        "abc\r" +                   // for input with name "String"
        "12345\r" +                 // for input with name "Integer"
        "56789\r" +                 // for input with name "Long"
        "true\r" +                  // for input with name "Boolean"
        "k1=v1\rk2=v2\r\r" +        // for input with name "Map"
        "0\r" +                     // for input with name "Enum"
        "l1\rl2\rl3\r\r" +          // for input with name "List"
        "12345678\r" +              // for input with name "DateTime"

        // To job config
        "def\r" +                   // for input with name "String"
        "11111\r" +                 // for input with name "Integer"
        "22222\r" +                 // for input with name "Long"
        "false\r" +                  // for input with name "Boolean"
        "k3=v3\rk4=v4\r\r" +        // for input with name "Map"
        "1\r" +                     // for input with name "Enum"
        "l4\rl5\rl6\r\r" +          // for input with name "List"
        "1234567\r" +              // for input with name "DateTime"

        // Driver config
        "hij\r" +                   // for input with name "String"
        "33333\r" +                 // for input with name "Integer"
        "44444\r" +                 // for input with name "Long"
        "true\r" +                  // for input with name "Boolean"
        "k1=v1\r\r" +               // for input with name "Map"
        "0\r" +                     // for input with name "Enum"
        "l1\rl2\rl3\r\r" +          // for input with name "List"
        "7654321\r");              // for input with name "DateTime"
    Status status = (Status) updateCmd.execute(Arrays.asList(Constants.FN_JOB, "-jid", "job_test"));
    assertTrue(status != null && status == Status.OK);
    assertEquals(job.getName(), "jobname");
    // check from job config
    assertEquals(job.getFromJobConfig().getStringInput("fromJobConfig.String").getValue(), "abc");
    assertEquals(job.getFromJobConfig().getIntegerInput("fromJobConfig.Integer").getValue().intValue(), 12345);
    assertEquals((job.getFromJobConfig().getLongInput("fromJobConfig.Long").getValue()).longValue(), 56789);
    assertTrue((job.getFromJobConfig().getBooleanInput("fromJobConfig.Boolean").getValue()));
    HashMap<String, String> map = new HashMap<String, String>();
    map.put("k1", "v1");
    map.put("k2", "v2");
    assertEquals(job.getFromJobConfig().getMapInput("fromJobConfig.Map").getValue(), map);
    assertEquals(job.getFromJobConfig().getEnumInput("fromJobConfig.Enum").getValue(), "YES");
    assertEquals(StringUtils.join(job.getFromJobConfig().getListInput("fromJobConfig.List").getValue(), "&"), "l1&l2&l3");
    assertEquals(job.getFromJobConfig().getDateTimeInput("fromJobConfig.DateTime").getValue().getMillis(), 12345678);

    // check to job config
    assertEquals(job.getToJobConfig().getStringInput("toJobConfig.String").getValue(), "def");
    assertEquals(job.getToJobConfig().getIntegerInput("toJobConfig.Integer").getValue().intValue(), 11111);
    assertEquals(job.getToJobConfig().getLongInput("toJobConfig.Long").getValue().longValue(), 22222);
    assertFalse(job.getToJobConfig().getBooleanInput("toJobConfig.Boolean").getValue());
    map = new HashMap<String, String>();
    map.put("k3", "v3");
    map.put("k4", "v4");
    assertEquals(job.getToJobConfig().getMapInput("toJobConfig.Map").getValue(), map);
    assertEquals(job.getToJobConfig().getEnumInput("toJobConfig.Enum").getValue(), "NO");
    assertEquals(StringUtils.join(job.getToJobConfig().getListInput("toJobConfig.List").getValue(), "&"), "l4&l5&l6");
    assertEquals(job.getToJobConfig().getDateTimeInput("toJobConfig.DateTime").getValue().getMillis(), 1234567);

    // check driver config
    assertEquals(job.getDriverConfig().getStringInput("driverConfig.String").getValue(), "hij");
    assertEquals(job.getDriverConfig().getIntegerInput("driverConfig.Integer").getValue().intValue(), 33333);
    assertEquals(job.getDriverConfig().getLongInput("driverConfig.Long").getValue().longValue(), 44444);
    assertTrue(job.getDriverConfig().getBooleanInput("driverConfig.Boolean").getValue());
    map = new HashMap<String, String>();
    map.put("k1", "v1");
    assertEquals(job.getDriverConfig().getMapInput("driverConfig.Map").getValue(), map);
    assertEquals(job.getDriverConfig().getEnumInput("driverConfig.Enum").getValue(), "YES");
    assertEquals(StringUtils.join(job.getDriverConfig().getListInput("driverConfig.List").getValue(), "&"), "l1&l2&l3");
    assertEquals(job.getDriverConfig().getDateTimeInput("driverConfig.DateTime").getValue().getMillis(), 7654321);
  }

  @SuppressWarnings("unchecked")
  private List<MConfig> getConfig(String configName) {
    List<MInput<?>> list = new ArrayList<MInput<?>>();
    list.add(new MStringInput(configName + "." + "String", false, InputEditable.ANY, StringUtils.EMPTY, (short)30, Collections.EMPTY_LIST));
    list.add(new MIntegerInput(configName + "." + "Integer", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST));
    list.add(new MLongInput(configName + "." + "Long", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST));
    list.add(new MBooleanInput(configName + "." + "Boolean", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST));
    list.add(new MMapInput(configName + "." + "Map", false, InputEditable.ANY, StringUtils.EMPTY, StringUtils.EMPTY, Collections.EMPTY_LIST));
    list.add(new MEnumInput(configName + "." + "Enum", false, InputEditable.ANY, StringUtils.EMPTY, new String[] {"YES", "NO"}, Collections.EMPTY_LIST));
    list.add(new MListInput(configName + "." + "List", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST));
    list.add(new MDateTimeInput(configName + "." + "DateTime", false, InputEditable.ANY, StringUtils.EMPTY, Collections.EMPTY_LIST));

    List<MConfig> configs = new ArrayList<MConfig>();
    configs.add(new MConfig(configName, list, Collections.EMPTY_LIST));
    return configs;
  }

  private void initData(String destData) {
    byte[] destDataBytes = destData.getBytes();
    System.arraycopy(destDataBytes, 0, data, 0, destDataBytes.length);
    in.reset();
  }

  private void initEnv() {
    in.reset();
    for (int i = 0; i < data.length; i++) {
      data[i] = '\0';
    }
  }
}
