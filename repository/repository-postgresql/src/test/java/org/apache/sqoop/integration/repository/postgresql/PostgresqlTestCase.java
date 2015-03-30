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
package org.apache.sqoop.integration.repository.postgresql;

import org.apache.commons.lang.StringUtils;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.test.db.DatabaseProvider;
import org.apache.sqoop.common.test.db.PostgreSQLProvider;
import org.apache.sqoop.json.DriverBean;
import org.apache.sqoop.model.*;
import org.apache.sqoop.repository.postgresql.PostgresqlRepositoryHandler;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Abstract class with convenience methods for testing postgresql repository.
 */
abstract public class PostgresqlTestCase {

  public static DatabaseProvider provider;
  public static PostgresqlTestUtils utils;
  public PostgresqlRepositoryHandler handler;

  @BeforeClass(alwaysRun = true)
  public void setUpClass() {
    provider = new PostgreSQLProvider();
    utils = new PostgresqlTestUtils(provider);
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    provider.start();

    handler = new PostgresqlRepositoryHandler();
    handler.createOrUpgradeRepository(provider.getConnection());
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() throws Exception {
    provider.dropSchema("SQOOP");
    provider.stop();
  }

  protected MConnector getConnector(String name, String className, String version, boolean from, boolean to) {
    return new MConnector(name, className, version, getLinkConfig(),
        from ? getFromConfig() : null, to ? getToConfig() : null);
  }

  protected MDriver getDriver() {
    return new MDriver(getDriverConfig(), DriverBean.CURRENT_DRIVER_VERSION);
  }

  protected MLink getLink(String name, MConnector connector) {
    MLink link = new MLink(connector.getPersistenceId(), connector.getLinkConfig());
    link.setName(name);
    fillLink(link);
    return link;
  }

  protected MJob getJob(String name, MConnector connectorA, MConnector connectorB, MLink linkA, MLink linkB) {
    MDriver driver = handler.findDriver(MDriver.DRIVER_NAME, provider.getConnection());
    MJob job = new MJob(
        connectorA.getPersistenceId(),
        connectorB.getPersistenceId(),
        linkA.getPersistenceId(),
        linkB.getPersistenceId(),
        connectorA.getFromConfig(),
        connectorB.getToConfig(),
        driver.getDriverConfig());
    job.setName(name);
    fillJob(job);

    return job;
  }

  protected MSubmission getSubmission(MJob job, SubmissionStatus submissionStatus) {
    MSubmission submission = new MSubmission(job.getPersistenceId(), new Date(), submissionStatus);
    fillSubmission(submission);
    return submission;
  }

  protected void fillLink(MLink link) {
    List<MConfig> configs = link.getConnectorLinkConfig().getConfigs();
    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("Value1");
    ((MStringInput) configs.get(1).getInputs().get(0)).setValue("Value2");
  }

  protected void fillJob(MJob job) {
    List<MConfig> configs = job.getFromJobConfig().getConfigs();
    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("Value1");
    ((MStringInput) configs.get(1).getInputs().get(0)).setValue("Value2");

    configs = job.getToJobConfig().getConfigs();
    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("Value1");
    ((MStringInput) configs.get(1).getInputs().get(0)).setValue("Value2");

    configs = job.getDriverConfig().getConfigs();
    ((MStringInput) configs.get(0).getInputs().get(0)).setValue("Value1");
    ((MStringInput) configs.get(1).getInputs().get(0)).setValue("Value2");
  }

  protected void fillSubmission(MSubmission submission) {
    Counters counters = new Counters();
    counters.addCounterGroup(new CounterGroup("test-1"));
    counters.addCounterGroup(new CounterGroup("test-2"));
    submission.setCounters(counters);
  }

  protected MLinkConfig getLinkConfig() {
    return new MLinkConfig(getConfigs("l1", "l2"));
  }

  protected MFromConfig getFromConfig() {
    return new MFromConfig(getConfigs("from1", "from2"));
  }

  protected MToConfig getToConfig() {
    return new MToConfig(getConfigs("to1", "to2"));
  }

  protected MDriverConfig getDriverConfig() {
    return new MDriverConfig(getConfigs("d1", "d2"));
  }

  protected List<MConfig> getConfigs(String configName1, String configName2) {
    List<MConfig> configs = new LinkedList<MConfig>();

    List<MInput<?>> inputs = new LinkedList<MInput<?>>();
    MInput<?> input = new MStringInput("I1", false, InputEditable.ANY, StringUtils.EMPTY, (short) 30);
    inputs.add(input);
    input = new MMapInput("I2", false, InputEditable.ANY, "I1");
    inputs.add(input);
    configs.add(new MConfig(configName1, inputs));

    inputs = new LinkedList<MInput<?>>();
    input = new MStringInput("I3", false, InputEditable.ANY, "I4", (short) 30);
    inputs.add(input);
    input = new MMapInput("I4", false, InputEditable.ANY, "I3");
    inputs.add(input);
    configs.add(new MConfig(configName2, inputs));

    return configs;
  }
}