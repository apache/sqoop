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
package org.apache.sqoop.driver;

import java.util.Locale;
import java.util.ResourceBundle;

import org.apache.log4j.Logger;
import org.apache.sqoop.connector.spi.RepositoryUpgrader;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.core.Reconfigurable;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.core.SqoopConfiguration.CoreConfigurationListener;
import org.apache.sqoop.driver.configuration.JobConfiguration;
import org.apache.sqoop.driver.configuration.LinkConfiguration;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.validation.Validator;

/**
 * Sqoop driver that manages the job lifecyle
 *
 * All Sqoop internals are handled in this class:
 * * Submission engine
 * * Execution engine
 * * Driver config
 *
 * Current implementation of entire submission engine is using repository
 * for keeping track of running submissions. Thus, server might be restarted at
 * any time without any affect on running jobs. This approach however might not
 * be the fastest way and we might want to introduce internal structures for
 * running jobs in case that this approach will be too slow.
 */
public class Driver implements Reconfigurable {

  /**
   * Logger object.
   */
  private static final Logger LOG = Logger.getLogger(Driver.class);

  /**
   * Private instance to singleton of this class.
   */
  private static Driver instance;

  /**
   * Create default object by default.
   *
   * Every Sqoop server application needs one so this should not be performance issue.
   */
  static {
    instance = new Driver();
  }

  /**
   * Return current instance.
   *
   * @return Current instance
   */
  public static Driver getInstance() {
    return instance;
  }

  /**
   * Allows to set instance in case that it's need.
   *
   * This method should not be normally used as the default instance should be sufficient. One target
   * user use case for this method are unit tests.
   *
   * @param newInstance New instance
   */
  public static void setInstance(Driver newInstance) {
    instance = newInstance;
  }

  /**
   * Driver config structure
   */
  private MDriverConfig mDriverConfig;

  /**
   * Validator instance
   */
  private final Validator validator;

  /**
   * Driver config upgrader instance
   */
  private final RepositoryUpgrader driverConfigUpgrader;

  /**
   * Default driver config auto upgrade option value
   */
  private static final boolean DEFAULT_AUTO_UPGRADE = false;

  public static final String CURRENT_DRIVER_VERSION = "1";

  public Class getJobConfigurationClass() {
      return JobConfiguration.class;
  }

  public Class getLinkConfigurationClass() {
      return LinkConfiguration.class;
  }

  public Driver() {
    MConnectionForms connectionForms = new MConnectionForms(
      FormUtils.toForms(getLinkConfigurationClass())
    );
    mDriverConfig = new MDriverConfig(connectionForms, new MJobForms(FormUtils.toForms(getJobConfigurationClass())),
        CURRENT_DRIVER_VERSION);

    // Build validator
    validator = new DriverValidator();
    // Build upgrader
    driverConfigUpgrader = new DriverConfigUpgrader();
  }

  public synchronized void initialize() {
    initialize(SqoopConfiguration.getInstance().getContext().getBoolean(ConfigurationConstants.DRIVER_AUTO_UPGRADE, DEFAULT_AUTO_UPGRADE));
  }

  public synchronized void initialize(boolean autoUpgrade) {
    LOG.trace("Begin Driver Config initialization");

    // Register driver config in repository
    mDriverConfig = RepositoryManager.getInstance().getRepository().registerDriverConfig(mDriverConfig, autoUpgrade);

    SqoopConfiguration.getInstance().getProvider().registerListener(new CoreConfigurationListener(this));

    LOG.info("Driver Config initialized: OK");
  }

  public  synchronized void destroy() {
    LOG.trace("Begin Driver Config destroy");
  }

  public Validator getValidator() {
    return validator;
  }

  public RepositoryUpgrader getDriverConfigRepositoryUpgrader() {
    return driverConfigUpgrader;
  }

  public MDriverConfig getDriverConfig() {
    return mDriverConfig;
  }

  public ResourceBundle getBundle(Locale locale) {
    return ResourceBundle.getBundle(DriverConstants.DRIVER_CONFIG_BUNDLE, locale);
  }

  @Override
  public void configurationChanged() {
    LOG.info("Begin Driver reconfiguring");
    // If there are configuration options for Driver,
    // implement the reconfiguration procedure right here.
    LOG.info("Driver reconfigured");
  }
}
