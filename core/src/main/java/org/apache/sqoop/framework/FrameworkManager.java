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
package org.apache.sqoop.framework;

import org.apache.log4j.Logger;
import org.apache.sqoop.connector.spi.MetadataUpgrader;
import org.apache.sqoop.core.ConfigurationConstants;
import org.apache.sqoop.core.Reconfigurable;
import org.apache.sqoop.core.SqoopConfiguration;
import org.apache.sqoop.core.SqoopConfiguration.CoreConfigurationListener;
import org.apache.sqoop.framework.configuration.ConnectionConfiguration;
import org.apache.sqoop.framework.configuration.ExportJobConfiguration;
import org.apache.sqoop.framework.configuration.ImportJobConfiguration;
import org.apache.sqoop.model.*;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.validation.Validator;

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Manager for Sqoop framework itself.
 *
 * All Sqoop internals are handled in this class:
 * * Submission engine
 * * Execution engine
 * * Framework metadata
 *
 * Current implementation of entire submission engine is using repository
 * for keeping track of running submissions. Thus, server might be restarted at
 * any time without any affect on running jobs. This approach however might not
 * be the fastest way and we might want to introduce internal structures for
 * running jobs in case that this approach will be too slow.
 */
public class FrameworkManager implements Reconfigurable {

  /**
   * Logger object.
   */
  private static final Logger LOG = Logger.getLogger(FrameworkManager.class);

  /**
   * Private instance to singleton of this class.
   */
  private static FrameworkManager instance;

  /**
   * Create default object by default.
   *
   * Every Sqoop server application needs one so this should not be performance issue.
   */
  static {
    instance = new FrameworkManager();
  }

  /**
   * Return current instance.
   *
   * @return Current instance
   */
  public static FrameworkManager getInstance() {
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
  public static void setInstance(FrameworkManager newInstance) {
    instance = newInstance;
  }

  /**
   * Framework metadata structures in MForm format
   */
  private MFramework mFramework;

  /**
   * Validator instance
   */
  private final Validator validator;

  /**
   * Upgrader instance
   */
  private final MetadataUpgrader upgrader;

  /**
   * Default framework auto upgrade option value
   */
  private static final boolean DEFAULT_AUTO_UPGRADE = false;

  public static final String CURRENT_FRAMEWORK_VERSION = "1";

  public Class getJobConfigurationClass(MJob.Type jobType) {
      switch (jobType) {
          case IMPORT:
              return ImportJobConfiguration.class;
          case EXPORT:
              return ExportJobConfiguration.class;
          default:
              return null;
      }
  }
    public Class getConnectionConfigurationClass() {
        return ConnectionConfiguration.class;
    }

  public FrameworkManager() {
    MConnectionForms connectionForms = new MConnectionForms(
      FormUtils.toForms(getConnectionConfigurationClass())
    );
    List<MJobForms> jobForms = new LinkedList<MJobForms>();
    jobForms.add(new MJobForms(MJob.Type.IMPORT,
      FormUtils.toForms(getJobConfigurationClass(MJob.Type.IMPORT))));
    jobForms.add(new MJobForms(MJob.Type.EXPORT,
      FormUtils.toForms(getJobConfigurationClass(MJob.Type.EXPORT))));
    mFramework = new MFramework(connectionForms, jobForms,
      CURRENT_FRAMEWORK_VERSION);

    // Build validator
    validator = new FrameworkValidator();

    // Build upgrader
    upgrader = new FrameworkMetadataUpgrader();
  }

  public synchronized void initialize() {
    initialize(SqoopConfiguration.getInstance().getContext().getBoolean(ConfigurationConstants.FRAMEWORK_AUTO_UPGRADE, DEFAULT_AUTO_UPGRADE));
  }

  public synchronized void initialize(boolean autoUpgrade) {
    LOG.trace("Begin submission engine manager initialization");

    // Register framework metadata in repository
    mFramework = RepositoryManager.getInstance().getRepository().registerFramework(mFramework, autoUpgrade);

    SqoopConfiguration.getInstance().getProvider().registerListener(new CoreConfigurationListener(this));

    LOG.info("Submission manager initialized: OK");
  }

  public  synchronized void destroy() {
    LOG.trace("Begin submission engine manager destroy");
  }

  public Validator getValidator() {
    return validator;
  }

  public MetadataUpgrader getMetadataUpgrader() {
    return upgrader;
  }

  public MFramework getFramework() {
    return mFramework;
  }

  public ResourceBundle getBundle(Locale locale) {
    return ResourceBundle.getBundle(
        FrameworkConstants.RESOURCE_BUNDLE_NAME, locale);
  }

  @Override
  public void configurationChanged() {
    LOG.info("Begin framework manager reconfiguring");
    // If there are configuration options for FrameworkManager,
    // implement the reconfiguration procedure right here.
    LOG.info("Framework manager reconfigured");
  }
}
