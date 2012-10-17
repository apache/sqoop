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
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.framework.configuration.ConnectionConfiguration;
import org.apache.sqoop.framework.configuration.ExportJobConfiguration;
import org.apache.sqoop.framework.configuration.ImportJobConfiguration;
import org.apache.sqoop.model.FormUtils;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.validation.Validator;

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Manager for Sqoop framework itself.
 *
 * All Sqoop internals (job execution engine, metadata) should be handled
 * within this manager.
 *
 */
public final class FrameworkManager {

  private static final Logger LOG = Logger.getLogger(FrameworkManager.class);

  private static final MFramework mFramework;
  private static final Validator validator;

  static {

    MConnectionForms connectionForms = new MConnectionForms(
      FormUtils.toForms(getConnectionConfigurationClass())
    );
    List<MJobForms> jobForms = new LinkedList<MJobForms>();
    jobForms.add(new MJobForms(MJob.Type.IMPORT,
      FormUtils.toForms(getJobConfigurationClass(MJob.Type.IMPORT))));
    jobForms.add(new MJobForms(MJob.Type.EXPORT,
      FormUtils.toForms(getJobConfigurationClass(MJob.Type.EXPORT))));
    mFramework = new MFramework(connectionForms, jobForms);

    // Build validator
    validator = new Validator();
  }

  public static synchronized void initialize() {
    LOG.trace("Begin connector manager initialization");

    // Register framework metadata
    RepositoryManager.getRepository().registerFramework(mFramework);
    if (!mFramework.hasPersistenceId()) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0000);
    }
  }

  public static Class getConnectionConfigurationClass() {
    return ConnectionConfiguration.class;
  }

  public static Class getJobConfigurationClass(MJob.Type jobType) {
    switch (jobType) {
      case IMPORT:
        return ImportJobConfiguration.class;
      case EXPORT:
        return ExportJobConfiguration.class;
      default:
        return null;
    }
  }

  public static MFramework getFramework() {
    return mFramework;
  }

  public static synchronized void destroy() {
    LOG.trace("Begin framework manager destroy");
  }

  public static Validator getValidator() {
    return validator;
  }

  public static ResourceBundle getBundle(Locale locale) {
    return ResourceBundle.getBundle(
        FrameworkConstants.RESOURCE_BUNDLE_NAME, locale);
  }

  private FrameworkManager() {
    // Instantiation of this class is prohibited
  }
}
