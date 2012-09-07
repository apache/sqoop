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
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.validation.Validator;

import static org.apache.sqoop.framework.FrameworkConstants.*;

import java.util.ArrayList;
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
public class FrameworkManager {

  private static final Logger LOG = Logger.getLogger(FrameworkManager.class);

  private static final MConnectionForms CONNECTION_FORMS;

  private static final List<MJobForms> JOB_FORMS;

  private static final MFramework mFramework;

  private static final Validator validator;

  static {

    List<MForm> conForms = new ArrayList<MForm>();

    // Build the CONNECTION_FORMS forms for import
    List<MInput<?>> connFormInputs = new ArrayList<MInput<?>>();

    MStringInput maxConnections = new MStringInput(
      INPUT_CONN_MAX_SIMULTANEOUS_CONNECTIONS, false, (short) 10);
    connFormInputs.add(maxConnections);

    MForm connForm = new MForm(FORM_SECURITY, connFormInputs);

    conForms.add(connForm);
    CONNECTION_FORMS = new MConnectionForms(conForms);

    // Build job forms for import
    List<MInput<?>> jobFormInputs = new ArrayList<MInput<?>>();

    MStringInput outputFormat = new MStringInput(INPUT_CONN_MAX_OUTPUT_FORMAT,
      false, (short) 25);
    jobFormInputs.add(outputFormat);

    MForm jobForm = new MForm(FORM_OUTPUT, jobFormInputs);
    List<MForm> jobForms = new ArrayList<MForm>();
    jobForms.add(jobForm);

    JOB_FORMS = new ArrayList<MJobForms>();
    JOB_FORMS.add(new MJobForms(MJob.Type.IMPORT, jobForms));
    JOB_FORMS.add(new MJobForms(MJob.Type.EXPORT, jobForms));

    mFramework = new MFramework(CONNECTION_FORMS, JOB_FORMS);

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
}
