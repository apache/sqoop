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
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MFramework;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.repository.RepositoryManager;

import static org.apache.sqoop.framework.FrameworkConstants.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Manager for Sqoop framework itself.
 *
 * All Sqoop internals (job execution engine, metadata) should be handled
 * within this manager.
 *
 */
public class FrameworkManager {

  private static final Logger LOG = Logger.getLogger(FrameworkManager.class);

  private static final List<MForm> CONNECTION_FORMS = new ArrayList<MForm>();
  private static final List<MForm> JOB_FORMS = new ArrayList<MForm>();

  private static MFramework mFramework;

  static {
    // Build the connection forms
    List<MInput<?>> connFormInputs = new ArrayList<MInput<?>>();

    MStringInput maxConnections = new MStringInput(
      INPUT_CONN_MAX_SIMULTANEOUS_CONNECTIONS, false, (short) 10);
    connFormInputs.add(maxConnections);

    MForm connForm = new MForm(FORM_SECURITY, connFormInputs);

    CONNECTION_FORMS.add(connForm);

    // Build job forms
    List<MInput<?>> jobFormInputs = new ArrayList<MInput<?>>();

    MStringInput outputFormat = new MStringInput(INPUT_CONN_MAX_OUTPUT_FORMAT,
      false, (short) 25);
    jobFormInputs.add(outputFormat);

    MForm jobForm = new MForm(FORM_OUTPUT, jobFormInputs);
    JOB_FORMS.add(jobForm);
  }

  public static synchronized void initialize() {
    LOG.trace("Begin connector manager initialization");

    // Register framework metadata
    mFramework = new MFramework(CONNECTION_FORMS, JOB_FORMS);
    RepositoryManager.getRepository().registerFramework(mFramework);
    if (!mFramework.hasPersistenceId()) {
      throw new SqoopException(FrameworkError.FRAMEWORK_0000);
    }
  }

  public static synchronized void destroy() {
    LOG.trace("Begin framework manager destroy");
  }

}
