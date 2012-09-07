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
package org.apache.sqoop.connector.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

import org.apache.sqoop.job.etl.Exporter;
import org.apache.sqoop.job.etl.Importer;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.connector.spi.SqoopConnector;
import org.apache.sqoop.validation.Validator;

import static org.apache.sqoop.connector.jdbc.GenericJdbcConnectorConstants.*;


public class GenericJdbcConnector implements SqoopConnector {

  private static final MConnectionForms CONNECTION_FORMS;
  private static final List<MJobForms> JOB_FORMS;

  private Importer IMPORTER = new Importer(
      GenericJdbcImportInitializer.class,
      GenericJdbcImportPartitioner.class,
      GenericJdbcImportExtractor.class,
      GenericJdbcImportDestroyer.class);

  private Exporter EXPORTER = new Exporter(
      GenericJdbcExportInitializer.class,
      GenericJdbcExportLoader.class,
      GenericJdbcExportDestroyer.class);

  static {
    // Connection forms
    List<MForm> forms = new ArrayList<MForm>();
    List<MInput<?>> inputs = new ArrayList<MInput<?>>();

    MStringInput jdbcDriver = new MStringInput(INPUT_CONN_JDBCDRIVER, false,
        (short) 128);
    inputs.add(jdbcDriver);

    MStringInput connectString = new MStringInput(INPUT_CONN_CONNECTSTRING,
      false, (short) 128);
    inputs.add(connectString);

    MStringInput username = new MStringInput(INPUT_CONN_USERNAME, false,
      (short) 36);
    inputs.add(username);

    MStringInput password = new MStringInput(INPUT_CONN_PASSWORD, true,
      (short) 10);
    inputs.add(password);

    MMapInput jdbcProperties = new MMapInput(INPUT_CONN_JDBCPROPS);
    inputs.add(jdbcProperties);

    MForm connForm = new MForm(FORM_CONNECTION, inputs);
    forms.add(connForm);

    CONNECTION_FORMS = new MConnectionForms(forms);

    // Job forms
    forms = new ArrayList<MForm>();
    inputs = new ArrayList<MInput<?>>();

    inputs.add(new MStringInput(INPUT_TBL_TABLE, false, (short) 50));
    forms.add(new MForm(FORM_TABLE, inputs));

    JOB_FORMS = new ArrayList<MJobForms>();
    JOB_FORMS.add(new MJobForms(MJob.Type.IMPORT, forms));
    JOB_FORMS.add(new MJobForms(MJob.Type.EXPORT, forms));
  }

  @Override
  public ResourceBundle getBundle(Locale locale) {
    return ResourceBundle.getBundle(
        GenericJdbcConnectorConstants.RESOURCE_BUNDLE_NAME, locale);
  }

  @Override
  public MConnectionForms getConnectionForms() {
    return CONNECTION_FORMS;
  }

  @Override
  public List<MJobForms> getJobsForms() {
    return JOB_FORMS;
  }

  @Override
  public Importer getImporter() {
    return IMPORTER;
  }

  @Override
  public Exporter getExporter() {
    return EXPORTER;
  }

  @Override
  public Validator getValidator() {
    // TODO(jarcec): Cache this object eventually
    return new GenericJdbcValidator();
  }

}
