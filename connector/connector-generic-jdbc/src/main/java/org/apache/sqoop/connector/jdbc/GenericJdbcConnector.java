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
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MMapInput;
import org.apache.sqoop.model.MStringInput;
import org.apache.sqoop.connector.spi.SqoopConnector;

public class GenericJdbcConnector implements SqoopConnector {

  private static final List<MForm> CONNECTION_FORMS = new ArrayList<MForm>();
  private static final List<MForm> JOB_FORMS = new ArrayList<MForm>();

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
    // Build the connection form
    List<MInput<?>> connFormInputs = new ArrayList<MInput<?>>();

    MStringInput jdbcDriver = new MStringInput(
        GenericJdbcConnectorConstants.INPUT_CONN_JDBCDRIVER, false,
        (short) 128);
    connFormInputs.add(jdbcDriver);

    MStringInput connectString = new MStringInput(
        GenericJdbcConnectorConstants.INPUT_CONN_CONNECTSTRING, false,
        (short) 128);
    connFormInputs.add(connectString);

    MStringInput username = new MStringInput(
        GenericJdbcConnectorConstants.INPUT_CONN_USERNAME, false, (short) 36);
    connFormInputs.add(username);

    MStringInput password = new MStringInput(
        GenericJdbcConnectorConstants.INPUT_CONN_PASSWORD, true, (short) 10);
    connFormInputs.add(password);

    MMapInput jdbcProperties = new MMapInput(
        GenericJdbcConnectorConstants.INPUT_CONN_JDBCPROPS);
    connFormInputs.add(jdbcProperties);

    MForm connForm = new MForm(GenericJdbcConnectorConstants.FORM_CONNECTION,
        connFormInputs);

    CONNECTION_FORMS.add(connForm);
  }

  @Override
  public ResourceBundle getBundle(Locale locale) {
    return ResourceBundle.getBundle(
        GenericJdbcConnectorConstants.RESOURCE_BUNDLE_NAME, locale);
  }

  @Override
  public List<MForm> getConnectionForms() {
    return CONNECTION_FORMS;
  }

  @Override
  public List<MForm> getJobForms() {
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

}
