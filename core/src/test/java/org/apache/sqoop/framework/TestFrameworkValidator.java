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

import org.apache.sqoop.framework.configuration.ConnectionConfiguration;
import org.apache.sqoop.framework.configuration.ExportJobConfiguration;
import org.apache.sqoop.framework.configuration.ImportJobConfiguration;
import org.apache.sqoop.framework.configuration.OutputCompression;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.validation.Status;
import org.apache.sqoop.validation.Validation;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestFrameworkValidator {

//  FrameworkValidator validator;
//
//  @Before
//  public void setUp() {
//    validator = new FrameworkValidator();
//  }
//
//  @Test
//  public void testConnectionValidation() {
//    ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration();
//
//    Validation validation = validator.validateConnection(connectionConfiguration);
//    assertEquals(Status.FINE, validation.getStatus());
//    assertEquals(0, validation.getMessages().size());
//  }
//
//  @Test
//  public void testExportJobValidation() {
//    ExportJobConfiguration configuration;
//    Validation validation;
//
//    // Empty form is not allowed
//    configuration = new ExportJobConfiguration();
//    validation = validator.validateJob(MJob.Type.EXPORT, configuration);
//    assertEquals(Status.UNACCEPTABLE, validation.getStatus());
//    assertTrue(validation.getMessages().containsKey(new Validation.FormInput("input.inputDirectory")));
//
//    // Explicitly setting extractors and loaders
//    configuration = new ExportJobConfiguration();
//    configuration.input.inputDirectory = "/czech/republic";
//    configuration.throttling.extractors = 3;
//    configuration.throttling.loaders = 3;
//
//    validation = validator.validateJob(MJob.Type.EXPORT, configuration);
//    assertEquals(Status.FINE, validation.getStatus());
//    assertEquals(0, validation.getMessages().size());
//
//    // Negative and zero values for extractors and loaders
//    configuration = new ExportJobConfiguration();
//    configuration.input.inputDirectory = "/czech/republic";
//    configuration.throttling.extractors = 0;
//    configuration.throttling.loaders = -1;
//
//    validation = validator.validateJob(MJob.Type.EXPORT, configuration);
//    assertEquals(Status.UNACCEPTABLE, validation.getStatus());
//    assertTrue(validation.getMessages().containsKey(new Validation.FormInput("throttling.extractors")));
//    assertTrue(validation.getMessages().containsKey(new Validation.FormInput("throttling.loaders")));
//  }
//
//
//  @Test
//  public void testImportJobValidation() {
//    ImportJobConfiguration configuration;
//    Validation validation;
//
//    // Empty form is not allowed
//    configuration = new ImportJobConfiguration();
//    validation = validator.validateJob(MJob.Type.IMPORT, configuration);
//    assertEquals(Status.UNACCEPTABLE, validation.getStatus());
//    assertTrue(validation.getMessages().containsKey(new Validation.FormInput("output.outputDirectory")));
//
//    // Explicitly setting extractors and loaders
//    configuration = new ImportJobConfiguration();
//    configuration.output.outputDirectory = "/czech/republic";
//    configuration.throttling.extractors = 3;
//    configuration.throttling.loaders = 3;
//
//    validation = validator.validateJob(MJob.Type.IMPORT, configuration);
//    assertEquals(Status.FINE, validation.getStatus());
//    assertEquals(0, validation.getMessages().size());
//
//    // Negative and zero values for extractors and loaders
//    configuration = new ImportJobConfiguration();
//    configuration.output.outputDirectory = "/czech/republic";
//    configuration.throttling.extractors = 0;
//    configuration.throttling.loaders = -1;
//
//    validation = validator.validateJob(MJob.Type.IMPORT, configuration);
//    assertEquals(Status.UNACCEPTABLE, validation.getStatus());
//    assertTrue(validation.getMessages().containsKey(new Validation.FormInput("throttling.extractors")));
//    assertTrue(validation.getMessages().containsKey(new Validation.FormInput("throttling.loaders")));
//
//    // specifying both compression as well as customCompression is
//    // unacceptable
//    configuration = new ImportJobConfiguration();
//    configuration.output.outputDirectory = "/czech/republic";
//    configuration.throttling.extractors = 2;
//    configuration.throttling.loaders = 2;
//    configuration.output.compression = OutputCompression.BZIP2;
//    configuration.output.customCompression = "some.compression.codec";
//
//    validation = validator.validateJob(MJob.Type.IMPORT, configuration);
//    assertEquals(Status.UNACCEPTABLE, validation.getStatus());
//    assertTrue(validation.getMessages().containsKey(new Validation.FormInput("output.compression")));
//
//    // specifying a customCompression is fine
//    configuration = new ImportJobConfiguration();
//    configuration.output.outputDirectory = "/czech/republic";
//    configuration.throttling.extractors = 2;
//    configuration.throttling.loaders = 2;
//    configuration.output.compression = OutputCompression.CUSTOM;
//    configuration.output.customCompression = "some.compression.codec";
//
//    validation = validator.validateJob(MJob.Type.IMPORT, configuration);
//    assertEquals(Status.FINE, validation.getStatus());
//
//    // specifying a customCompression without codec name is unacceptable
//    configuration = new ImportJobConfiguration();
//    configuration.output.outputDirectory = "/czech/republic";
//    configuration.throttling.extractors = 2;
//    configuration.throttling.loaders = 2;
//    configuration.output.compression = OutputCompression.CUSTOM;
//    configuration.output.customCompression = "";
//
//    validation = validator.validateJob(MJob.Type.IMPORT, configuration);
//    assertEquals(Status.UNACCEPTABLE, validation.getStatus());
//    assertTrue(validation.getMessages().containsKey(new Validation.FormInput("output.compression")));
//
//    configuration = new ImportJobConfiguration();
//    configuration.output.outputDirectory = "/czech/republic";
//    configuration.throttling.extractors = 2;
//    configuration.throttling.loaders = 2;
//    configuration.output.compression = OutputCompression.CUSTOM;
//    configuration.output.customCompression = null;
//
//    validation = validator.validateJob(MJob.Type.IMPORT, configuration);
//    assertEquals(Status.UNACCEPTABLE, validation.getStatus());
//    assertTrue(validation.getMessages().containsKey(new Validation.FormInput("output.compression")));
//
//  }
}
