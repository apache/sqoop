/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.importjob.numerictypes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.importjob.configuration.AvroTestConfiguration;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.AvroTestUtils;
import org.apache.sqoop.testutil.NumericTypesTestUtils;

public abstract class NumericTypesAvroImportTestBase<T extends AvroTestConfiguration> extends NumericTypesImportTestBase<T>  {

  public static final Log LOG = LogFactory.getLog(NumericTypesAvroImportTestBase.class.getName());

  public NumericTypesAvroImportTestBase(T configuration, boolean failWithoutExtraArgs, boolean failWithPaddingOnly) {
    super(configuration, failWithoutExtraArgs, failWithPaddingOnly);
  }

  @Override
  public ArgumentArrayBuilder getArgsBuilder() {
    ArgumentArrayBuilder builder =  new ArgumentArrayBuilder();
    includeCommonOptions(builder);
    builder.withOption("as-avrodatafile");
    NumericTypesTestUtils.addEnableAvroDecimal(builder);
    return builder;
  }

  @Override
  public void verify() {
    AvroTestUtils.registerDecimalConversionUsageForVerification();
    AvroTestUtils.verify(configuration.getExpectedResultsForAvro(), getConf(), getTablePath());
  }

}
