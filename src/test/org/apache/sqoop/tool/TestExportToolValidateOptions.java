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

package org.apache.sqoop.tool;

import com.cloudera.sqoop.SqoopOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.manager.SupportedManagers;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
public class TestExportToolValidateOptions {
  ExportTool exportTool = new ExportTool();


  @Test
  public void givenDirectImportHasDirectConnectorValidationPasses() throws SqoopOptions.InvalidOptionsException {
    SqoopOptions options = stubDirectOptions(SupportedManagers.NETEZZA);
    exportTool.vaildateDirectExportOptions(options);
  }

  @Test(expected = org.apache.sqoop.SqoopOptions.InvalidOptionsException.class)
  public void givenDirectImportNoDirectConnectorValidationThrows() throws SqoopOptions.InvalidOptionsException {
    SqoopOptions options = stubDirectOptions(SupportedManagers.HSQLDB);
    exportTool.vaildateDirectExportOptions(options);
  }

  @Test
  public void givenNoDirectOptionWhenNoDirectConnectorAvailableValidationPasses() throws SqoopOptions.InvalidOptionsException {
    SqoopOptions options = stubNotDirectOptions(SupportedManagers.HSQLDB);
    exportTool.vaildateDirectExportOptions(options);
  }

  private SqoopOptions stubDirectOptions(SupportedManagers supportedManagers) {
    return stubOptions(supportedManagers, true);
  }

  private SqoopOptions stubNotDirectOptions(SupportedManagers supportedManagers) {
    return stubOptions(supportedManagers, false);
  }

  private SqoopOptions stubOptions(SupportedManagers supportedManagers, boolean isDirect) {
    SqoopOptions options = mock(SqoopOptions.class);
    when(options.getConnectString()).thenReturn(supportedManagers.getSchemePrefix() + "//localhost");
    when(options.isDirect()).thenReturn(isDirect);
    when(options.getConf()).thenReturn(mock(Configuration.class));
    return options;
  }
}
