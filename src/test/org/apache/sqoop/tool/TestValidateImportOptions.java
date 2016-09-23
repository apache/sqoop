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
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestValidateImportOptions {

    ImportTool importTool = new ImportTool();

    private static final String mysqlConnectionString = "jdbc:mysql://" + RandomStringUtils.random(5);


    @Test
    public void givenDirectImportMysqlTextFileValidationPasses() throws SqoopOptions.InvalidOptionsException {
        SqoopOptions options = mock(SqoopOptions.class);
        when(options.getFileLayout()).thenReturn(SqoopOptions.FileLayout.TextFile);
        when(options.getConnectString()).thenReturn(mysqlConnectionString);
        importTool.validateDirectMysqlOptions(options);
        verify(options, times(1)).getFileLayout();
        verifyNoMoreInteractions(options);
    }


    @Test(expected = org.apache.sqoop.SqoopOptions.InvalidOptionsException.class)
    public void givenDirectImportMysqlSequenceFileValidationThrows() throws SqoopOptions.InvalidOptionsException {
        SqoopOptions options = mock(SqoopOptions.class);
        when(options.getFileLayout()).thenReturn(SqoopOptions.FileLayout.SequenceFile);
        when(options.getConnectString()).thenReturn(mysqlConnectionString);
        importTool.validateDirectMysqlOptions(options);
        verify(options, times(1)).getFileLayout();
        verify(options, times(1)).getConnectString();
        verifyNoMoreInteractions(options);
    }

    @Test(expected = org.apache.sqoop.SqoopOptions.InvalidOptionsException.class)
    public void givenDirectImportMysqlParquetFileValidationThrows() throws SqoopOptions.InvalidOptionsException {
        SqoopOptions options = mock(SqoopOptions.class);
        when(options.getFileLayout()).thenReturn(SqoopOptions.FileLayout.ParquetFile);
        when(options.getConnectString()).thenReturn(mysqlConnectionString);
        importTool.validateDirectMysqlOptions(options);
        verify(options, times(1)).getFileLayout();
        verify(options, times(1)).getConnectString();
        verifyNoMoreInteractions(options);
    }
    @Test(expected = org.apache.sqoop.SqoopOptions.InvalidOptionsException.class)
    public void givenDirectImportMysqlAvroDataFileValidationThrows() throws SqoopOptions.InvalidOptionsException {
        SqoopOptions options = mock(SqoopOptions.class);
        when(options.getFileLayout()).thenReturn(SqoopOptions.FileLayout.AvroDataFile);
        when(options.getConnectString()).thenReturn(mysqlConnectionString);
        importTool.validateDirectMysqlOptions(options);
        verify(options, times(1)).getFileLayout();
        verify(options, times(1)).getConnectString();
        verifyNoMoreInteractions(options);
    }

    @Test(expected = org.apache.sqoop.SqoopOptions.InvalidOptionsException.class)
    public void givenDirectImportHiveDropDelimValidationThrows() throws SqoopOptions.InvalidOptionsException {
        SqoopOptions options = mock(SqoopOptions.class);
        when(options.doHiveDropDelims()).thenReturn(true);
        importTool.validateDirectDropHiveDelimOption(options);
        verify(options, times(1)).doHiveDropDelims();
        verifyNoMoreInteractions(options);
    }

    @Test
    public void givenDirectImportHasDirectConnectorValidationPasses() throws SqoopOptions.InvalidOptionsException {
        SqoopOptions options = stubDirectOptions(SupportedManagers.NETEZZA);
        importTool.validateDirectImportOptions(options);
    }

    @Test(expected = org.apache.sqoop.SqoopOptions.InvalidOptionsException.class)
    public void givenDirectImportNoDirectConnectorValidationThrows() throws SqoopOptions.InvalidOptionsException {
        SqoopOptions options = stubDirectOptions(SupportedManagers.HSQLDB);
        importTool.validateDirectImportOptions(options);
    }

    @Test
    public void givenNoDirectOptionWhenNoDirectConnectorAvailableValidationPasses() throws SqoopOptions.InvalidOptionsException {
        SqoopOptions options = stubNotDirectOptions(SupportedManagers.HSQLDB);
        importTool.validateDirectImportOptions(options);
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

    //TODO create tests for all old validations as well
}
