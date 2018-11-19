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

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.testcategories.sqooptest.UnitTest;
import org.apache.sqoop.util.BlockJUnit4ClassRunnerWithParametersFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.sqoop.SqoopOptions.IncrementalMode.AppendRows;
import static org.apache.sqoop.SqoopOptions.IncrementalMode.DateLastModified;
import static org.apache.sqoop.tool.BaseSqoopTool.INCREMENT_TYPE_ARG;
import static org.apache.sqoop.tool.BaseSqoopTool.TEMP_ROOTDIR_ARG;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(UnitTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(BlockJUnit4ClassRunnerWithParametersFactory.class)
public class TestS3IncrementalImportOptionValidations {

    @Parameterized.Parameters(name = "incrementalMode = {0}")
    public static Iterable<? extends Object> parameters() {
        return Arrays.asList(AppendRows, DateLastModified);
    }

    private static final String TEST_TABLE = "testtable";
    private static final String TEST_CONNECTION_STRING = "testconnectstring";
    private static final String TEST_TARGET_DIR = "s3a://test-bucket";
    private static final String TEST_NOT_S3_TEMPORARY_ROOTDIR = "file:///test_temporary_rootdir";
    private static final String TEST_S3_TEMPORARY_ROOTDIR = "s3a://test_temporary_rootdir";
    private static final String TEST_CHECK_COLUMN = "testcheckcolumn";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private final SqoopOptions.IncrementalMode incrementalMode;

    private SqoopOptions sqoopOptions;

    private ImportTool importTool;

    public TestS3IncrementalImportOptionValidations(SqoopOptions.IncrementalMode incrementalMode) {this.incrementalMode = incrementalMode;}

    @Before
    public void before() {
        sqoopOptions = mock(SqoopOptions.class);
        when(sqoopOptions.getTableName()).thenReturn(TEST_TABLE);
        when(sqoopOptions.getConnectString()).thenReturn(TEST_CONNECTION_STRING);
        when(sqoopOptions.getTargetDir()).thenReturn(TEST_TARGET_DIR);
        when(sqoopOptions.getIncrementalTestColumn()).thenReturn(TEST_CHECK_COLUMN);
        when(sqoopOptions.getMapColumnHive()).thenReturn(new Properties());

        importTool = new ImportTool();
        importTool.extraArguments = new String[0];
    }

    @Test
    public void testValidateOptionsThrowsWhenS3IncrementalImportIsPerformedWithoutTemporaryRootdir() throws Exception {
        expectedException.expect(SqoopOptions.InvalidOptionsException.class);
        expectedException.expectMessage("For an " + INCREMENT_TYPE_ARG + " import into an S3 bucket --"
                + TEMP_ROOTDIR_ARG + " option must be always set to a location in S3.");

        when(sqoopOptions.getIncrementalMode()).thenReturn(incrementalMode);

        importTool.validateOptions(sqoopOptions);
    }

    @Test
    public void testValidateOptionsThrowsWhenS3IncrementalImportIsPerformedWithNotS3TemporaryRootdir() throws Exception {
        expectedException.expect(SqoopOptions.InvalidOptionsException.class);
        expectedException.expectMessage("For an " + INCREMENT_TYPE_ARG + " import into an S3 bucket --"
                + TEMP_ROOTDIR_ARG + " option must be always set to a location in S3.");

        when(sqoopOptions.getIncrementalMode()).thenReturn(incrementalMode);
        when(sqoopOptions.getTempRootDir()).thenReturn(TEST_NOT_S3_TEMPORARY_ROOTDIR);

        importTool.validateOptions(sqoopOptions);
    }

    @Test
    public void testValidateOptionsSucceedsWhenS3IncrementalImportIsPerformedWithS3TemporaryRootdir() throws Exception {
        when(sqoopOptions.getIncrementalMode()).thenReturn(incrementalMode);
        when(sqoopOptions.getTempRootDir()).thenReturn(TEST_S3_TEMPORARY_ROOTDIR);

        importTool.validateOptions(sqoopOptions);
    }
}
