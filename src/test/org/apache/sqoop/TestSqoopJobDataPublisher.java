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

package org.apache.sqoop;

import com.cloudera.sqoop.hive.HiveImport;
import com.cloudera.sqoop.testutil.CommonArgs;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.tool.SqoopTool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.sqoop.config.ConfigurationConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestSqoopJobDataPublisher extends ImportJobTestCase {

    public static final Log LOG = LogFactory.getLog(TestSqoopJobDataPublisher.class.getName());

    @Before
    public void setUp() {
        super.setUp();
        HiveImport.setTestMode(true);
    }

    @After
    public void tearDown() {
        super.tearDown();
        HiveImport.setTestMode(false);
    }
    /**
     * Create the argv to pass to Sqoop.
     * @return the argv as an array of strings.
     */
    protected String [] getArgv(boolean includeHadoopFlags, String [] moreArgs) {
        ArrayList<String> args = new ArrayList<String>();

        if (includeHadoopFlags) {
            CommonArgs.addHadoopFlags(args);
        }

        args.add("-D");
        args.add(ConfigurationConstants.DATA_PUBLISH_CLASS + "=" + DummyDataPublisher.class.getName());

        if (null != moreArgs) {
            for (String arg: moreArgs) {
                args.add(arg);
            }
        }

        args.add("--table");
        args.add(getTableName());
        args.add("--warehouse-dir");
        args.add(getWarehouseDir());
        args.add("--connect");
        args.add(getConnectString());
        args.add("--hive-import");
        String [] colNames = getColNames();
        if (null != colNames) {
            args.add("--split-by");
            args.add(colNames[0]);
        } else {
            fail("Could not determine column names.");
        }

        args.add("--num-mappers");
        args.add("1");

        for (String a : args) {
            LOG.debug("ARG : "+ a);
        }

        return args.toArray(new String[0]);
    }

    private void runImportTest(String tableName, String [] types,
                               String [] values, String verificationScript, String [] args,
                               SqoopTool tool) throws IOException {

        // create a table and populate it with a row...
        createTableWithColTypes(types, values);

        // set up our mock hive shell to compare our generated script
        // against the correct expected one.
        com.cloudera.sqoop.SqoopOptions options = getSqoopOptions(args, tool);
        String hiveHome = options.getHiveHome();
        assertNotNull("hive.home was not set", hiveHome);
        String testDataPath = new Path(new Path(hiveHome),
                "scripts/" + verificationScript).toString();
        System.setProperty("expected.script",
                new File(testDataPath).getAbsolutePath());

        // verify that we can import it correctly into hive.
        runImport(tool, args);
    }

    private com.cloudera.sqoop.SqoopOptions getSqoopOptions(String [] args, SqoopTool tool) {
        com.cloudera.sqoop.SqoopOptions opts = null;
        try {
            opts = tool.parseArguments(args, null, null, true);
        } catch (Exception e) {
            fail("Invalid options: " + e.toString());
        }

        return opts;
    }

    protected void setNumCols(int numCols) {
        String [] cols = new String[numCols];
        for (int i = 0; i < numCols; i++) {
            cols[i] = "DATA_COL" + i;
        }

        setColNames(cols);
    }

    /** Test that strings and ints are handled in the normal fashion. */
    @Test
    public void testNormalHiveImport() throws IOException {
        final String TABLE_NAME = "NORMAL_HIVE_IMPORT";
        setCurTableName(TABLE_NAME);
        setNumCols(3);
        String [] types = { "VARCHAR(32)", "INTEGER", "CHAR(64)" };
        String [] vals = { "'test'", "42", "'somestring'" };
        runImportTest(TABLE_NAME, types, vals, "normalImport.q",
                getArgv(false, null), new ImportTool());
        assert (DummyDataPublisher.hiveTable.equals("NORMAL_HIVE_IMPORT"));
        assert (DummyDataPublisher.storeTable.equals("NORMAL_HIVE_IMPORT"));
        assert (DummyDataPublisher.storeType.equals("hsqldb"));
        assert (DummyDataPublisher.operation.equals("import"));
    }

}
