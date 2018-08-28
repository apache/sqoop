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

package org.apache.sqoop.testutil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TextFileTestUtils {

    private static final String DEFAULT_OUTPUT_FILE_NAME = "/part-m-00000";

    public static final Log LOG = LogFactory.getLog(
            TextFileTestUtils.class.getName());

    /**
     * Verify results at the given tablePath.
     * @param expectedResults string array of expected results
     * @param fileSystem current filesystem
     * @param tablePath path of the output table
     */
    public static void verify(String[] expectedResults, FileSystem fileSystem, Path tablePath) throws IOException {
        String outputFilePathString = tablePath.toString() + DEFAULT_OUTPUT_FILE_NAME;
        readAndVerify(expectedResults, fileSystem, outputFilePathString);
    }

    /**
     * Verify results at the given tablePath.
     * @param expectedResults string array of expected results
     * @param fileSystem current filesystem
     * @param tablePath path of the output table
     * @param outputFileName MapReduce output filename
     */
    public static void verify(String[] expectedResults, FileSystem fileSystem, Path tablePath, String outputFileName) {
        String outputFilePathString = tablePath.toString() + "/" + outputFileName;
        readAndVerify(expectedResults, fileSystem, outputFilePathString);
    }

    private static void readAndVerify(String[] expectedResults, FileSystem fileSystem, String outputFilePathString) {
        Path outputFilePath = new Path(outputFilePathString);

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(outputFilePath), Charset.forName("UTF-8")))) {
            String line = br.readLine();
            int i = 0;

            if (line == null && expectedResults != null && expectedResults.length > 0) {
                fail("Empty output file was not expected");
            }

            while (line != null) {
                assertEquals(expectedResults[i++], line);
                line = br.readLine();
            }

            if (expectedResults != null && expectedResults.length > i) {
                fail("More output data was expected");
            }

        } catch (IOException ioe) {
            LOG.error("Issue with verifying the output", ioe);
            throw new RuntimeException(ioe);
        }
    }
}
