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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.util.ClassLoaderStack;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SequenceFileTestUtils {

    private static final String OUTPUT_FILE_NAME = "/part-m-00000";

    public static final Log LOG = LogFactory.getLog(
            SequenceFileTestUtils.class.getName());

    /**
     * Verify results at the given tablePath.
     * @param testCase current instance of BaseSqoopTestCase
     * @param expectedResults string array of expected results
     * @param fileSystem current fileSystem
     * @param tablePath path of the output table
     */
    public static void verify(BaseSqoopTestCase testCase, String[] expectedResults, FileSystem fileSystem, Path tablePath) throws Exception{
        String outputFilePathString = tablePath.toString() + OUTPUT_FILE_NAME;
        readAndVerify(testCase, expectedResults, fileSystem, outputFilePathString);
    }

    /**
     * Verify results at the given tablePath.
     * @param testCase current instance of BaseSqoopTestCase
     * @param expectedResults string array of expected results
     * @param fileSystem current fileSystem
     * @param tablePath path of the output table
     * @param outputFileName MapReduce output filename
     */
    public static void verify(BaseSqoopTestCase testCase, String[] expectedResults, FileSystem fileSystem, Path tablePath,  String outputFileName) throws Exception{
        String outputFilePathString = tablePath.toString() + "/" + outputFileName;
        readAndVerify(testCase, expectedResults, fileSystem, outputFilePathString);
    }

    private static void readAndVerify(BaseSqoopTestCase testCase, String[] expectedResults, FileSystem fileSystem, String outputFilePathString) throws Exception {
        Path outputFilePath = new Path(outputFilePathString);

        Configuration conf = fileSystem.getConf();

        ClassLoader prevClassLoader = ClassLoaderStack.addJarFile(
                new Path(new Path(new SqoopOptions().getJarOutputDir()), testCase.getTableName() + ".jar").toString(),
                testCase.getTableName());

        // Needs to set the classLoader for the Configuration object otherwise SequenceFile cannot load custom classes
        conf.setClassLoader(Thread.currentThread().getContextClassLoader());

        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(outputFilePath))) {
            WritableComparable key = (WritableComparable) reader.getKeyClass().newInstance();
            Writable value = (Writable) reader.getValueClass().newInstance();
            boolean hasNextRecord = reader.next(key, value);
            int i = 0;

            if (!hasNextRecord && expectedResults != null && expectedResults.length > 0) {
                fail("Empty output file was not expected");
            }

            while (hasNextRecord) {
                assertEquals(expectedResults[i++], value.toString());
                hasNextRecord = reader.next(key, value);
            }

            if (expectedResults != null && expectedResults.length > i) {
                fail("More output data was expected");
            }
        } catch (IOException ioe) {
            LOG.error("Issue with verifying the output", ioe);
            throw new RuntimeException(ioe);
        } finally {
            ClassLoaderStack.setCurrentClassLoader(prevClassLoader);
        }
    }
}
