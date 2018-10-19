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
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.sqoop.hive.minicluster.HiveMiniCluster;
import org.apache.sqoop.hive.minicluster.NoAuthenticationConfiguration;
import org.apache.sqoop.util.FileSystemUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.apache.sqoop.util.AppendUtils.MAPREDUCE_OUTPUT_BASENAME_PROPERTY;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

public class S3TestUtils {

    private static final String PROPERTY_GENERATOR_COMMAND = "s3.generator.command";
    private static final String PROPERTY_BUCKET_URL = "s3.bucket.url";

    private static final String TEMPORARY_CREDENTIALS_PROVIDER_CLASS = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";

    private static final String BUCKET_TEMP_DIR = "/tmp/";

    private static final String TARGET_DIR_NAME_PREFIX = "/testdir";
    private static final String HIVE_EXTERNAL_DIR_NAME_PREFIX = "/externaldir";

    private static final String TEMPORARY_ROOTDIR_SUFFIX = "_temprootdir";

    public static final String HIVE_EXTERNAL_TABLE_NAME = "test_external_table";

    private static String targetDirName;
    private static String hiveExternalTableDirName;

    private static final String[] COLUMN_NAMES = {"ID",  "SUPERHERO", "COMICS", "DEBUT"};
    private static final String[] COLUMN_TYPES = { "INT", "VARCHAR(25)", "VARCHAR(25)", "INT"};

    private static final String[] COLUMN_NAMES_FOR_MERGE = { "DEBUT", "SUPERHERO1", "SUPERHERO2", "RECORD_DATE"};
    private static final String[] COLUMN_TYPES_FOR_MERGE = { "INT", "VARCHAR(25)", "VARCHAR(25)", "TIMESTAMP"};
    private static final String INITIAL_TIMESTAMP_FOR_MERGE = "2018-07-23 15:00:00.000";
    private static final String NEW_TIMESTAMP_FOR_MERGE = "2018-08-16 16:30:09.000";
    private static final String EXPECTED_INITIAL_TIMESTAMP_FOR_MERGE = "2018-07-23 15:00:00.0";
    private static final String EXPECTED_NEW_TIMESTAMP_FOR_MERGE = "2018-08-16 16:30:09.0";

    public static final String MAPREDUCE_OUTPUT_BASENAME = "custom";
    public static final String CUSTOM_MAP_OUTPUT_FILE_00001 = MAPREDUCE_OUTPUT_BASENAME + "-m-00001";
    public static final String CUSTOM_REDUCE_OUTPUT_FILE_00000 = MAPREDUCE_OUTPUT_BASENAME + "-r-00000";

    public static final Log LOG = LogFactory.getLog(
            S3TestUtils.class.getName());

    public static String getGeneratorCommand() {
        return System.getProperty(PROPERTY_GENERATOR_COMMAND);
    }

    private static String getPropertyBucketUrl() {
        return System.getProperty(PROPERTY_BUCKET_URL);
    }

    private static String getTemporaryCredentialsProviderClass() {
        return TEMPORARY_CREDENTIALS_PROVIDER_CLASS;
    }

    private static String generateUniqueDirName(String dirPrefix) {
        String uuid = UUID.randomUUID().toString();
        return dirPrefix + "-" + uuid;
    }

    private static void resetTargetDirName() {
        targetDirName = null;
    }

    private static void resetHiveExternalDirName() {
        hiveExternalTableDirName = null;
    }

    private static String getTargetDirName() {
        return targetDirName;
    }

    private static String getHiveExternalTableDirName() {
        return hiveExternalTableDirName;
    }

    public static Path getTargetDirPath() {
        String targetPathString = getBucketTempDirPath() + getTargetDirName();
        return new Path(targetPathString);
    }

    private static Path getBucketTempDirPath() {
        String targetPathString = getPropertyBucketUrl() + BUCKET_TEMP_DIR;
        return new Path(targetPathString);
    }

    public static Path getExternalTableDirPath() {
        String externalTableDir = getBucketTempDirPath() + getHiveExternalTableDirName();
        return new Path(externalTableDir);
    }

    public static void runTestCaseOnlyIfS3CredentialsAreSet(S3CredentialGenerator s3CredentialGenerator) {
        assumeNotNull(s3CredentialGenerator);
        assumeNotNull(s3CredentialGenerator.getS3AccessKey());
        assumeNotNull(s3CredentialGenerator.getS3SecretKey());
    }

    public static FileSystem setupS3ImportTestCase(S3CredentialGenerator s3CredentialGenerator) throws IOException {
        Configuration hadoopConf = new Configuration();
        setS3CredentialsInConf(hadoopConf, s3CredentialGenerator);
        setHadoopConfigParametersForS3UnitTests(hadoopConf);

        FileSystem s3Client = FileSystem.get(hadoopConf);

        targetDirName = generateUniqueDirName(TARGET_DIR_NAME_PREFIX);

        cleanUpDirectory(s3Client, getTargetDirPath());

        return s3Client;
    }

    public static void setS3CredentialsInConf(Configuration conf,
                                              S3CredentialGenerator s3CredentialGenerator) {
        conf.set(Constants.ACCESS_KEY, s3CredentialGenerator.getS3AccessKey());
        conf.set(Constants.SECRET_KEY, s3CredentialGenerator.getS3SecretKey());

        if (s3CredentialGenerator.getS3SessionToken() != null) {
            conf.set(Constants.SESSION_TOKEN, s3CredentialGenerator.getS3SessionToken());
            conf.set(Constants.AWS_CREDENTIALS_PROVIDER, TEMPORARY_CREDENTIALS_PROVIDER_CLASS);
        }
    }

    private static void setHadoopConfigParametersForS3UnitTests(Configuration hadoopConf) {
        // Default filesystem needs to be set to S3 for the output verification phase
        hadoopConf.set("fs.defaultFS", getPropertyBucketUrl());

        // FileSystem has a static cache that should be disabled during tests to make sure
        // Sqoop relies on the S3 credentials set via the -D system properties.
        // For details please see SQOOP-3383
        hadoopConf.setBoolean("fs.s3a.impl.disable.cache", true);
    }

    public static HiveMiniCluster setupS3ExternalHiveTableImportTestCase(S3CredentialGenerator s3CredentialGenerator) {
        hiveExternalTableDirName = generateUniqueDirName(HIVE_EXTERNAL_DIR_NAME_PREFIX);
        HiveMiniCluster hiveMiniCluster = new HiveMiniCluster(new NoAuthenticationConfiguration());
        hiveMiniCluster.start();
        S3TestUtils.setS3CredentialsInConf(hiveMiniCluster.getConfig(), s3CredentialGenerator);

        return hiveMiniCluster;
    }

    public static ArgumentArrayBuilder getArgumentArrayBuilderForS3UnitTests(BaseSqoopTestCase testCase,
                                                                             S3CredentialGenerator s3CredentialGenerator) {
        ArgumentArrayBuilder builder = new ArgumentArrayBuilder();
        return builder.withCommonHadoopFlags()
                .withProperty(Constants.ACCESS_KEY, s3CredentialGenerator.getS3AccessKey())
                .withProperty(Constants.SECRET_KEY, s3CredentialGenerator.getS3SecretKey())
                .withProperty(Constants.SESSION_TOKEN, s3CredentialGenerator.getS3SessionToken())
                .withProperty(Constants.AWS_CREDENTIALS_PROVIDER, getTemporaryCredentialsProviderClass())
                .withOption("connect", testCase.getConnectString())
                .withOption("num-mappers", "1")
                .withOption("table", testCase.getTableName())
                .withOption("target-dir", getTargetDirPath().toString());
    }

    public static ArgumentArrayBuilder getArgumentArrayBuilderForHadoopCredProviderS3UnitTests(BaseSqoopTestCase testCase) {

        ArgumentArrayBuilder builder = new ArgumentArrayBuilder();
        return builder.withCommonHadoopFlags()
                .withProperty("fs.s3a.impl.disable.cache", "true")
                .withProperty(Constants.AWS_CREDENTIALS_PROVIDER, getTemporaryCredentialsProviderClass())
                .withOption("connect", testCase.getConnectString())
                .withOption("num-mappers", "1")
                .withOption("table", testCase.getTableName())
                .withOption("target-dir", getTargetDirPath().toString());
    }

    public static ArgumentArrayBuilder getArgumentArrayBuilderForS3UnitTestsWithFileFormatOption(BaseSqoopTestCase testCase,
                                                                                                 S3CredentialGenerator s3CredentialGenerator,
                                                                                                 String fileFormat) {
        ArgumentArrayBuilder builder = getArgumentArrayBuilderForS3UnitTests(testCase, s3CredentialGenerator);
        builder.withOption(fileFormat);
        return builder;
    }

    public static String[] getArgsForS3UnitTestsWithFileFormatOption(BaseSqoopTestCase testCase,
                                                                     S3CredentialGenerator s3CredentialGenerator,
                                                                     String fileFormat) {
        ArgumentArrayBuilder builder = getArgumentArrayBuilderForS3UnitTests(testCase, s3CredentialGenerator);
        builder.withOption(fileFormat);
        return builder.build();
    }

    public static ArgumentArrayBuilder addExternalHiveTableImportArgs(ArgumentArrayBuilder builder,
                                                                      String hs2Url) {
        return builder
                .withOption("hive-import")
                .withOption("hs2-url", hs2Url)
                .withOption("external-table-dir", getExternalTableDirPath().toString());
    }

    public static ArgumentArrayBuilder addCreateHiveTableArgs(ArgumentArrayBuilder builder) {
        return builder
                .withOption("create-hive-table")
                .withOption("hive-table", HIVE_EXTERNAL_TABLE_NAME);
    }

    private static Path getTemporaryRootDirPath() {
        return new Path(getTargetDirPath().toString() + TEMPORARY_ROOTDIR_SUFFIX);
    }

    public static ArgumentArrayBuilder addIncrementalAppendImportArgs(ArgumentArrayBuilder builder) {
        return builder
                .withOption("incremental", "append")
                .withOption("check-column", "ID")
                .withOption("last-value", "4")
                .withOption("temporary-rootdir", getTemporaryRootDirPath().toString());
    }

    public static ArgumentArrayBuilder addIncrementalMergeImportArgs(ArgumentArrayBuilder builder) {
        return builder
                .withOption("incremental", "lastmodified")
                .withOption("check-column", "RECORD_DATE")
                .withOption("merge-key", "DEBUT")
                .withOption("last-value", INITIAL_TIMESTAMP_FOR_MERGE)
                .withOption("temporary-rootdir", getTemporaryRootDirPath().toString());
    }

    private static List<String[]> getInputData() {
        List<String[]> data = new ArrayList<>();
        data.add(new String[]{"1", "'Ironman'", "'Marvel'", "1963"});
        data.add(new String[]{"2", "'Wonder Woman'", "'DC'", "1941"});
        data.add(new String[]{"3", "'Batman'", "'DC'", "1939"});
        data.add(new String[]{"4", "'Hulk'", "'Marvel'", "1962"});
        return data;
    }

    public static String[] getExtraInputData() {
        return new String[]{"5", "'Black Widow'", "'Marvel'", "1964"};
    }

    public static List<List<Object>> getInitialInputDataForMerge() {
        return Arrays.<List<Object>>asList(
            Arrays.<Object>asList(1940, "Black Widow", "Falcon", INITIAL_TIMESTAMP_FOR_MERGE),
            Arrays.<Object>asList(1974, "Iron Fist", "The Punisher", INITIAL_TIMESTAMP_FOR_MERGE));
    }

    public static List<List<Object>> getNewInputDataForMerge() {
        return Arrays.<List<Object>>asList(
                Arrays.<Object>asList(1962, "Spiderman", "Thor", NEW_TIMESTAMP_FOR_MERGE),
                Arrays.<Object>asList(1974, "Wolverine", "The Punisher", NEW_TIMESTAMP_FOR_MERGE));
    }

    public static void createTestTableFromInputData(BaseSqoopTestCase testCase) {
        List<String[]> inputData = getInputData();
        testCase.createTableWithColTypesAndNames(COLUMN_NAMES, COLUMN_TYPES, new String[0]);
        for (String[] dataRow : inputData) {
            insertInputDataIntoTable(testCase, dataRow);
        }
    }

    public static void insertInputDataIntoTable(BaseSqoopTestCase testCase, String[] inputData) {
        testCase.insertIntoTable(COLUMN_NAMES, COLUMN_TYPES, inputData);
    }

    public static void createTestTableFromInitialInputDataForMerge(BaseSqoopTestCase testCase) {
        testCase.createTableWithRecordsWithColTypesAndNames(COLUMN_NAMES_FOR_MERGE, COLUMN_TYPES_FOR_MERGE, getInitialInputDataForMerge());
    }

    public static void insertInputDataIntoTableForMerge(BaseSqoopTestCase testCase, List<List<Object>> inputData) {
        testCase.insertRecordsIntoTableWithColTypesAndNames(COLUMN_NAMES_FOR_MERGE, COLUMN_TYPES_FOR_MERGE, inputData);
    }

    public static String[] getExpectedTextOutput() {
        return new String[] {
                "1,Ironman,Marvel,1963",
                "2,Wonder Woman,DC,1941",
                "3,Batman,DC,1939",
                "4,Hulk,Marvel,1962"
        };
    }

    public static List<String> getExpectedTextOutputAsList() {
        return Arrays.asList(getExpectedTextOutput());
    }

    public static String[] getExpectedExtraTextOutput() {
        return new String[] {
                "5,Black Widow,Marvel,1964"
        };
    }

    public static String[] getExpectedTextOutputBeforeMerge() {
        return new String[] {
                "1940,Black Widow,Falcon," + EXPECTED_INITIAL_TIMESTAMP_FOR_MERGE,
                "1974,Iron Fist,The Punisher," + EXPECTED_INITIAL_TIMESTAMP_FOR_MERGE
        };
    }

    public static String[] getExpectedTextOutputAfterMerge() {
        return new String[] {
                "1940,Black Widow,Falcon," + EXPECTED_INITIAL_TIMESTAMP_FOR_MERGE,
                "1962,Spiderman,Thor," + EXPECTED_NEW_TIMESTAMP_FOR_MERGE,
                "1974,Wolverine,The Punisher," + EXPECTED_NEW_TIMESTAMP_FOR_MERGE
        };
    }

    public static String[] getExpectedSequenceFileOutput() {
        return new String[] {
                "1,Ironman,Marvel,1963\n",
                "2,Wonder Woman,DC,1941\n",
                "3,Batman,DC,1939\n",
                "4,Hulk,Marvel,1962\n"
        };
    }

    public static String[] getExpectedExtraSequenceFileOutput() {
        return new String[] {
                "5,Black Widow,Marvel,1964\n"
        };
    }

    public static String[] getExpectedAvroOutput() {
        return new String[] {
                "{\"ID\": 1, \"SUPERHERO\": \"Ironman\", \"COMICS\": \"Marvel\", \"DEBUT\": 1963}",
                "{\"ID\": 2, \"SUPERHERO\": \"Wonder Woman\", \"COMICS\": \"DC\", \"DEBUT\": 1941}",
                "{\"ID\": 3, \"SUPERHERO\": \"Batman\", \"COMICS\": \"DC\", \"DEBUT\": 1939}",
                "{\"ID\": 4, \"SUPERHERO\": \"Hulk\", \"COMICS\": \"Marvel\", \"DEBUT\": 1962}"
        };
    }

    public static String[] getExpectedExtraAvroOutput() {
        return new String[] {
                "{\"ID\": 5, \"SUPERHERO\": \"Black Widow\", \"COMICS\": \"Marvel\", \"DEBUT\": 1964}"
        };
    }

    public static List<String> getExpectedParquetOutput() {
        return asList(
                "1,Ironman,Marvel,1963",
                "2,Wonder Woman,DC,1941",
                "3,Batman,DC,1939",
                "4,Hulk,Marvel,1962");
    }

    public static List<String> getExpectedParquetOutputAfterAppend() {
        return asList(
                "1,Ironman,Marvel,1963",
                "2,Wonder Woman,DC,1941",
                "3,Batman,DC,1939",
                "4,Hulk,Marvel,1962",
                "5,Black Widow,Marvel,1964");
    }

    public static List<String> getExpectedParquetOutputWithTimestampColumn(BaseSqoopTestCase testCase) {
        return asList(
                "1940,Black Widow,Falcon," + testCase.timeFromString(INITIAL_TIMESTAMP_FOR_MERGE),
                "1974,Iron Fist,The Punisher," + testCase.timeFromString(INITIAL_TIMESTAMP_FOR_MERGE));
    }

    public static List<String> getExpectedParquetOutputWithTimestampColumnAfterMerge(BaseSqoopTestCase testCase) {
        return asList(
                "1940,Black Widow,Falcon," + testCase.timeFromString(INITIAL_TIMESTAMP_FOR_MERGE),
                "1962,Spiderman,Thor," + testCase.timeFromString(NEW_TIMESTAMP_FOR_MERGE),
                "1974,Wolverine,The Punisher," + testCase.timeFromString(NEW_TIMESTAMP_FOR_MERGE));
    }

    public static void failIfOutputFilePathContainingPatternExists(FileSystem s3Client, String pattern) throws IOException {
        List<Path> outputFilesWithPathContainingPattern = FileSystemUtil.findFilesWithPathContainingPattern(getTargetDirPath(),
                s3Client.getConf(), pattern);
        if (outputFilesWithPathContainingPattern.size() != 0) {
            fail("No output file was expected with pattern" + pattern);
        }
    }

    public static void failIfOutputFilePathContainingPatternDoesNotExists(FileSystem s3Client, String pattern) throws IOException {
        List<Path> outputFilesWithPathContainingPattern = FileSystemUtil.findFilesWithPathContainingPattern(getTargetDirPath(),
                s3Client.getConf(), pattern);
        if (outputFilesWithPathContainingPattern.size() == 0) {
            fail("No output file was found with pattern" + pattern);
        }
    }

    public static void cleanUpDirectory(FileSystem s3Client, Path directoryPath) {

        try {
            if (s3Client.exists(directoryPath)) {
                s3Client.delete(directoryPath, true);
            }
        } catch (Exception e) {
            LOG.error("Issue with cleaning up directory", e);
        }
    }

    private static void cleanUpTargetDir(FileSystem s3Client) {
        cleanUpDirectory(s3Client, getTargetDirPath());
        resetTargetDirName();
    }

    public static void tearDownS3ImportTestCase(FileSystem s3Client) {
        cleanUpTargetDir(s3Client);
    }

    public static void tearDownS3IncrementalImportTestCase(FileSystem s3Client) {
        cleanUpTargetDir(s3Client);
        cleanUpDirectory(s3Client, getTemporaryRootDirPath());
        System.clearProperty(MAPREDUCE_OUTPUT_BASENAME_PROPERTY);
    }

    public static void tearDownS3ExternalHiveTableImportTestCase(FileSystem s3Client) {
        cleanUpTargetDir(s3Client);
        cleanUpDirectory(s3Client, getExternalTableDirPath());
        resetHiveExternalDirName();
    }
}
