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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assume.assumeNotNull;

public class S3TestUtils {

    private static final String PROPERTY_GENERATOR_COMMAND = "s3.generator.command";
    private static final String PROPERTY_BUCKET_URL = "s3.bucket.url";

    private static final String TEMPORARY_CREDENTIALS_PROVIDER_CLASS = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";

    private static final String BUCKET_TEMP_DIR = "/tmp/";

    private static final String TARGET_DIR_NAME_PREFIX = "testdir";

    private static String targetDirName = TARGET_DIR_NAME_PREFIX;

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

    private static void setUniqueTargetDirName() {
        String uuid = UUID.randomUUID().toString();
        targetDirName = targetDirName + "-" + uuid;
    }

    public static void resetTargetDirName() {
        targetDirName = TARGET_DIR_NAME_PREFIX;
    }

    private static String getTargetDirName() {
        return targetDirName;
    }

    public static Path getTargetDirPath() {
        String targetPathString = getPropertyBucketUrl() + BUCKET_TEMP_DIR + getTargetDirName();
        return new Path(targetPathString);
    }

    public static void runTestCaseOnlyIfS3CredentialsAreSet(S3CredentialGenerator s3CredentialGenerator) {
        assumeNotNull(s3CredentialGenerator);
        assumeNotNull(s3CredentialGenerator.getS3AccessKey());
        assumeNotNull(s3CredentialGenerator.getS3SecretKey());
    }

    public static FileSystem setupS3ImportTestCase(S3CredentialGenerator s3CredentialGenerator,
                                             BaseSqoopTestCase testCase) throws IOException {
        createTestTableFromInputData(testCase);

        Configuration hadoopConf = new Configuration();
        S3TestUtils.setS3CredentialsInHadoopConf(hadoopConf, s3CredentialGenerator);
        FileSystem s3Client = FileSystem.get(hadoopConf);

        setUniqueTargetDirName();

        clearTargetDir(s3Client);

        return s3Client;
    }

    private static void setS3CredentialsInHadoopConf(Configuration hadoopConf,
                                                     S3CredentialGenerator s3CredentialGenerator) {
        hadoopConf.set("fs.defaultFS", getPropertyBucketUrl());
        hadoopConf.set(Constants.ACCESS_KEY, s3CredentialGenerator.getS3AccessKey());
        hadoopConf.set(Constants.SECRET_KEY, s3CredentialGenerator.getS3SecretKey());

        if (s3CredentialGenerator.getS3SessionToken() != null) {
            hadoopConf.set(Constants.SESSION_TOKEN, s3CredentialGenerator.getS3SessionToken());
            hadoopConf.set(Constants.AWS_CREDENTIALS_PROVIDER, TEMPORARY_CREDENTIALS_PROVIDER_CLASS);
        }
    }

    public static ArgumentArrayBuilder getArgumentArrayBuilderForUnitTests(BaseSqoopTestCase testCase,
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

    private static List<String[]> getInputData() {
        List<String[]> data = new ArrayList<>();
        data.add(new String[]{"1", "'Ironman'", "'Marvel'", "1963"});
        data.add(new String[]{"2", "'Wonder Woman'", "'DC'", "1941"});
        data.add(new String[]{"3", "'Batman'", "'DC'", "1939"});
        data.add(new String[]{"4", "'Hulk'", "'Marvel'", "1962"});
        return data;
    }

    private static void createTestTableFromInputData(BaseSqoopTestCase testCase) {
        String[] names = {"ID",  "SUPERHERO", "COMICS", "DEBUT"};
        String[] types = { "INT", "VARCHAR(25)", "VARCHAR(25)", "INT"};
        List<String[]> inputData = getInputData();
        testCase.createTableWithColTypesAndNames(names, types, new String[0]);
        testCase.insertIntoTable(names, types, inputData.get(0));
        testCase.insertIntoTable(names, types, inputData.get(1));
        testCase.insertIntoTable(names, types, inputData.get(2));
        testCase.insertIntoTable(names, types, inputData.get(3));
    }

    public static String[] getExpectedTextOutput() {
        return new String[] {
                "1,Ironman,Marvel,1963",
                "2,Wonder Woman,DC,1941",
                "3,Batman,DC,1939",
                "4,Hulk,Marvel,1962"
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

    public static String[] getExpectedAvroOutput() {
        return new String[] {
                "{\"ID\": 1, \"SUPERHERO\": \"Ironman\", \"COMICS\": \"Marvel\", \"DEBUT\": 1963}",
                "{\"ID\": 2, \"SUPERHERO\": \"Wonder Woman\", \"COMICS\": \"DC\", \"DEBUT\": 1941}",
                "{\"ID\": 3, \"SUPERHERO\": \"Batman\", \"COMICS\": \"DC\", \"DEBUT\": 1939}",
                "{\"ID\": 4, \"SUPERHERO\": \"Hulk\", \"COMICS\": \"Marvel\", \"DEBUT\": 1962}"
        };
    }

    public static void clearTargetDir(FileSystem s3Client) throws IOException{

        try {
            if (s3Client.exists(getTargetDirPath())) {
                s3Client.delete(getTargetDirPath(), true);
            }
        } catch (Exception e) {
            LOG.error("Issue with cleaning up output directory", e);
        }
    }
}
