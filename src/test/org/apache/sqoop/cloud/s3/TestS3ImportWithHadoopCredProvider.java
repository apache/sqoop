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

package org.apache.sqoop.cloud.s3;

import org.apache.sqoop.cloud.AbstractTestImportWithHadoopCredProvider;
import org.apache.sqoop.testcategories.thirdpartytest.S3Test;
import org.apache.sqoop.util.BlockJUnit4ClassRunnerWithParametersFactory;
import org.apache.sqoop.util.password.CredentialProviderHelper;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@Category(S3Test.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(BlockJUnit4ClassRunnerWithParametersFactory.class)
public class TestS3ImportWithHadoopCredProvider extends AbstractTestImportWithHadoopCredProvider {

  @Parameterized.Parameters(name = "credentialProviderPathProperty = {0}")
  public static Iterable<? extends Object> parameters() {
    return Arrays.asList(CredentialProviderHelper.HADOOP_CREDENTIAL_PROVIDER_PATH,
        CredentialProviderHelper.S3A_CREDENTIAL_PROVIDER_PATH);
  }

  @ClassRule
  public static S3CredentialsRule s3CredentialsRule = new S3CredentialsRule();

  static {
    AbstractTestImportWithHadoopCredProvider.credentialsRule = s3CredentialsRule;
  }

  public TestS3ImportWithHadoopCredProvider(String credentialProviderPathProperty) {
    super(credentialProviderPathProperty);
  }
}
