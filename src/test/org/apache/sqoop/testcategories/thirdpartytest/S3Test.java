package org.apache.sqoop.testcategories.thirdpartytest;

/**
 * An S3 test shall test the integration with the Amazon S3 cloud service.
 * These tests also require AWS credentials to access S3 and they run only if these
 * credentials are provided via the -Ds3.generator.command=<credential-generator-command> property
 * as well as the target S3 location via the -Ds3.bucket.url=<bucket-url> property.
 */
public interface S3Test extends ThirdPartyTest {
}
