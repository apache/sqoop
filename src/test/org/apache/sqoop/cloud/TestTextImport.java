package org.apache.sqoop.cloud;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.cloud.tools.CloudCredentialsRule;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.TextFileTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public abstract class TestTextImport extends CloudImportJobTestCase {

  public static final Log LOG = LogFactory.getLog(TestTextImport.class.getName());

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  protected TestTextImport(CloudCredentialsRule credentialsRule) {
    super(credentialsRule);
  }

  @Test
  public void testImportWithoutDeleteTargetDirOptionWhenTargetDirDoesNotExist() throws IOException {
    String[] args = getArgs(false);
    runImport(args);
    TextFileTestUtils.verify(getDataSet().getExpectedTextOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath());
  }

  @Test
  public void testImportWithDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws IOException {
    String[] args = getArgs(false);
    runImport(args);

    args = getArgsWithDeleteTargetOption(false);
    runImport(args);
    TextFileTestUtils.verify(getDataSet().getExpectedTextOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath());
  }

  @Test
  public void testImportWithoutDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws IOException {
    String[] args = getArgs(false);
    runImport(args);

    thrown.expect(IOException.class);
    runImport(args);
  }

  @Test
  public void testImportAsTextFile() throws IOException {
    String[] args = getArgs(true);
    runImport(args);
    TextFileTestUtils.verify(getDataSet().getExpectedTextOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath());
  }

  @Test
  public void testImportAsTextFileWithDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws IOException {
    String[] args = getArgs(true);
    runImport(args);

    args = getArgsWithDeleteTargetOption(true);
    runImport(args);
    TextFileTestUtils.verify(getDataSet().getExpectedTextOutput(), fileSystemRule.getCloudFileSystem(), fileSystemRule.getTargetDirPath());
  }

  @Test
  public void testImportAsTextFileWithoutDeleteTargetDirOptionWhenTargetDirAlreadyExists() throws IOException {
    String[] args = getArgs(true);
    runImport(args);

    thrown.expect(IOException.class);
    runImport(args);
  }

  private String[] getArgs(boolean withAsTextFileOption) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTests(fileSystemRule.getTargetDirPath().toString());
    if (withAsTextFileOption) {
      builder.withOption("as-textfile");
    }
    return builder.build();
  }

  private String[] getArgsWithDeleteTargetOption(boolean withAsTextFileOption) {
    ArgumentArrayBuilder builder = getArgumentArrayBuilderForUnitTests(fileSystemRule.getTargetDirPath().toString());
    builder.withOption("delete-target-dir");
    if (withAsTextFileOption) {
      builder.withOption("as-textfile");
    }
    return builder.build();
  }
}
