package org.apache.sqoop.tool;

import com.cloudera.sqoop.SqoopOptions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ImportToolValidateOptionsTest {

  private static final String TABLE_NAME = "testTableName";
  private static final String CONNECT_STRING = "testConnectString";
  private static final String CHECK_COLUMN_NAME = "checkColumnName";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private ImportTool importTool;

  @Before
  public void setup() {
    importTool = new ImportTool();
    importTool.extraArguments = new String[0];
  }

  @Test
  public void testValidationFailsWithHiveImportAndIncrementalLastmodified() throws Exception {
    SqoopOptions options = buildBaseSqoopOptions();
    options.setHiveImport(true);
    options.setIncrementalTestColumn(CHECK_COLUMN_NAME);
    options.setIncrementalMode(SqoopOptions.IncrementalMode.DateLastModified);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.expectMessage(BaseSqoopTool.HIVE_IMPORT_WITH_LASTMODIFIED_NOT_SUPPORTED);

    importTool.validateOptions(options);
  }

  /**
   * Note that append mode (--append) is designed to be used with HDFS import and not Hive import.
   * However this test case is added to make sure that the error message generated is correct even if --append is used.
   *
   */
  @Test
  public void testValidationFailsWithHiveImportAndAppendModeIncrementalLastmodified() throws Exception {
    SqoopOptions options = buildBaseSqoopOptions();
    options.setHiveImport(true);
    options.setIncrementalTestColumn(CHECK_COLUMN_NAME);
    options.setIncrementalMode(SqoopOptions.IncrementalMode.DateLastModified);
    options.setAppendMode(true);

    thrown.expect(SqoopOptions.InvalidOptionsException.class);
    thrown.expectMessage(BaseSqoopTool.HIVE_IMPORT_WITH_LASTMODIFIED_NOT_SUPPORTED);

    importTool.validateOptions(options);
  }

  @Test
  public void testValidationSucceedsWithHiveImportAndIncrementalAppendRows() throws Exception {
    SqoopOptions options = buildBaseSqoopOptions();
    options.setHiveImport(true);
    options.setIncrementalTestColumn(CHECK_COLUMN_NAME);
    options.setIncrementalMode(SqoopOptions.IncrementalMode.AppendRows);

    importTool.validateOptions(options);
  }

  /**
   * Note that append mode (--append) is designed to be used with HDFS import and not Hive import.
   * However this test case is added to make sure that SQOOP-2986 does not break the already existing validation.
   *
   */
  @Test
  public void testValidationSucceedsWithHiveImportAndAppendModeAndIncrementalAppendRows() throws Exception {
    SqoopOptions options = buildBaseSqoopOptions();
    options.setHiveImport(true);
    options.setIncrementalTestColumn(CHECK_COLUMN_NAME);
    options.setIncrementalMode(SqoopOptions.IncrementalMode.AppendRows);
    options.setAppendMode(true);

    importTool.validateOptions(options);
  }

  private SqoopOptions buildBaseSqoopOptions() {
    SqoopOptions result = new SqoopOptions();
    result.setTableName(TABLE_NAME);
    result.setConnectString(CONNECT_STRING);
    return result;
  }

}

