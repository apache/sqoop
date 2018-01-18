package org.apache.sqoop.hive;

import com.cloudera.sqoop.testutil.ImportJobTestCase;
import org.apache.commons.lang3.StringUtils;
import org.apache.sqoop.hive.minicluster.HiveMiniCluster;
import org.apache.sqoop.hive.minicluster.NoAuthenticationConfiguration;
import org.apache.sqoop.testutil.ArgumentArrayBuilder;
import org.apache.sqoop.testutil.HS2TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestHS2Import extends ImportJobTestCase {

  private HiveMiniCluster hiveMiniCluster;

  private HS2TestUtil hs2TestUtil;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    hiveMiniCluster = new HiveMiniCluster(new NoAuthenticationConfiguration());
    hiveMiniCluster.start();
    hs2TestUtil = new HS2TestUtil(hiveMiniCluster.getUrl());
  }

  @Override
  @After
  public void tearDown() {
    super.tearDown();
    hiveMiniCluster.stop();
  }

  @Test
  public void testImport() throws Exception {
    List<Object> columnValues = Arrays.<Object>asList("test", 42, "somestring");

    String[] types = {"VARCHAR(32)", "INTEGER", "CHAR(64)"};
    createTableWithColTypes(types, toStringArray(columnValues));

    String[] args = new ArgumentArrayBuilder()
        .withOption("connect", getConnectString())
        .withOption("table", getTableName())
        .withOption("hive-import")
        .withOption("hs2-url", hiveMiniCluster.getUrl())
        .withOption("split-by", getColName(1))
        .build();

    runImport(args);

    List<List<Object>> rows = hs2TestUtil.loadRawRowsFromTable(getTableName());
    assertEquals(columnValues, rows.get(0));
  }

  private String[] toStringArray(List<Object> columnValues) {
    String[] result = new String[columnValues.size()];

    for (int i = 0; i < columnValues.size(); i++) {
      if (columnValues.get(i) instanceof String) {
        result[i] = StringUtils.wrap((String) columnValues.get(i), '\'');
      } else {
        result[i] = columnValues.get(i).toString();
      }
    }

    return result;
  }

}
