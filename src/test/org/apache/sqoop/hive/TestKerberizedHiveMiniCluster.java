package org.apache.sqoop.hive;

import org.apache.sqoop.db.JdbcConnectionFactory;
import org.apache.sqoop.hive.hiveserver2.HiveServer2ConnectionFactory;
import org.apache.sqoop.hive.minicluster.HiveMiniCluster;
import org.apache.sqoop.hive.minicluster.KerberosAuthenticationConfiguration;
import org.apache.sqoop.infrastructure.kerberos.MiniKdcInfrastructureRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class TestKerberizedHiveMiniCluster {

  private HiveMiniCluster hiveMiniCluster;

  private JdbcConnectionFactory connectionFactory;

  @ClassRule
  public static MiniKdcInfrastructureRule miniKdcInfrastructure = new MiniKdcInfrastructureRule();

  @Before
  public void before() {
    hiveMiniCluster = new HiveMiniCluster(new KerberosAuthenticationConfiguration(miniKdcInfrastructure));
    hiveMiniCluster.start();

    connectionFactory = new HiveServer2ConnectionFactory(hiveMiniCluster.getUrl(), null, null);
  }

  @Test
  public void test() {
    System.out.println("hello, world");
  }

}
