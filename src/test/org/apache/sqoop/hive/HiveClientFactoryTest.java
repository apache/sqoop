package org.apache.sqoop.hive;

import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.db.decorator.KerberizedConnectionFactoryDecorator;
import org.apache.sqoop.hive.hiveserver2.HiveServer2ConnectionFactory;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

public class HiveClientFactoryTest {
  
  private static final String TEST_HS2_URL = "jdbc:hive2://myhost:10000/default";

  private static final String TEST_HS2_USER = "testuser";

  private static final String TEST_HS2_KEYTAB = "testkeytab";
  
  private HiveClientFactory hiveClientFactory;
  
  @Before
  public void before() {
    hiveClientFactory = new HiveClientFactory();
  }
  
  @Test
  public void createHiveClient() throws Exception {
  }

  @Test
  public void testCreateJdbcConnectionFactoryWithoutKerberosConfiguredReturnsHiveServer2ConnectionFactoryWithCorrectUrl() throws Exception {
    SqoopOptions sqoopOptions = new SqoopOptions();
    sqoopOptions.setHs2Url(TEST_HS2_URL);
    HiveServer2ConnectionFactory connectionFactory = (HiveServer2ConnectionFactory) hiveClientFactory.createJdbcConnectionFactory(sqoopOptions);
    assertEquals(TEST_HS2_URL, connectionFactory.getConnectionString());
  }

  @Test
  public void testCreateJdbcConnectionFactoryWithKerberosConfiguredReturnsDecoratedHiveServer2ConnectionFactory() throws Exception {
    SqoopOptions sqoopOptions = createSqoopOptionsWithKerberosOptions();
    KerberizedConnectionFactoryDecorator connectionFactory = (KerberizedConnectionFactoryDecorator) hiveClientFactory.createJdbcConnectionFactory(sqoopOptions);
    
    assertThat(connectionFactory.getDecorated(), instanceOf(HiveServer2ConnectionFactory.class));
  }
  
  private SqoopOptions createSqoopOptionsWithKerberosOptions() {
    SqoopOptions sqoopOptions = new SqoopOptions();
    sqoopOptions.setHs2Url(TEST_HS2_URL);
    sqoopOptions.setHs2User(TEST_HS2_USER);
    sqoopOptions.setHs2Keytab(TEST_HS2_KEYTAB);
    return sqoopOptions;
  }

}