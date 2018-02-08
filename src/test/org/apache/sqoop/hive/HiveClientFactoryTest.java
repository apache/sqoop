package org.apache.sqoop.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.authentication.KerberosAuthenticator;
import org.apache.sqoop.db.JdbcConnectionFactory;
import org.apache.sqoop.db.decorator.KerberizedConnectionFactoryDecorator;
import org.apache.sqoop.manager.ConnManager;
import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class HiveClientFactoryTest {
  
  private static final String TEST_HS2_URL = "jdbc:hive2://myhost:10000/default";

  private static final String TEST_HS2_USER = "testuser";

  private static final String TEST_HS2_KEYTAB = "testkeytab";
  
  private static final String TEST_TABLE_NAME = "testTableName";
  
  private static final String TEST_HIVE_TABLE_NAME = "testHiveTableName";
  
  private HiveClientFactory hiveClientFactory;
  
  private ConnManager connectionManager;
  
  private SqoopOptions sqoopOptions;
  
  private Configuration configuration;
  
  private JdbcConnectionFactory jdbcConnectionFactory;
  
  private TableDefWriter tableDefWriter;
  
  private KerberosAuthenticator kerberosAuthenticator;
  
  private SoftAssertions softly;
  
  @Before
  public void before() {
    hiveClientFactory = spy(new HiveClientFactory());
    softly = new SoftAssertions();
    
    connectionManager = mock(ConnManager.class);
    sqoopOptions = mock(SqoopOptions.class);
    configuration = mock(Configuration.class);
    jdbcConnectionFactory = mock(JdbcConnectionFactory.class);
    tableDefWriter = mock(TableDefWriter.class);
    kerberosAuthenticator = mock(KerberosAuthenticator.class);
    
    when(sqoopOptions.getConf()).thenReturn(configuration);
    when(sqoopOptions.getTableName()).thenReturn(TEST_TABLE_NAME);
    when(sqoopOptions.getHiveTableName()).thenReturn(TEST_HIVE_TABLE_NAME);
    when(sqoopOptions.getHs2User()).thenReturn(TEST_HS2_USER);
  }
  
  @Test
  public void testCreateHiveClientCreatesHiveImportWhenHs2UrlIsNotProvided() throws Exception {
    HiveClient hiveClient = hiveClientFactory.createHiveClient(sqoopOptions, connectionManager);
    assertThat(hiveClient, instanceOf(HiveImport.class));
  }
  
  @Test
  public void testCreateHiveClientInitializesHiveImportProperly() throws Exception {
    HiveImport hiveImport = (HiveImport) hiveClientFactory.createHiveClient(sqoopOptions, connectionManager);
    
    softly.assertThat(hiveImport.getOptions()).isSameAs(sqoopOptions);
    softly.assertThat(hiveImport.getConnManager()).isSameAs(connectionManager);
    softly.assertThat(hiveImport.getConfiguration()).isSameAs(configuration);
    softly.assertThat(hiveImport.isGenerateOnly()).isFalse();
    softly.assertAll();
  }
  
  @Test
  public void testCreateHiveClientCreatesHiveServer2ClientWhenHs2UrlIsProvided() throws Exception {
    when(sqoopOptions.getHs2Url()).thenReturn(TEST_HS2_URL);
    HiveClient hiveClient = hiveClientFactory.createHiveClient(sqoopOptions, connectionManager);
    assertThat(hiveClient, instanceOf(HiveServer2Client.class));
  }

  @Test
  public void testCreateHiveClientInitializesHiveServer2ClientProperly() throws Exception {
    when(sqoopOptions.getHs2Url()).thenReturn(TEST_HS2_URL);
    doReturn(jdbcConnectionFactory).when(hiveClientFactory).createJdbcConnectionFactory(sqoopOptions);
    doReturn(tableDefWriter).when(hiveClientFactory).createTableDefWriter(sqoopOptions, connectionManager);
    
    HiveServer2Client hs2Client = (HiveServer2Client) hiveClientFactory.createHiveClient(sqoopOptions, connectionManager);
    
    softly.assertThat(hs2Client.getSqoopOptions()).isSameAs(sqoopOptions);
    softly.assertThat(hs2Client.getHs2ConnectionFactory()).isSameAs(jdbcConnectionFactory);
    softly.assertThat(hs2Client.getTableDefWriter()).isSameAs(tableDefWriter);
    softly.assertAll();
  }
  
  @Test
  public void testCreateTableDefWriterInitializesFieldsProperly() {
    TableDefWriter tableDefWriter = hiveClientFactory.createTableDefWriter(sqoopOptions, connectionManager);

    softly.assertThat(tableDefWriter.getOptions()).isSameAs(sqoopOptions);
    softly.assertThat(tableDefWriter.getConnManager()).isSameAs(connectionManager);
    softly.assertThat(tableDefWriter.getInputTableName()).isEqualTo(TEST_TABLE_NAME);
    softly.assertThat(tableDefWriter.getOutputTableName()).isEqualTo(TEST_HIVE_TABLE_NAME);
    softly.assertThat(tableDefWriter.getConfiguration()).isSameAs(configuration);
    softly.assertThat(tableDefWriter.isCommentsEnabled()).isFalse();
    
    softly.assertAll();
  }
  
  @Test
  public void testCreateJdbcConnectionFactoryWithoutKerberosConfiguredReturnsHiveServer2ConnectionFactory() throws Exception {
    JdbcConnectionFactory connectionFactory = hiveClientFactory.createJdbcConnectionFactory(sqoopOptions);
    
    assertThat(connectionFactory, instanceOf(HiveServer2ConnectionFactory.class));
  }
  
  @Test
  public void testCreateJdbcConnectionFactoryInitializesHiveServer2ConnectionFactoryProperly() throws Exception {
    when(sqoopOptions.getHs2Url()).thenReturn(TEST_HS2_URL);
    HiveServer2ConnectionFactory connectionFactory = (HiveServer2ConnectionFactory) hiveClientFactory.createJdbcConnectionFactory(sqoopOptions);
    
    assertEquals(TEST_HS2_URL, connectionFactory.getConnectionString());
  }

  @Test
  public void testCreateJdbcConnectionFactoryWithKerberosConfiguredReturnsKerberizedConnectionFactoryDecorator() throws Exception {
    when(sqoopOptions.getHs2Keytab()).thenReturn(TEST_HS2_KEYTAB);
    
    JdbcConnectionFactory connectionFactory = hiveClientFactory.createJdbcConnectionFactory(sqoopOptions);
    
    assertThat(connectionFactory, instanceOf(KerberizedConnectionFactoryDecorator.class));
  }

  @Test
  public void testCreateJdbcConnectionFactoryWithKerberosConfiguredInitializesDecoratorProperly() throws Exception {
    when(sqoopOptions.getHs2Keytab()).thenReturn(TEST_HS2_KEYTAB);
    doReturn(kerberosAuthenticator).when(hiveClientFactory).createKerberosAuthenticator(sqoopOptions);
    
    KerberizedConnectionFactoryDecorator connectionFactory = (KerberizedConnectionFactoryDecorator) hiveClientFactory.createJdbcConnectionFactory(sqoopOptions);

    softly.assertThat(connectionFactory.getDecorated()).isInstanceOf(HiveServer2ConnectionFactory.class);
    softly.assertThat(connectionFactory.getAuthenticator()).isSameAs(kerberosAuthenticator);
    
    softly.assertAll();
  }

  @Test
  public void testCreateKerberosAuthenticatorInitializesFieldsProperly() {
    when(sqoopOptions.getHs2Keytab()).thenReturn(TEST_HS2_KEYTAB);
    
    KerberosAuthenticator kerberosAuthenticator = hiveClientFactory.createKerberosAuthenticator(sqoopOptions);

    softly.assertThat(kerberosAuthenticator.getConfiguration()).isSameAs(configuration);
    softly.assertThat(kerberosAuthenticator.getKeytabLocation()).isSameAs(TEST_HS2_KEYTAB);
    softly.assertThat(kerberosAuthenticator.getPrincipal()).isSameAs(TEST_HS2_USER);

    softly.assertAll();
  }
  
}