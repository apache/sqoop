package org.apache.sqoop.connector;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertEquals;
import org.apache.sqoop.utils.ClassUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.AssertJUnit;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Set;

public class TestConnectorManagerUtils {

  private String workingDir;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    workingDir = System.getProperty("user.dir");
  }

  @Test
  public void testGetConnectorJarsNullPath() {
    Set<File> files = ConnectorManagerUtils.getConnectorJars(null);
    assertNull(files);
  }

  @Test
  public void testGetConnectorJarsNonNullPath() {
    String path = workingDir + "/src/test/resources";
    Set<File> files = ConnectorManagerUtils.getConnectorJars(path);
    assertEquals(1, files.size());
  }

  @Test
  public void testIsConnectorJar() {
    String path = workingDir + "/src/test/resources/test-connector.jar";
    File connectorJar = new File(path);
    assertTrue(connectorJar.exists());
    assertTrue(ConnectorManagerUtils.isConnectorJar(connectorJar));
  }

  @Test
  public void testIsNotConnectorJar() {
    String path = workingDir + "/src/test/resources/test-non-connector.jar";
    File file = new File(path);
    assertTrue(file.exists());
    assertFalse(ConnectorManagerUtils.isConnectorJar(file));
  }

  @Test
  public void testAddExternalConnectorJarToClasspath() {
    String path = workingDir + "/src/test/resources";
    ConnectorManagerUtils.addExternalConnectorsJarsToClasspath(path);
    List<URL> urls = ConnectorManagerUtils.getConnectorConfigs();
    assertEquals(1, urls.size());
    ClassUtils.loadClass("org.apache.sqoop.connector.jdbc.GenericJdbcConnector");
  }

}
