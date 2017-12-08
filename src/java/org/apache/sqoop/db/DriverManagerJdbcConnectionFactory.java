package org.apache.sqoop.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DriverManagerJdbcConnectionFactory implements JdbcConnectionFactory {

  private final String driverClass;
  private final String connectionString;
  private final String username;
  private final String password;
  private final Properties additionalProps;

  public DriverManagerJdbcConnectionFactory(String driverClass, String connectionString, String username,
                                            String password, Properties additionalProps) {
    this.driverClass = driverClass;
    this.connectionString = connectionString;
    this.username = username;
    this.password = password;
    this.additionalProps = additionalProps;
  }

  public DriverManagerJdbcConnectionFactory(String driverClass, String connectionString, String username, String password) {
    this(driverClass, connectionString, username, password, null);
  }

  @Override
  public Connection createConnection() throws SQLException {
    loadDriverClass();

    Properties connectionProperties = new Properties();
    if (username != null) {
      connectionProperties.put("user", username);
    }

    if (password != null) {
      connectionProperties.put("password", password);
    }

    connectionProperties.putAll(additionalProps);
    return DriverManager.getConnection(connectionString, connectionProperties);
  }

  private void loadDriverClass() {
    try {
      Class.forName(driverClass);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not load db driver class: " + driverClass);
    }
  }
}
