package org.apache.sqoop.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DriverManagerConnectionFactory implements ConnectionFactory {

  private final String driverName;
  private final String jdbcString;
  private final String username;
  private final String password;
  private final Properties additionalProps;

  public DriverManagerConnectionFactory(String driverName, String jdbcString, String username,
                                        String password, Properties additionalProps) {
    this.driverName = driverName;
    this.jdbcString = jdbcString;
    this.username = username;
    this.password = password;
    this.additionalProps = additionalProps;
  }

  public DriverManagerConnectionFactory(String driverName, String jdbcString, String username, Properties additionalProps) {
    this(driverName, jdbcString, username, null, additionalProps);
  }

  @Override
  public Connection createConnection() throws SQLException {
    Connection connection;

    loadDriverClass();

    Properties props = new Properties();
    if (username != null) {
      props.put("user", username);
    }

    if (password != null) {
      props.put("password", password);
    }

    props.putAll(additionalProps);
    connection = DriverManager.getConnection(jdbcString, props);

    return connection;
  }

  private void loadDriverClass() {
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not load db driver class: " + driverName);
    }
  }
}
