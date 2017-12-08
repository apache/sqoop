package org.apache.sqoop.hive.hiveserver2;

import org.apache.sqoop.db.DriverManagerJdbcConnectionFactory;

public class HiveServer2ConnectionFactory extends DriverManagerJdbcConnectionFactory {

    private static final String HS2_DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";

    public HiveServer2ConnectionFactory(String connectionString, String username, String password) {
        super(HS2_DRIVER_CLASS, connectionString, username, password);
    }
}
