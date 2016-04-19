package org.jdbcloner.core;

import java.sql.SQLException;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * A stack of static utilities for work with databases
 * 
 * @author Giovanni Aguirre
 * @Created on: Jul 19, 2014
 */
public class ConnectionUtils 
{
	private final String MYSQL_DRIVER_CLASSNAME  = "com.mysql.jdbc.Driver";
	private final String ORACLE_DRIVER_CLASSNAME = "oracle.jdbc.driver.OracleDriver";
	
	
	/**
	 * Creates a new Spring JDBCTemplate and configure its DataSource
	 * with a MySQL Database.
	 * 
	 * @param user User to connect with database
	 * @param pass Password to create connection
	 * @param host IP or host name from MySQL Server
	 * @param port MySQL Server port
	 * @param database Database to connect name
	 * 
	 * @return Spring JDBCTemplate configured with DataSource
	 */
	public JdbcTemplate getMySQLJDBCTemplate(final String user, final String pass,
			final String host, final String port, final String database)
	{
		DriverManagerDataSource dSource = getMySQLDataSource(user, pass, host, port, database);
		JdbcTemplate jdbcTemplate = new JdbcTemplate(dSource);
		
		return jdbcTemplate;
	}
	
	/**
	 * Creates a new Spring JDBCTemplate and configure its DataSource
	 * with a Oracle Database
	 * 
	 * @param user User to connect with database
     * @param pass Password to create connection
     * @param host IP or host name from MySQL Server
     * @param port MySQL Server port
     * @param database Database to connect name
     * 
	 * @return Spring JDBCTemplate configured with DataSource
	 */
	public JdbcTemplate getOracleJDBCTemplate(final String user, final String pass,
            final String host, final String port, final String database)
    {
	    DriverManagerDataSource dSource = getOracleDataSource(user, pass, host, port, database);
	    JdbcTemplate jdbcTemplate = new JdbcTemplate(dSource);
	    
	    return jdbcTemplate;
    }
	
	/**
	 * Creates a DataSource with given parameters to a MySQL Database
	 * 
	 * @param user User to connect with database
	 * @param pass Password to create connection
	 * @param host IP or host name from MySQL Server
	 * @param port MySQL Server port
	 * @param database Database to connect name
	 * 
	 * @return
	 */
	public DriverManagerDataSource getMySQLDataSource(final String user, final String pass,
			final String host, final String port, final String database)
	{
		StringBuilder DB_URL = new StringBuilder("jdbc:mysql://")
			.append(host)
			.append(":")
			.append(port)
			.append("/")
			.append(database);
	
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(MYSQL_DRIVER_CLASSNAME);
		dataSource.setUrl(DB_URL.toString());
		dataSource.setUsername(user);
		dataSource.setPassword(pass);
		
		return dataSource;
	}
	
	/**
	 * Creates a DataSource with given params to an Oracle Database
	 * 
	 * @param username
	 * @param password
	 * @param host
	 * @param port
	 * @param database
	 * @return
	 */
	public DriverManagerDataSource getOracleDataSource(final String username, final String password,
	        final String host, final String port, final String database)
	{
	    StringBuilder DB_URL = new StringBuilder("jdbc:oracle:thin:@")
	        .append(host)
	        .append(":")
	        .append(port)
	        .append(":")
	        .append(database);
	    
	    DriverManagerDataSource dataSource = new DriverManagerDataSource();
	    dataSource.setDriverClassName(ORACLE_DRIVER_CLASSNAME);
	    dataSource.setUrl(DB_URL.toString());
	    dataSource.setUsername(username);
	    dataSource.setPassword(password);
	        
	    return dataSource;
	}

	/**
	 * Validate is jdbcTemplate can provide connections to its database
	 * @param jdbcTemplate
	 * @return true is connection successfully | false otherwise
	 */
	public boolean isValidTemplate(JdbcTemplate jdbcTemplate)
	{
		System.out.println("Validating connection params ...");
		try
		{
			jdbcTemplate.getDataSource().getConnection();
			System.out.println("Connection Successfull");
			return true;
		} 
		catch(SQLException err)
		{
			System.err.println("Can not get database connection with given parameters |||");
			return false;
		}
	}
}
