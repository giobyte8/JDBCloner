package org.jdbcloner.core;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Abstraction layer of frequent operations on databases
 * 
 * @author Giovanni Aguirre
 * @Created on: Jul 19, 2014
 */
public class JDBCloner 
{
	
	/**
	 * Gets a list of tables in database used by JDBCTemplate
	 * 
	 * @param jdbcTemplate an yet configured jdbcTemplate, you can get one from
	 *         <code>ConnectionUtils</code> class
	 *         
	 * @return <code>java.util.List</code> with Tables names.
	 */
	public List<String> getTablesNames(JdbcTemplate jdbcTemplate)
	{
		List<String> tablesNames = new ArrayList<String>();
		
		try 
		{
			DatabaseMetaData metadata = jdbcTemplate.getDataSource().getConnection().getMetaData();
			ResultSet rs = metadata.getTables(null, null, null, null);
			while(rs.next())
			{
				tablesNames.add(rs.getString(3));
			}
		} 
		catch (SQLException e) 
		{
			System.err.println("Error ocurred while accessing tables names");
			e.printStackTrace();
		}
		
		return tablesNames;
	}

}
