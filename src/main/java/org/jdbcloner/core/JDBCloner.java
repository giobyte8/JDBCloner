package org.jdbcloner.core;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
     * Copy rows from one table to another. Please verify that origin and target
     * tables both has same structure to avoid conflicts
     * 
     * @param sourceTableName
     * @param targetTableName
     * @param sourceJTemp
     * @param targetJTemp
     * 
     * @return true if copy success | false
     */
    public boolean cloneRows(String sourceTableName, String targetTableName,
            JdbcTemplate sourceJTemp, JdbcTemplate targetJTemp)
    {
        int originRowsCount = getTableCount(sourceJTemp, sourceTableName);
        if(originRowsCount <= 1000000)
        {
            System.out.println("\nJDBCloner now preparing to copy table rows ...");
            List<String> sourceColumNames = getTableColumns(sourceJTemp, sourceTableName);
            List<String> targetColumnNames = getTableColumns(targetJTemp, targetTableName);
            if(sourceColumNames.size() != targetColumnNames.size())
            {
                System.out.println("The number of columns in Source table is diferent"
                        + " to number of columns in Target table");
                return false;
            }
            else
            {
                System.out.println("JDBCloner now goin to copy: " + originRowsCount + " records");
                List<Map<String, Object>> rows = sourceJTemp.queryForList("SELECT * FROM " + sourceTableName);
                
                // Prepare insert SQL
                StringBuilder insertSQL = new StringBuilder("INSERT INTO " + targetTableName + " VALUES(");
                for (int i=0; i<sourceColumNames.size()-1; i++) 
                { 
                    insertSQL.append("?, "); 
                }
                insertSQL.append("?)");
                
                // INSERT ROWS
                int insertedRows = 0;
                Object[] rowParams = new Object[sourceColumNames.size()];
                for(Map<String, Object> row: rows)
                {
                    for(int i=0; i<sourceColumNames.size(); i++)
                    {
                        rowParams[i] = row.get(sourceColumNames.get(i));
                    }
                    targetJTemp.update(insertSQL.toString(), rowParams);
                    System.out.println("Inserted " + (++insertedRows) + " of " + originRowsCount);
                }
                System.out.println("\nINSERTED " + insertedRows + " OF " + originRowsCount);
            }
        }
        else
        {
            System.out.println("Wow !! It seems that " + sourceTableName + " contains more than");
            System.out.println("1 million rows. JDBCloner only copy <1000000 of rows");
            
            System.out.println("\nPlease be careful with big databases.");
            System.out.println("Information is very valuable");
            return false;
        }
        
        return true;
    }
    
    public void cloneTable(String sourceTableName, String targetTableName,
            JdbcTemplate sourceJTemp, JdbcTemplate targetJTemp)
    {
        // TODO Currently dont works (Make this works
        try
        {
            DatabaseMetaData metadata = sourceJTemp.getDataSource().getConnection().getMetaData();
            ResultSet rs = metadata.getTables(null, null, null, null);
            System.out.println("Schemas: ");
            while(rs.next())
            {
                System.out.print(" ** ");
                System.out.println(rs.getString(3));
            }
        } 
        catch (SQLException e)
        {
            System.err.println("Fatal error fetching metadata from databases.");
            e.printStackTrace();
        }
    }
	
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
			
			rs.close();
			metadata.getConnection().close();
		} 
		catch (SQLException e) 
		{
			System.err.println("Error ocurred while accessing tables names");
			e.printStackTrace();
		}
		
		return tablesNames;
	}
	
	public List<String> getTablesBySchema(JdbcTemplate jdbcTemplate, String schema)
	{
	    List<String> tables = new ArrayList<String>();
	    
	    try
	    {
	        DatabaseMetaData metadata = jdbcTemplate.getDataSource().getConnection().getMetaData();
	        ResultSet rs = metadata.getTables(null, "GES_VIN", null, null);
	        while(rs.next())
	        {
	            tables.add(rs.getString(3));
	        }
	        
	        rs.close();
	        metadata.getConnection().close();
	    }
	    catch(SQLException e)
	    {
	        System.err.println("Error ocurred while accessing tables names by schema");
	        e.printStackTrace();
	    }
	    
	    return tables;
	}
	
	/**
	 * Gets a list of schemas in database used by JDBCTemplate
	 * 
	 * @param jdbcTemplate
	 * @return <code>java.util.List</code> with schemas Names
	 */
	public List<String> getSchemasNames(JdbcTemplate jdbcTemplate)
	{
	    List<String> schemasNames = new ArrayList<String>();
	    
	    try
	    {
	        DatabaseMetaData metadata = jdbcTemplate.getDataSource().getConnection().getMetaData();
	        ResultSet rs = metadata.getSchemas();
	        while(rs.next())
	        {
	            schemasNames.add(rs.getString(1));
	        }
	        
	        rs.close();
	        metadata.getConnection().close();
	    }
	    catch(SQLException e)
	    {
	        System.err.println("Error ocurred while accessing schemas names");
	        e.printStackTrace();
	    }
	    
	    return schemasNames;
	}

	/**
	 * Counts the rows in given table.
	 * Please ensure that given jdbctemplate contains given table
	 * @param jdbcTemplate
	 * @param TableName
	 * @return Number of rows | -1 if any error
	 */
	public int getTableCount(JdbcTemplate jdbcTemplate, String tableName)
	{
	    final String sql = "SELECT COUNT(*) FROM " + tableName;
	    return jdbcTemplate.queryForObject(sql, Integer.class);
	}
	
	/**
	 * Get the name of columns for given table
	 * @param jdbcTemplate
	 * @param tableName
	 * @return A List with columns names | null if SQLException
	 */
	public List<String> getTableColumns(JdbcTemplate jdbcTemplate, String tableName)
	{
        try
        {
            List<String> columns = new ArrayList<>();
            Connection con = jdbcTemplate.getDataSource().getConnection();
            Statement  st  = con.createStatement();  
            ResultSet  rs  = st.executeQuery("SELECT * FROM " + tableName + " LIMIT 1");
            
            int columnCount = rs.getMetaData().getColumnCount();
            for(int i=1; i<=columnCount; i++)
            {
                columns.add(rs.getMetaData().getColumnName(i));
            }
            
            st.close();
            con.close();
            
            return columns;
        } 
        catch (SQLException e)
        {
            System.err.println("Fatal error while geting column names");
            e.printStackTrace();
            return null;
        }
	}
	
	/**
	 * Get all the rows in given table
	 * 
	 * @param jdbcTemplate
	 * @param table
	 * @return
	 */
	public List<Map<String, Object>> getRowsInTable(JdbcTemplate jdbcTemplate, String table)
	{
	    return jdbcTemplate.queryForList("SELECT * FROM " + table);
	}
}
