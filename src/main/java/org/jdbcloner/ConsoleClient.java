package org.jdbcloner;

import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.jdbcloner.core.ConnectionUtils;
import org.jdbcloner.core.JDBCloner;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Hello world!
 *
 */
public class ConsoleClient 
{
	Scanner scanner = new Scanner(System.in);
	
	ConnectionUtils connUtils = new ConnectionUtils();
	JDBCloner jdbCloner = new JDBCloner();
	
	private JdbcTemplate jTemplateSource;
	private JdbcTemplate jTemplateTarget;
	 
    public static void main( String[] args )
    {
    	System.out.println("\n*** *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** ***");
    	System.out.println("-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --");
    	
        System.out.println("\n\t             |JDBCLONER| - |DATABASE CLONER|" );
        System.out.println("\t ... Copy rows, tables or whole databases beetwen servers ...");
        
        System.out.println("\nFirst you most configure the source/origin\n"
        		+ "and target/destine database connections");
        
        ConsoleClient client = new ConsoleClient();
        client.configureSource();
        client.configureTarget();
        
        client.cloneMagic(); // Execute the magic functions
    }
    
    /**
     * Shows a menu asking to user his database provider.
     * @return
     */
    private String askDBProvider()
    {
        int option = -1;
        do
        {
            System.out.println("\nPLEASE SELECT A SUPPORTED DATABASE PROVIDER: ");
            System.out.println(" ** 1. Oracle");
            System.out.println(" ** 2. MySQL");
            System.out.println(" ** 3. HSQLDB");
            System.out.print("Select: ");
            
            String selection = scanner.next();
            try 
            {
                option = Integer.parseInt(selection);
                if( !(option>=1) && !(option<=3) )
                {
                    option = -1;
                }
            } 
            catch(Exception err) { }
        }while(option == -1);
        
        switch(option)
        {
            case 1: return "ORACLE";
            case 2: return "MySQL";
            case 3: return "HSQLDB";
            default: return "";
        }
    }
    
    /**
     * Ask for host, port, database, user and password for and database.
     * 
     * @return a String array [host, port, database, user, pass]
     */
    private String[] askConnParams()
    {
        System.out.println("Please provide required parameters ... ");
        
        System.out.print("** Database host: ");
        final String host = scanner.nextLine();
        
        System.out.print("** Database port: ");
        final String port = scanner.nextLine();
        
        System.out.print("** Database name: ");
        final String dbName = scanner.nextLine();
        
        System.out.print("** Database username: ");
        final String username = scanner.nextLine();
        
        System.out.print("** Database password: ");
        final String pass = scanner.nextLine();
        
        final String[] params = {host, port, dbName, username, pass};
        return params;
    }
    
    /**
     * Executes the main functions
     * 
     * Ensure that jTemplates is configured before call this method
     */
    private void cloneMagic() 
    {
    	do
    	{
    	    System.out.println("\n\n*** *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** *** ***");
    	    System.out.println("\nSELECT ONE FUNCTION FROM JDBCLONER");
    	    System.out.println(" 0. Exit JDBCloner");
    	    System.out.println(" 1. Show tables in Source/Origin database");
    	    System.out.println(" 2. Show tables in Target/Destine database");
    	    System.out.println(" 3. Copy single table structure [Origin to Target]");
    	    System.out.println(" 4. Copy single table structure AND ROWS [Origin to Target]");
    	    System.out.println(" 5. Copy rows from one table [Origin] to another in Destine [Target]");
    	    System.out.println(" 6. Show Schemas in Source/Origin database");
    	    System.out.println(" 7. Show Schemas in Target/Destine dtabase");
    	    System.out.println(" 8. Show tables names by Schema in Source/Origin [Only for ORACLE]");
    	    System.out.println(" 9. Show tables names by Schema in Target/Destine [Only for ORACLE]");
    	    System.out.println("10. Show rows in a table in Source/Origin database");
    	    System.out.println("11. Show rows in a table in Target/Destino database");
    	    
    	    System.out.print("Select: ");
    	    switch(scanner.nextInt())
    	    {
    	        case 0:
    	            System.out.println("\nHAPPY CODING !! ... BYE");
    	            System.exit(0);
    	            break;
    	        case 1:
    	            showTables(jTemplateSource);
    	            break;
    	        case 2:
    	            showTables(jTemplateTarget);
    	            break;
    	        case 3:
    	            System.out.print("Origin table name: ");
    	            String sourceTableName = scanner.next();
    	            
    	            System.out.print("Target table name: ");
                    String targetTableName = scanner.next();
                    
    	            jdbCloner.cloneTable(sourceTableName, targetTableName, jTemplateSource, jTemplateTarget);
    	            break;
    	        case 5:
    	            System.out.println("\nWARNING: Please ensure that both origin and target "
    	                    + "tables has same structure to avoid conflicts");
    	            
    	            System.out.print("Origin table name: ");
    	            String sourceTable = scanner.next();
    	            
    	            System.out.print("Target table name: ");
    	            String targetTable = scanner.next();
    	            
    	            jdbCloner.cloneRows(sourceTable, targetTable, jTemplateSource, jTemplateTarget);
    	            break;
    	        case 6:
    	            showSchemas(jTemplateSource);
    	            break;
    	        case 7:
    	            showSchemas(jTemplateTarget);
    	            break;
    	        case 8:
    	            System.out.print("Table name: ");
    	            String schema = scanner.next();
    	            showTablesBySchema(jTemplateSource, schema);
    	            break;
    	        case 9:
    	            System.out.print("Table name: ");
    	            String schema2 = scanner.next();
    	            showTablesBySchema(jTemplateTarget, schema2);
    	            break;
    	        case 10:
    	            System.out.print("Table name: ");
    	            String tableName = scanner.next();
    	            showRowsInTable(jTemplateSource, tableName);
    	        case 11:
    	            System.out.print("Table name: ");
                    String tableN = scanner.next();
                    showRowsInTable(jTemplateTarget, tableN);
    	        default:
    	            System.out.println("Please select a valid option.");
    	    }
    	    
    	    System.out.print("[TYPE A CHAR AND ENTER TO CONTINUE]");
    	    scanner.next();
    	    
    	}while(true);
	}

	/**
     * Configure the origin database connections
     */
    private void configureSource()
    {
    	do
    	{
    		System.out.println("\nLETS TO CONFIGURE THE SOURCE/ORIGIN DATABASE CONNECTION [PRESS ANY KEY]");
    		scanner.nextLine();
    		jTemplateSource = configureTemplate(jTemplateSource);
    		
    	}while(jTemplateSource == null);
    }
    
    private void configureTarget()
    {
    	do
    	{
    		System.out.println("\nLETS TO CONFIGURE THE TARGET/DESTINE DATABASE CONNECTION [PRESS ANY KEY]");
    		scanner.nextLine();
    		jTemplateTarget = configureTemplate(jTemplateTarget);
    		
    	}while(jTemplateTarget == null);
    }
    
    /**
     * Ask to user for connection params and configure and validate the given
     * jdbcTemplate
     * 
     * @param jTemplate
     * @return true if configuration successful | false otherwise
     */
    private JdbcTemplate configureTemplate(JdbcTemplate jTemplate)
    {
    	String[] params = askConnParams();
    	switch(askDBProvider())
    	{
    		case "ORACLE":
    		    jTemplate = connUtils.getOracleJDBCTemplate(params[3], params[4], 
    		            params[0], params[1], params[2]);
    			break;
    		case "MySQL":
    			jTemplate = connUtils.getMySQLJDBCTemplate(params[3], 
    					params[4], params[0], params[1], params[2]);
    			break;
    		default:
    			System.err.println("Unsupported database provider");
    			return null;
    	}
    	
	    return (connUtils.isValidTemplate(jTemplate)) ? jTemplate : null;
    }
    
    /**
     * Display a list of tables in given jTemplate connection
     * @param jTemplate
     */
    private void showTables(JdbcTemplate jTemplate)
    {
        System.out.println("\nTables in Database: ");
        for(String tableName : jdbCloner.getTablesNames(jTemplate))
        {
            System.out.println(" * " + tableName);
        }
    }
    
    /**
     * Display a list of tables in given jTemplate and given Schema [Oracle]
     * @param jTemplate
     * @param schemaName
     */
    private void showTablesBySchema(JdbcTemplate jTemplate, String schemaName)
    {
        System.out.println("\nTables in Database for schema: " + schemaName);
        for(String table : jdbCloner.getTablesBySchema(jTemplate, schemaName))
        {
            System.out.println(" * " + table);
        }
    }
    
    private void showSchemas(JdbcTemplate jTemplate)
    {
        System.out.println("\nSchemas in database");
        for(String schemaName : jdbCloner.getSchemasNames(jTemplate))
        {
            System.out.println(" * " + schemaName);
        }
    }
    
    private void showRowsInTable(JdbcTemplate jTemplate, String table)
    {
        System.out.println("\n\nRows in " + table.toUpperCase());
        
        List<String> columns = jdbCloner.getTableColumns(jTemplate, table);
        for(Map<String, Object> row : jdbCloner.getRowsInTable(jTemplate, table))
        {
            for(String column : columns)
            {
                System.out.print(row.get(column) + "\t");
            }
            System.out.println("\n");
        }
    }
    
}
