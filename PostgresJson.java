package com.oe.basic;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class PostgresJson 
{
	public static void main(String[] args) throws SQLException, JsonGenerationException, JsonMappingException, IOException 
	{
		    Connection conn = null;
			Statement stmt=null;
			ResultSet rs = null;
			String url = "jdbc:postgresql://localhost/testdb?user=postgres&password=password";
			String sql = "select * from sample_d";
			try 
			{
				try {
					Class.forName("org.postgresql.Driver");
				} catch (ClassNotFoundException e) 
				  {
					e.printStackTrace();
				  } //Check for driver
				
	//		DriverManager.getConnection(url).createStatement().executeQuery(sql)
				conn = DriverManager.getConnection(url); //connection
				System.out.println("Opened database successfully");
				stmt =conn.createStatement(); //connection to statement
				rs = stmt.executeQuery(sql); 
	//-----------------------------------------------------------------------------------------------
				
		                List<Map<String, Object>>  contents = getEntitiesFromResultSet(rs);  //get 
		                ObjectMapper mapper = new ObjectMapper();   // Jack json library

		                String json = mapper.writeValueAsString(contents);
		                System.out.println(json);
		                /*Final Output: [{"age":25,"name":"Dinesh"},
		                 * {"age":29,"name":"Aarya"},
		                 * {"age":26,"name":"arjun"},
		                 * {"age":30,"name":"Drona"}] 
		                 * */

		     } catch (SQLException e) {
		                throw (e);
		       } finally {
		                if (rs != null) {
		                    rs.close();
		                }
		          }
	}
	//--------------------------------------------------------------
    protected static List<Map<String, Object>> getEntitiesFromResultSet(ResultSet resultSet) throws SQLException 
    {
        ArrayList<Map<String, Object>> entities = new ArrayList<Map<String, Object>>();
        while (resultSet.next()) {
            entities.add(getEntityFromResultSet(resultSet));
        }
        //System.out.println(entities);
        return entities;
        /* output:[{age=25, name=Dinesh},
         *  {age=29, name=Aarya}, 
         *  {age=26, name=arjun}, 
         *  {age=30, name=Drona}]
        */
    }
	
	private static Map<String, Object> getEntityFromResultSet(ResultSet rs1) throws SQLException 
	{
		 ResultSetMetaData metaData = rs1.getMetaData();
	        int columnCount = metaData.getColumnCount();
	        Map<String,Object> resultsMap = new HashMap<String,Object>();
	        for (int i = 1; i <= columnCount; ++i) {
	            String columnName = metaData.getColumnName(i).toLowerCase();
	            Object object = rs1.getObject(i);
	            resultsMap.put(columnName, object);
	        }
	        //System.out.println(resultsMap);
	        return resultsMap;	
	        /* output:
	           {age=25, name=Dinesh}
				{age=29, name=Aarya}
				{age=26, name=arjun}
				{age=30, name=Drona} */
	}
}
