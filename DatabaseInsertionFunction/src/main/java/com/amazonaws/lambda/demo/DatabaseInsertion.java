package com.amazonaws.lambda.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;

import org.json.simple.*;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DatabaseInsertion implements RequestHandler<JSONObject, JSONObject> {

	private final String url = "";
	private final String user =  "";
	private final String password = "";
	private Connection connection = null;
	
	JSONObject dataOutput = new JSONObject();
	JSONObject headersObject = new JSONObject();
	JSONObject bodyText = new JSONObject();
	
	//Method for establishing connection to the PostgreSQL server
	public Connection connect() {
		if(connection == null) {
	        try {
	            connection = DriverManager.getConnection(url, user, password);
	            System.out.println("Connected to the PostgreSQL server successfully." + "\n");
	        } 
	        catch (SQLException e) {
	            System.out.println(e.getMessage());
	        }
		}		
		return connection;
    }
	//Main handler method (the main function code) for getting the desired data from the database before returning a JSON object with the data within it
	@Override
    public JSONObject handleRequest(JSONObject input, Context context) {
		connect();
		
		context.getLogger().log("Input: " + input);
                
        ObjectWriter JSONToString = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
			String json = JSONToString.writeValueAsString(input.get("JSONKey"));      		    	     	   
        	Timestamp timeStamp = new Timestamp(System.currentTimeMillis());
			
    	    connection.createStatement().executeUpdate("INSERT INTO \"tableName\" (\"columnName1\", \"columnName2\") VALUES ('"+ timeStamp + "', '" + json + "')");
			ResultSet recordID = connection.createStatement().executeQuery("SELECT \"PrimaryKey\" FROM \"tableName\" order by \"PrimaryKey\" desc limit 1");
    	    System.out.println("Data inserted into the database successfully.");
    	    
    	    Integer ID = 0;
    	    
    	    if(recordID.next()) {
    	    	ID = recordID.getInt(1);
    	    }
    	        	    
    	   	headersObject.put("Access-Control-Allow-Origin", "*");
			bodyText.put("bodyText", "The function was successful. Record with PrimaryKey of " + ID + " was inputted into the database.");
									
			dataOutput.put("isBase64Encoded", false);
			dataOutput.put("statusCode", 200);
            dataOutput.put("headers", headersObject);
            dataOutput.put("body", bodyText.toJSONString());
		}
        catch (SQLException | JsonProcessingException e) {
			e.printStackTrace();
		}
        System.out.print(dataOutput.toJSONString());
        return dataOutput;
    }
}