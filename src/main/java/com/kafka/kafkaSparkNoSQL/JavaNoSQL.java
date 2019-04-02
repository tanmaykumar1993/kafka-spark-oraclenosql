package com.kafka.kafkaSparkNoSQL;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.FaultException;
import oracle.kv.StatementResult;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import scala.util.parsing.json.JSONObject;

public class JavaNoSQL {
	
	static StatementResult result = null;
	static String statement = null;
	static KVStore store;
	static TableAPI tableAPI;

	public static void main(String[] args) {
		
		String[] hhosts = {"localhost:5000"};
		KVStoreConfig kconfig = new KVStoreConfig("mystore", hhosts);
		store = KVStoreFactory.getStore(kconfig);
		
		tableAPI = store.getTableAPI();
		
		//createTable();
		insertValues("{\"item\":\"i2\",\"description\":\"d2\",\"count\":4,\"percentage\":10.20}");
		
//		 String storeName = "mystore";
//		 String hostName = "localhost";
//		 String hostPort = "5000";
//		
//		 KVStoreConfig config = new KVStoreConfig(storeName, 
//		 hostName + ":" + hostPort);
		 //KVStore kvStore = KVStoreFactory.getStore(config);
		 
		// Login modeled as /Login/$userName: $password
//		 final String userName = "Jasper";
//		 final String password = "welcome";
//
//
//		 // Create a login for Jasper
//		 List<String> majorPath = Arrays.asList("Login", userName);
//		 Key key = Key.createKey(majorPath);
//
//
//		 byte[] bytes = password.getBytes();
//		 Value value = Value.createValue(bytes);
//
//		 kvStore.putIfAbsent(key, value);
//
//
//		 // Read 
//		 ValueVersion vv2 = kvStore.get(key);
//		 Value value2 = vv2.getValue();
//		 byte[] bytes2 = value2.getValue();
//		 String password2 = new String(bytes);


//		 // Update
//		 Value value3 = Value.createValue("welcome3".getBytes());
//		 kvStore.put(key, value3);
//		 
//		 // Delete
//		 kvStore.delete(key); 

	}

	public static void createTable() {
	    StatementResult result = null;
	    String statement = null;

	    try {
	        statement =
	            "CREATE TABLE myTable2 (" +
	            "item STRING," +
	            "description STRING," +
	            "count INTEGER," +
	            "percentage DOUBLE," +
	            "PRIMARY KEY (item))"; // Required"
	        result = store.executeSync(statement);

	        displayResult(result, statement);

	    } catch (IllegalArgumentException e) {
	        System.out.println("Invalid statement:\n" + e.getMessage());
	    } catch (FaultException e) {
	        System.out.println
	            ("Statement couldn't be executed, please retry: " + e);
	    }
	}

	private static void displayResult(StatementResult result, String statement) {
	    System.out.println("===========================");
	    if (result.isSuccessful()) {
	        System.out.println("Statement was successful:\n\t" + 
	            statement);
	        System.out.println("Results:\n\t" + result.getInfo());
	    } else if (result.isCancelled()) {
	        System.out.println("Statement was cancelled:\n\t" + 
	            statement);
	    } else {
	        /*
	         * statement was not successful: may be in error, or may still
	         * be in progress.
	         */
	        if (result.isDone()) {
	            System.out.println("Statement failed:\n\t" + statement);
	            System.out.println("Problem:\n\t" + 
	                result.getErrorMessage());
	        } else {
	            System.out.println("Statement in progress:\n\t" + 
	                statement);
	            System.out.println("Status:\n\t" + result.getInfo());
	        }
	    }
	} 
	
	static Table getTable(String tableName) {
        Table table = tableAPI.getTable(tableName);
        return table;
    }
	
	static void insertValues(String str) {
		ObjectMapper mapper = new ObjectMapper();
		JsonNode node;
		try {
			node = mapper.readTree(str);
//			String item = node.get("item").asText();
//			String description = node.get("description").asText();
//			String count = node.get("count").asText();
//			String percentage = node.get("percentage").asText();
//			System.out.println(item +" : "+ description +" : "+ count +" : "+ percentage);
//			node.fieldNames().forEachRemaining(e -> System.out.println(e));
			Table table = getTable("myTable2");
	        Row nosqlRow = table.createRow();
	        
	        Item itm = mapper.readValue(str, Item.class);
	        nosqlRow.put("item", itm.getItem());
	        nosqlRow.put("description", itm.getDescription());
	        nosqlRow.put("count", itm.getCount());
	        nosqlRow.put("percentage", itm.getPercentage());
	        
			node.fieldNames().forEachRemaining(item -> {
				
				//nosqlRow.put(item, node.get(item).isInt()? node.get(item).asText() : node.get(item).asText());
			});
			tableAPI.put(nosqlRow, null, null);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}
