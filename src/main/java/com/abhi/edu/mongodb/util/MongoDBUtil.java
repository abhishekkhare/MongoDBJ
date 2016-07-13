package com.abhi.edu.mongodb.util;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoDBUtil {

	public static void printAllDataFromTable(String tableName,String collectionName){
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase(tableName);
		MongoCollection<Document> collection = db.getCollection(collectionName);
		MongoCursor<Document> cursor = collection.find().iterator();
		try{
			while(cursor.hasNext()){
				Document document = cursor.next();
				JSONUtil.printJson(document);
			}
		}finally{
			cursor.close();
			client.close();
		}
	}
}
