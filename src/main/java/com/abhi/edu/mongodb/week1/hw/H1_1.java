package com.abhi.edu.mongodb.week1.hw;

import org.bson.Document;

import com.abhi.edu.mongodb.util.JSONUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
 * 
 * @author abhishekkhare
 * query:::db.hw1.findOne() 
 * response:::42
 *
 */
public class H1_1 {

	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("m101");
		MongoCollection<Document> collection = db.getCollection("hw1");
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
