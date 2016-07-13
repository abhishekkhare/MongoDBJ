package com.abhi.edu.mongodb.week2.read;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;

import com.abhi.edu.mongodb.util.JSONUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;


public class FindTest {

	public static void main(String[] args) {
		
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("course");
		MongoCollection<Document> collection = db.getCollection("findTest");
		collection.drop();
		for (int i = 0; i < 10; i++) {
			collection.insertOne(new Document("x",i));
		}
		System.out.println("find");
		
		findOne(collection);
		System.out.println("findAll");
		findAll(collection);
		/**
		 * Used for large dataset
		 */
		System.out.println("finaAllWithCursor");
		finaAllWithCursor(collection);
		System.out.println("count");
		count(collection);
		client.close();
		System.out.println("done");
	}

	/**
	 * Find all the documents in the collection.
	 * Query::
	 * db.findTest.find()
	 * @param collection
	 */
	private static void finaAllWithCursor(MongoCollection<Document> collection) {
		MongoCursor<Document> cursor = collection.find().iterator();
		try{
			while(cursor.hasNext()){
				Document document = cursor.next();
				JSONUtil.printJson(document);
			}
		}finally{
			cursor.close();
		}
		
	}

	/**
	 * Count the number of documents contained in a collection
	 * db.findTest.count()
	 * @param collection
	 */
	private static void count(MongoCollection<Document> collection) {
		System.out.println("Count::" + collection.count());	
	}

	/**
	 * Find all the documents in the collection.
	 * Query::
	 * db.findTest.find()
	 * @param collection
	 */
	private static void findAll(MongoCollection<Document> collection) {
		List<Document> all = collection.find().into(new ArrayList<Document>());
		for (Iterator <Document>iterator = all.iterator(); iterator.hasNext();) {
			Document document = iterator.next();
			JSONUtil.printJson(document);
		
		}
		
	}

	/**
	 * Find the first document returned by the collection.
	 * Query::
	 * db.findTest.findOne()
	 * @param collection
	 * 
	 */
	private static void findOne(MongoCollection<Document> collection) {
		Document document = collection.find().first();
		JSONUtil.printJson(document);
		
	}

}
