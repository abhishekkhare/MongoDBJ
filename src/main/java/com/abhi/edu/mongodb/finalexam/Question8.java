package com.abhi.edu.mongodb.finalexam;

import java.io.IOException;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * 
 * @author abhishekkhare
 *
 */
public class Question8 {

	/**
	 * 
	 * @param args
	 * @throws IOException
	 */

	public static void main(String[] args) throws IOException {
		MongoClient c = new MongoClient();
		MongoDatabase db = c.getDatabase("test");
		MongoCollection<Document> animals = db.getCollection("animals");

		Document animal = new Document("animal", "monkey");

		animals.insertOne(animal);
		animal.remove("animal");
		animal.append("animal", "cat");
		animals.insertOne(animal);
		animal.remove("animal");
		animal.append("animal", "lion");
		animals.insertOne(animal);
		c.close();
	}
}
