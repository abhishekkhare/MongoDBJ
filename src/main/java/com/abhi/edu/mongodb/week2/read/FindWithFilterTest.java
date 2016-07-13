package com.abhi.edu.mongodb.week2.read;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;
import com.abhi.edu.mongodb.util.JSONUtil;

public class FindWithFilterTest {

	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("course");
		MongoCollection<Document> collection = db.getCollection("findWithFilterTest");
		collection.drop();
		for (int i = 0; i < 10; i++) {
			collection.insertOne(new Document().append("x", new Random().nextInt(2))
										.append("y", new Random().nextInt(100)));
		}
		{
			List<Document> all = collection.find().into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count());	
		}
		
		{
			System.out.println("######## Filter 1 ########");
			//Bson filter = new Document("x",0);
			//another way to build query string
			Bson filter = eq("x", 0);
			List<Document> all = collection.find(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		
		
		{
			System.out.println("######## Filter 2 ########");
			//Bson filter = new Document("x",0).append("y", new Document("$gt",50));
			//another way to build query string
			Bson filter = and(eq("x", 0),gt("y",50));
			List<Document> all = collection.find(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}

		{
			System.out.println("######## Filter 3 ########");
			//Bson filter = new Document("x",0).append("y", new Document("$gt",50).append("$lt", 70));
			//another way to build query string
			Bson filter = and(eq("x", 0),gt("y",50),lt("y",70));
			List<Document> all = collection.find(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		
		client.close();
	}

}
