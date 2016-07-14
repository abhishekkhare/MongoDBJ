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


/**
 * Eamples for AND,OR,EQUAL,GREATER THAN,GREATER THAN and EQUAL, LESS THAN, LESS THAN and EQUAL,NOT EQUAL,IN and NIN.
 * @author abhishekkhare
 *
 */
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
		
		//adding more documents to our collection
		for (int i = 0; i < 10; i++) {
			collection.insertOne(new Document().append("x", new Random().nextInt(2)).append("y", new Random().nextInt(100)).append("qty", 5*i));
		}
		{
			List<Document> all = collection.find().into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count());	
		}
		
		
		/**
		 * Equal
		 * Query:: 
		 * db.findWithFilterTest.find({"x":0})
		 * or
		 * db.findWithFilterTest.find({"x":{$eq:0}})
		 */
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
		
		/**
		 * Equal, And,Greater Than
		 * Query:: 
		 * db.findWithFilterTest.find({"x":{$eq:0},"y":{$gt:50}}) // AND &&
		 * Can also be written with $AND operator
		 * db.findWithFilterTest.find({$and:[{"x":{$eq:0}},{"y":{$gt:50}}]})
		 * 
		 */
		
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

		/**
		 * Equal, And,Greater Than. Less Than. Note this is an example of && on the same field(Y).
		 * Query:: 
		 * db.findWithFilterTest.find({"x":{$eq:0},"y":{$gt:20,$lt:70}})
		 * or
		 * db.findWithFilterTest.find({$and:[{"x":{$eq:0}},{"y":{$gt:20,$lt:70}}]})
		 * or 
		 * db.findWithFilterTest.find({$and:[{"x":{$eq:0}},{$and:[{"y":{$gt:20}},{"y":{$lt:70}}]}]})
		 * 
		 */
		{
			System.out.println("######## Filter 3 ########");
			//Bson filter = new Document("x",0).append("y", new Document("$gt",50).append("$lt", 70));
			//another way to build query string
			Bson filter = and(eq("x", 0),gt("y",20),lt("y",70));
			List<Document> all = collection.find(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		
		/**
		 * NOT Equal, OR ,Greater Than Equal
		 * Query:: 
		 * db.findWithFilterTest.find({$or:[{"x":{$ne:0}},{"y":{$gte:20}}]}). 
		 * Notice the 4or operator needs the [] and each expression should have separate {} 
		 * 
		 */
		{
			System.out.println("######## Filter 4 ########");
			Bson filter = or(ne("x", 0),gte("y",20));
			List<Document> all = collection.find(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		
		/**
		 * NOT Equal, OR ,Greater Than Equal
		 * Query:: 
		 * db.findWithFilterTest.find({$or:[{"x":{$ne:0}},{"y":{$gte:20}}]}). 
		 * Notice the 4or operator needs the [] and each expression should have separate {} 
		 * 
		 */
		{
			System.out.println("######## Filter 5 ########");
			Bson filter = or(ne("x", 0),gte("y",20));
			List<Document> all = collection.find(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		
		/**
		 * Mix and AND and OR
		 * Query:: 
		 * db.findWithFilterTest.find({$or:[{$and:[{x:{$eq:0}},{y:{$gt:20}}]},{y:{$lte:17}}]})
		 */
		{
			System.out.println("######## Filter 6 ########");
			Bson filter = or(and(eq("x", 0),gt("y",20)),(lte("y",17)));			
			List<Document> all = collection.find(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		
		
		/**
		 * NOT and NOT EQUAL
		 * Query:: 
		 * db.findWithFilterTest.find({x:{$not:{$ne:0}}})
		 */
		{
			System.out.println("######## Filter 7 ########");
			Bson filter = not(ne("x", 0));			
			List<Document> all = collection.find(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		
		
		/**
		 * NOR
		 * Query:: 
		 * db.findWithFilterTest.find({$nor:[{$and:[{x:{$eq:0}},{y:{$gt:20}}]},{y:{$lte:17}}]})
		 */
		{
			System.out.println("######## Filter 8 ########");
			Bson filter = nor(and(eq("x", 0),gt("y",20)),(lte("y",17)));			
			List<Document> all = collection.find(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		
		/**
		 * IN
		 * Query:: 
		 * db.findWithFilterTest.find({"y":{$in:[48,91,56,75,24]}})
		 */
		{
			System.out.println("######## Filter 9 ########");
			Bson filter = in("y",48,91,56,75,24);			
			List<Document> all = collection.find(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		
		/**
		 * NIN
		 * Query:: 
		 * db.findWithFilterTest.find({"y":{$nin:[48,91,56,75,24]}})
		 */
		{
			System.out.println("######## Filter 10 ########");
			Bson filter = nin("y",48,91,56,75,24);			
			List<Document> all = collection.find(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		
		/**
		 * exists
		 * Query:: 
		 * db.findWithFilterTest.find({"qty":{$exists:true}})
		 * exists only returns documents that have the field in them. A false here would neagte the same and would return all the documents that do not have the field
		 */
		{
			System.out.println("######## Filter 11 ########");
			Bson filter = exists("qty",true);			
			List<Document> all = collection.find(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		
		
		/**
		 * exists and nin
		 * Query:: 
		 * db.findWithFilterTest.find({"qty":{$exists:true,$nin:[5,15,25]}})
		 * exists only returns documents that have the field in them. A false here would negate the same and would return all the documents that do not have the field
		 */
		{
			System.out.println("######## Filter 12 ########");
			Bson filter = and(exists("qty",true),nin("qty",5,15,25));			
			List<Document> all = collection.find(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		
		
		/**
		 * mod
		 * Query:: 
		 * db.findWithFilterTest.find({ qty: { $mod: [ 3, 0 ] } } )
		 * simple mathematical mod
		 * 
		 */
		{
			System.out.println("######## Filter 13 ########");
			Bson filter = mod("qty", 3, 0);			
			List<Document> all = collection.find(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		client.close();

	}
}
