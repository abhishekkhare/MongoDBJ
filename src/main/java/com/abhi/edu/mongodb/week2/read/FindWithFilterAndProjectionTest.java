package com.abhi.edu.mongodb.week2.read;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.lt;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.abhi.edu.mongodb.util.JSONUtil;
/**
 * 
 * MongoDB FIND and PROJECTION
 * @author abhishekkhare
 *
 */
public class FindWithFilterAndProjectionTest {

	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("course");
		MongoCollection<Document> collection = db.getCollection("findWithFilterProjectionTest");
		collection.drop();
		for (int i = 0; i < 10; i++) {
			collection.insertOne(new Document().append("x", new Random().nextInt(2))
										.append("y", new Random().nextInt(100))
										.append("i",i));
		}
		{
			List<Document> all = collection.find().into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count());	
		}
		
		/**
		 * Projection exclude.
		 * Query 
		 * db.findWithFilterProjectionTest.find({x:0},{x:0})
		 * or 
		 * db.findWithFilterProjectionTest.find({x:{$eq:0}},{x:0})
		 */
		{
			System.out.println("######## Filter & Projection 1 ########");
			Bson filter = eq("x", 0);
			Bson projection = Projections.exclude("x");
			List<Document> all = collection.find(filter).projection(projection).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		
		/**
		 * Projection exclude multiple.
		 * db.findWithFilterProjectionTest.find({$and:[{x:{$eq:0}},{y:{$gt:50}}]},{x:0,_id:0})
		 * 
		 */
		{
			System.out.println("######## Filter & Projection 2 ########");
			Bson filter = and(eq("x", 0),gt("y",50));
			Bson projection = Projections.exclude("x","_id");
			List<Document> all = collection.find(filter).projection(projection).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}

		/**
		 * Projection Include.
		 * db.findWithFilterProjectionTest.find({$and:[{x:{$eq:0}},{y:{$gt:50}},{y:{$lt:70}}]},{y:1,_i:1})
		 * Note that _id field needs to be excluded explicitly, else it is always returned.
		 * 
		 */
		{
			System.out.println("######## Filter & Projection 3 ########");
			Bson filter = and(eq("x", 0),gt("y",50),lt("y",70));
			Bson projection = Projections.include("y","i");
			List<Document> all = collection.find(filter).projection(projection).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		
		/**
		 * Projection a mix of Include and Exclude.
		 * db.findWithFilterProjectionTest.find({$and:[{x:{$eq:0}},{y:{$gt:50}},{y:{$lt:70}}]},{y:1,_i:1,_id:0})
		 */
		{
			System.out.println("######## Filter & Projection 4 ########");
			Bson filter = and(eq("x", 0),gt("y",50),lt("y",70));
			Bson projection = Projections.fields(Projections.include("y","i"),Projections.excludeId());
			List<Document> all = collection.find(filter).projection(projection).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Count::"+collection.count(filter));	
		}
		client.close();
	}

}
