package com.abhi.edu.mongodb.week2.update;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.max;
import static com.mongodb.client.model.Updates.min;
import static com.mongodb.client.model.Updates.mul;
import static com.mongodb.client.model.Updates.rename;
import static com.mongodb.client.model.Updates.set;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
/**
 * MongoDB UPDATE
 * @author abhishekkhare
 *
 */
public class UpdateTest {

	public static void main(String[] args) {
		
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("course");
		MongoCollection<Document> collection = db.getCollection("updateTest");
		collection.drop();
		for (int i = 1; i <=20; i++) {
			if(i%2==0)
				collection.insertOne(new Document("x",i).append("y", i*2));
			else if(i%3==0)
				collection.insertOne(new Document("x",i).append("yy", i*3));
			else
				collection.insertOne(new Document("x",i));
		}
		System.out.println("Data Inserted");
		
		updateOne(collection);
		updateMany(collection);
		client.close();
		System.out.println("done");
	}

	private static void updateMany(MongoCollection<Document> collection) {
		/**
		 * Query::
		 * db.updateTest.update({"x":{$lt:7}},{$set:{"x":55}})
		 * this will update only the 1st document that matches the filter criteria
		 * db.updateTest.update({"x":{$lt:7}},{$set:{"x":55}},{multi:true})
		 * this will modify all documents that match the filter criteria.
		 * or you can use this
		 * db.updateTest.updateMany({"x":{$lt:15}},{$set:{"x":555}})
		 * 
		 */
		{
			Bson filter = lt("x",7);
			Bson update = set("x",33);
			UpdateResult result = collection.updateMany(filter, update);
			System.out.println("Set:" + result.wasAcknowledged());	
		}
	}

	/**
	 * Update only the first document the meet the filter criteria.
	 * @param collection
	 * 
	 */
	private static void updateOne(MongoCollection<Document> collection) {
		/**
		 * Query::
		 * db.updateTest.updateOne({"x":{$lt:4}},{$set:{"x":55}})
		 * You can see, we have more elements that are greater than 4, but only one is mondified.
		 */
		{
			Bson filter = lt("x",3);
			Bson update = set("x",33);
			UpdateResult result = collection.updateOne(filter, update);
			System.out.println("Set:" + result.wasAcknowledged());	
		}
		
		/**
		 * Query::
		 * db.updateTest.updateOne({"x":{$eq:7}},{$inc:{"x":100}})
		 */
		
		{
			Bson filter = eq("x",7);
			Bson update = inc("x", 100);
			UpdateResult result = collection.updateOne(filter, update);
			System.out.println("Inc:" + result.wasAcknowledged());
		}
		
		/**
		 * Query::
		 * db.updateTest.updateOne({"x":{$eq:8}},{$mul:{"x":100}})
		 */
		{
			Bson filter = eq("x",8);
			Bson update = mul("x", 100);
			UpdateResult result = collection.updateOne(filter, update);
			System.out.println("Mul:" + result.wasAcknowledged());
		}
		
		/**
		 * Query::
		 * db.updateTest.updateOne({x:3},{$rename:{"yy":"yyyYYYY"}})
		 * however if we run the below query nothing happens, since x=4 does not have a matching "YY".
		 * db.updateTest.updateOne({x:4},{$rename:{"yy":"yyyYYYY"}})
		 */
		{
			Bson filter = eq("x",3);
			Bson update = rename("yy", "yyYYYY");
			UpdateResult result = collection.updateOne(filter, update);
			System.out.println("Rename:" + result.wasAcknowledged());
		}
		
		
		/**
		 * Query::
		 * db.updateTest.updateOne({"x":{$eq:20}},{$min:{"y":10}})
		 * now if the below query is run, it causes no effect since the actual value of the document is already less than min value.
		 * db.updateTest.updateOne({"x":{$eq:20}},{$min:{"y":20}})
		 * 
		 */
		{
			Bson filter = eq("x",20);
			Bson update = min("y", 10);
			UpdateResult result = collection.updateOne(filter, update);
			System.out.println("Min:" + result.wasAcknowledged());
		}
		
		
		/**
		 * Query::
		 * db.updateTest.updateOne({"x":{$eq:18}},{$max:{"y":100}})
		 * now if the below query is run, it causes no effect since the actual value of the document is already more than max value.
		 * db.updateTest.updateOne({"x":{$eq:18}},{$max:{"y":80}})
		 * 
		 */
		{
			Bson filter = eq("x",18);
			Bson update = max("y", 100);
			UpdateResult result = collection.updateOne(filter, update);
			System.out.println("Max:" + result.wasAcknowledged());
		}

	}

}
