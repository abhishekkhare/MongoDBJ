package com.abhi.edu.mongodb.week2.read;

import static com.mongodb.client.model.Filters.exists;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.abhi.edu.mongodb.util.JSONUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;

import static com.mongodb.client.model.Filters.*;

/**
 * 
 * @author abhishekkhare
 * This class uses the MovieDetails collection and boxOffice collection, the json for the same can be found in resources.
 */
public class FindWithArray {
	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("m101");
		MongoCollection<Document> collection = db.getCollection("movieDetails");
		MongoCollection<Document> boxOffice = db.getCollection("boxOffice");
		/**
		 * db.movieDetails.find().pretty()
		 */
		{
			System.out.println("Print the entire collection");
			List<Document> all = collection.find().into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("count::" + all.size());
		}
		

		/**
		 * Array exists
		 * db.movieDetails.find({"tomato.meter":{$exists:true}}).pretty()
		 */
		
		{
			System.out.println("Print documents that have tomato meter rating");
			Bson filter = exists("tomato.meter", true);
			List<Document> all = collection.find().filter(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("count::" + all.size());
		}
		
		/**
		 * Array, Filter, Projection and exists
		 * db.movieDetails.find({$and:[{"tomato.meter":{$exists:true}},{"tomato.meter":{$gt:90}}]},{title:1,_id:0,"tomato.meter":1}).pretty()
		 */
		
		{
			System.out.println("Print documents that have tomato meter rating of more than 90");
			Bson filter = and(exists("tomato.meter", true),gt("tomato.meter",90));
			Bson projection = Projections.fields(Projections.include("title","tomato.meter"),Projections.excludeId());
			List<Document> all = collection.find().filter(filter).projection(projection).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("count::" + all.size());
		}
		
		/**
		 * $all
		 * Find documents that have an array with exact match
		 * db.movieDetails.find({genres:{$all:["Comedy","Crime","Drama"]}},{title:1,genres:1,_id:0}).pretty()
		 * 
		 * Note here the order in the query and in the array are not ignored, it just validates that the elements all the elements in the filter and array macth
		 */
		
		{
			System.out.println("Print documents that have genres exactly as Comedy,Crime,Drama.");
			Bson filter = all("genres", "Comedy","Crime","Drama");
			Bson projection = Projections.fields(Projections.include("title","genres"),Projections.excludeId());
			List<Document> all = collection.find().filter(filter).projection(projection).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("count::" + all.size());
		}
		
		/**
		 * $size
		 * Find documents that have an array with a given size
		 * db.movieDetails.find({countries:{$size:3}},{title:1,countries:1,_id:0}).pretty()
		 * 
		 */
		
		{
			System.out.println("Print documents that have countries with 3 elements");
			Bson filter = size("countries", 3);
			Bson projection = Projections.fields(Projections.include("title","countries"),Projections.excludeId());
			List<Document> all = collection.find().filter(filter).projection(projection).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("count::" + all.size());
		}
		
		//db.boxOffice.insert({boxOffice:[{ "country": "USA", "revenue": 41.3 },{ "country": "Australia", "revenue": 2.9 },{ "country": "UK", "revenue": 10.1 },{ "country": "Germany", "revenue": 4.3 },{ "country": "France", "revenue": 3.5 } ]})
		//db.boxOffice.insert({boxOffice:[{ "country": "USA", "revenue": 51.3 },{ "country": "Australia", "revenue": 3.9 },{ "country": "UK", "revenue": 11.1 },{ "country": "Germany", "revenue": 5.3 },{ "country": "France", "revenue": 4.5 } ]})
		//db.boxOffice.insert({boxOffice:[{ "country": "USA", "revenue": 31.3 },{ "country": "Australia", "revenue": 1.9 },{ "country": "UK", "revenue": 9.1 },{ "country": "Germany", "revenue": 2.3 },{ "country": "France", "revenue": 2.5 } ]})
		/**
		 * $elematch - Selects documents if element in the array field matches all the specified $elemMatch conditions.
		 * 
		 * 1. db.boxOffice.find({"boxOffice.country":"UK","boxOffice.revenue":{$gt:10}}).pretty()
		 * 
		 * 2. db.boxOffice.find({boxOffice:{$elemMatch:{"country":"UK","revenue":{$gt:10}}}}).pretty()
		 * 
		 * 
		 */
		
		{  
			System.out.println("Print all documents");
			List<Document> all = boxOffice.find().into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("count::" + all.size());
		}
		
		{  
			System.out.println("Print 1 Query");
			Bson filter = and(eq("boxOffice.country","UK"),gt("boxOffice.revenue",10));
			List<Document> all = boxOffice.find(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("count::" + all.size());
		}
		
		{  
			System.out.println("Print 2 Query");
			Bson filter = and(eq("country","UK"),gt("revenue",10));
			Bson elemMatch = elemMatch("boxOffice", filter);
			List<Document> all = boxOffice.find(elemMatch).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("count::" + all.size());
		}
		client.close();
	}
}
