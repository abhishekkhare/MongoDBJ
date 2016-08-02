package com.abhi.edu.mongodb.week5.aggregation.pipeline;


import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.group;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.abhi.edu.mongodb.util.JSONUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;



/**
 * { $group: { _id: <expression>, <field1>: { <accumulator1> : <expression1> },... } }
 * 
 * @author abhishekkhare
 *
 */
public class GroupTest {

	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("m101");
		MongoCollection<Document> collection = db.getCollection("movieDetails");
		
		System.out.println("######## GROUP SUM #################");
		/**
		 * db.movieDetails.aggregate([{ $group : { _id : "$year", totalReviews:{$sum:"$tomato.userReviews"}  } }])
		 * to validate use this query  db.movieDetails.find({year:{$eq:1995}},{_id:0,"tomato.userReviews":1})
		 */
		{
			List<Bson> pipeline = new ArrayList<Bson>();
			Bson group = group("$year", sum("totalReviews", "$tomato.userReviews"));
			pipeline.add(group);
			AggregateIterable<Document> itr = collection.aggregate(pipeline);

			for (Document document : itr) {
				JSONUtil.printJson(document);
			}	
		}
		System.out.println("######## GROUP AVG #################");
		/**
		 * db.movieDetails.aggregate([{ $group : { _id : "$year", avergaeReview:{$avg:"$tomato.userReviews"}  } }])
		 * to validate use this query  db.movieDetails.find({year:{$eq:1995}},{_id:0,"tomato.userReviews":1})
		 */
		
		{
			List<Bson> pipeline = new ArrayList<Bson>();
			Bson group = group("$year", avg("avergaeReview", "$tomato.userReviews"));
			pipeline.add(group);
			AggregateIterable<Document> itr = collection.aggregate(pipeline);

			for (Document document : itr) {
				JSONUtil.printJson(document);
			}	
		}
		System.out.println("######## GROUP FIRST #################");
		/**
		 * db.movieDetails.aggregate([{ $group : { _id : "$year", First:{$first:"$title"}  } }])
		 * to validate use this query  db.movieDetails.find({year:{$eq:1995}},{_id:0,"title":1})
		 */
		
		{
			List<Bson> pipeline = new ArrayList<Bson>();
			Bson group = group("$year", first("First", "$title"));
			pipeline.add(group);
			AggregateIterable<Document> itr = collection.aggregate(pipeline);

			for (Document document : itr) {
				JSONUtil.printJson(document);
			}	
		}
		
		System.out.println("######## GROUP LAST #################");
		/**
		 * db.movieDetails.aggregate([{ $group : { _id : "$year", Last:{$last:"$title"}  } }])
		 * to validate use this query  db.movieDetails.find({year:{$eq:1995}},{_id:0,"title":1})
		 */
		
		{
			List<Bson> pipeline = new ArrayList<Bson>();
			Bson group = group("$year", last("Last", "$title"));
			pipeline.add(group);
			AggregateIterable<Document> itr = collection.aggregate(pipeline);

			for (Document document : itr) {
				JSONUtil.printJson(document);
			}	
		}
		
		System.out.println("######## GROUP MAX #################");
		/**
		 * db.movieDetails.aggregate([{ $group : { _id : "$year", "Max User Reviews":{$max:"$tomato.userReviews"}}}])
		 * to validate use this query  db.movieDetails.find({year:{$eq:1995}},{_id:0,"tomato.userReviews":1})
		 */
		
		{
			List<Bson> pipeline = new ArrayList<Bson>();
			Bson group = group("$year", max("Max User Reviews", "$tomato.userReviews"));
			pipeline.add(group);
			AggregateIterable<Document> itr = collection.aggregate(pipeline);

			for (Document document : itr) {
				JSONUtil.printJson(document);
			}	
		}
		
		System.out.println("######## GROUP MIN #################");
		/**
		 * db.movieDetails.aggregate([{ $group : { _id : "$year", "Min User Reviews":{$min:"$tomato.userReviews"}}}])
		 * to validate use this query  db.movieDetails.find({year:{$eq:1995}},{_id:0,"tomato.userReviews":1})
		 */
		
		{
			List<Bson> pipeline = new ArrayList<Bson>();
			Bson group = group("$year", min("Min User Reviews", "$tomato.userReviews"));
			pipeline.add(group);
			AggregateIterable<Document> itr = collection.aggregate(pipeline);

			for (Document document : itr) {
				JSONUtil.printJson(document);
			}	
		}
		
		System.out.println("######## GROUP PUSH #################");
		/**
		 * db.movieDetails.aggregate([{ $group : { _id : "$year", titles: {$push: "$title" } } }])
		 * to validate use the below query - 
		 * db.movieDetails.find({year:{$eq:1909}}).count()
		 */
		{
			List<Bson> pipeline = new ArrayList<Bson>();
			Bson group = group("$year", push("titles","$title"));
			pipeline.add(group);
			AggregateIterable<Document> itr = collection.aggregate(pipeline);

			for (Document document : itr) {
				JSONUtil.printJson(document);
			}	
		}
		
		
		System.out.println("######## GROUP addToSet #################");
		/**
		 * db.movieDetails.aggregate([{ $group : { _id : "$year", rated: {$addToSet: "$rated" } } }])
		 * to validate use the below query - 
		 * db.movieDetails.find({year:{$eq:1909}}).pretty()
		 */
		{
			List<Bson> pipeline = new ArrayList<Bson>();
			Bson group = group("$year", addToSet("rated", "$rated"));
			pipeline.add(group);
			AggregateIterable<Document> itr = collection.aggregate(pipeline);

			for (Document document : itr) {
				JSONUtil.printJson(document);
			}	
		}
		
		client.close();
		System.out.println("Done");

	}

}
