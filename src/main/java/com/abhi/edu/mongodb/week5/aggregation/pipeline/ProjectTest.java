package com.abhi.edu.mongodb.week5.aggregation.pipeline;

import static com.mongodb.client.model.Aggregates.*;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.model.Projections;
import com.abhi.edu.mongodb.util.JSONUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class ProjectTest {

	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("m101");
		MongoCollection<Document> collection = db.getCollection("movieDetails");
		/**
		 * db.movieDetails.aggregate([{$project:{title:1,genres:1}}])
		 */
		{
			List <Bson> pipeline = new ArrayList<Bson>();
			Bson project = project(Projections.include("title","genres"));
			pipeline.add(project);
			AggregateIterable<Document> itr = collection.aggregate(pipeline);
			for (Document document : itr) {
				JSONUtil.printJson(document);
			}
		}
		System.out.println("######## Suppress _id field ####################" );
		
		/**
		 * Suppress the _id field.
		 * 
		 * db.movieDetails.aggregate([{$project:{title:1,genres:1,_id:0}}])
		 */
		{
		
			List <Bson> pipeline = new ArrayList<Bson>();
			Bson project = project(Projections.fields(Projections.include("title","genres"),Projections.excludeId()));
			pipeline.add(project);
			
			AggregateIterable<Document> itr = collection.aggregate(pipeline);
			for (Document document : itr) {
				JSONUtil.printJson(document);
			}
		}
		System.out.println("######## Include Specific Fields from Embedded Documents ####################" );
		/**
		 * Include Specific Fields from Embedded Documents
		 * db.movieDetails.aggregate([{$project:{title:1,_id:0,"tomato.userMeter":1}}])
		 * or we can also nest the inclusion
		 * db.movieDetails.aggregate([{$project:{title:1,_id:0,tomato:{userMeter:1}}}])
		 * Notice that some of the documents may not have the included fields, such documents are not ignored, since projection does not do any kind of filtering.
		 */
		
		{
			
			List <Bson> pipeline = new ArrayList<Bson>();
			Bson project = project(Projections.fields(Projections.include("title","tomato.userMeter"),Projections.excludeId()));
			pipeline.add(project);
			
			AggregateIterable<Document> itr = collection.aggregate(pipeline);
			for (Document document : itr) {
				JSONUtil.printJson(document);
			}
		}
		
		
		
		client.close();
		System.out.println("Done");

	}

}
