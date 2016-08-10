package com.abhi.edu.mongodb.week5.aggregation.booleann;

import static com.mongodb.client.model.Aggregates.project;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.abhi.edu.mongodb.util.JSONUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;

public class AndOrNotTest {

	public static void main(String[] args) {
		
		
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("m101");
		MongoCollection<Document> collection = db.getCollection("movieDetails");

		/**
		 * db.movieDetails.aggregate([{$project:{_id:0,year: 1,runtime: 1,runtimeRange: { $and: [ { $gt: [ "$runtime", 100 ] }, { $lt: [ "$runtime", 115 ] } ] }}}])
		 */
		
		/**
		 * TODO - needs to find the correct way to use boolean operator with aggregation. the below is not working.
		 */
		List<Bson> pipeline = new ArrayList<Bson>();
		//Bson project = project(Projections.fields(Projections.include("year","runtime"),Projections.excludeId(),Projections.computed("runtimeRange",and(gt("runtime", 100),lt("runtime", 115)))));
		//Bson project = project(Projections.fields(Projections.include("year","runtime"),Projections.excludeId(),Projections.computed("runtimeRange","$and: [ { $gt: [ $runtime, 100 ] }, { $lt: [ $runtime, 115 ]}]")));
		Bson project = project(Projections.fields(Projections.include("year","runtime","runtimeRange"),Projections.excludeId(),Projections.computed("runtimeRange","$project:{runtimeRange: { $and: [ { $gt: [\"$runtime\", 100 ] }, { $lt: [ \"$runtime\", 115 ] } ] }}")));
		//Bson project = project(Projections.fields(Projections.include("year","runtime"),Projections.excludeId(),Projections.computed("runtimeRange", "$runtime")));
		//Bson project = project(Projections.fields(Projections.include("year","runtime"),Projections.excludeId(),Projections.computed("runtimeRange", "$runtime")));
		pipeline.add(project);
		AggregateIterable<Document> itr = collection.aggregate(pipeline);

		for (Document document : itr) {
			JSONUtil.printJson(document);
		}	

		/**
		 * db.movieDetails.aggregate([{$project:{_id:0,year: 1,runtime: 1,runtimeRange: { $or: [ { $gt: [ "$runtime", 100 ] }, { $lt: [ "$runtime", 80 ] } ] }}}])
		 */
		
		
		
		
		/**
		 * db.movieDetails.aggregate([{$project:{_id:0,year: 1,runtime: 1,runtimeRange: { $not: [ { $gt: [ "$runtime", 100 ] }] }}}])
		 */
		
		client.close();
		System.out.println("Done");
	}

}
