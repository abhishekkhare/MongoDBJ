package com.abhi.edu.mongodb.week5.aggregation.pipeline;

import static com.mongodb.client.model.Aggregates.*;

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

/**
 * To specify a field path, prefix the field name with a dollar sign $ and enclose in quotes.
 * @author abhishekkhare
 *
 */
public class UnwindTest {

	public static void main(String[] args) {

		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("m101");
		MongoCollection<Document> collection = db.getCollection("movieDetails");
		/**
		 * db.movieDetails.aggregate([{$project:{title:1,genres:1,actors:1,_id:0}},{$limit:5},{$skip:2},{$unwind:"$actors"}])
		 */
		{
			List <Bson> pipeline = new ArrayList<Bson>();
			
			Bson project = project(Projections.fields(Projections.include("title","genres","actors"),Projections.excludeId()));
			Bson limit = limit(5);
			Bson skip = skip(2);
			Bson unwind = unwind("$actors");
			pipeline.add(project);

			pipeline.add(limit);
			pipeline.add(skip);
			pipeline.add(unwind);
			
			AggregateIterable<Document> itr = collection.aggregate(pipeline);
			for (Document document : itr) {
				JSONUtil.printJson(document);
			}
		}
		client.close();
		System.out.println("Done");

	

	}

}
