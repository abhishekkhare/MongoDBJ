package com.abhi.edu.mongodb.week5.aggregation.pipeline;

import static com.mongodb.client.model.Aggregates.sample;

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
 * 
 * @author abhishekkhare
 *
 * { $sample: { size: <positive integer> } }
 */
public class SampleTest {

	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("m101");
		MongoCollection<Document> collection = db.getCollection("movieDetails");
		/**
		 * db.movieDetails.aggregate([{$sample:{size:3}}]).pretty()
		 */
		List<Bson> pipeline = new ArrayList<Bson>();
		Bson sample = sample(3);
		pipeline.add(sample);
		AggregateIterable<Document> itr = collection.aggregate(pipeline);

		for (Document document : itr) {
			JSONUtil.printJson(document);
		}	

		
		client.close();
		System.out.println("Done");

	}

}
