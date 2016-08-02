package com.abhi.edu.mongodb.week5.aggregation.pipeline;

import static com.mongodb.client.model.Aggregates.sort;

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
 * { $sort: { <field1>: <sort order>, <field2>: <sort order> ... } }
 * @author abhishekkhare
 *
 */


public class SortTest {

	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("m101");
		MongoCollection<Document> collection = db.getCollection("movieDetails");
		/**
		 * db.movieDetails.aggregate([{$sort:{year:1,runtime:-1}}]).pretty()
		 */
		List<Bson> pipeline = new ArrayList<Bson>();
		Bson sort = sort(new Document("year",1).append("runtime", -1));
		pipeline.add(sort);
		AggregateIterable<Document> itr = collection.aggregate(pipeline);

		for (Document document : itr) {
			JSONUtil.printJson(document);
		}	

		
		client.close();
		System.out.println("Done");

	}

}
