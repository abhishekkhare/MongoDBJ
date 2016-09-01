package com.abhi.edu.mongodb.week2.update;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Updates.pullByFilter;

import java.util.ArrayList;
import java.util.List;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import com.abhi.edu.mongodb.util.JSONUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;


/**
 * 
 * @author abhishekkhare This class uses the MovieDetails collection,
 *         boxOffice collection and fruitVegetable, the json for the same can be found in
 *         resources.
 *         
 *         mongoimport --db m101 --collection movieDetails --file contacts.json
 */
public class UpdateOnMultipleCriteria {


	
	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("video");
		MongoCollection<Document> collection = db.getCollection("movieDetails");
		
		/**
		 * Query:
		 * db.movieDetails.find({imdb:{"lt":10000}, "year":{"gte":2010,"lte":2013}, $and:[{"tomato.consensus":{$exists:true}}, {"tomato.consensus":null}] }, 
		 * 					{ $unset: { "tomato.consensus": "" } })
		 */
		{
			Bson filter = new Bson() {
				
				public <TDocument> BsonDocument toBsonDocument(Class<TDocument> documentClass, CodecRegistry codecRegistry) {
					return new BsonDocument();
				}
			};
			Bson filter1 = and(lt("imdb.votes", 10000),gte("year", 2010), lte("year", 2013), exists("tomato.consensus"),eq("tomato.consensus", null));
			
			//Find
			List<Document> all = collection.find().filter(filter1).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			System.out.println("Records to update::"+collection.count(filter1));	
			
			// Update
			Bson update = pullByFilter(filter1);
			UpdateResult result = collection.updateMany(filter, update);
			System.out.println("Records Modified"+ result.getModifiedCount());
		}
		
		client.close();
		System.out.println("Done");
	}

}
