package com.abhi.edu.mongodb.week2.hw;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.abhi.edu.mongodb.util.JSONUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Filters.*;

import static com.mongodb.client.model.Updates.*;

import java.util.ArrayList;
import java.util.List;

public class AdditionalQuestion {
	public static void main(String[] args) {
		System.out.println("Start");
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("m101");
		MongoCollection<Document> collection = db.getCollection("movieDetails");
		/**
		 * Question: Write an update command that will remove the
		 * "tomato.consensus" field for all documents matching the following
		 * criteria:
		 * 
		 * The number of imdb votes is less than 10,000 The year for the movie
		 * is between 2010 and 2013 inclusive The tomato.consensus field is null
		 * 
		 * How many documents required an update to eliminate a
		 * "tomato.consensus" field?
		 * 
		 * Query find : db.movieDetails.find({$and:[{"imdb.votes":{$lt:10000}},{"year":{$gte:2010}},{"year":{$lte:2013}},{"tomato.consensus":{$exists:true}},{"tomato.consensus":null}]},{"imdb.votes":1,year:1,"tomato.consensus":1})
		 * 
		 * Query Update : db.movieDetails.update({$and:[{"imdb.votes":{$lt:10000}},{"year":{$gte:2010}},{"year":{$lte:2013}},{"tomato.consensus":{$exists:true}},{"tomato.consensus":null}]},{$unset:{"tomato.consensus": ""}},{ multi: true })
		 */
		Bson filter = and(lt("imdb.votes", 10000),gte("year", 2010),lte("year", 2013),exists("tomato.consensus"),eq("tomato.consensus", null));
		//Bson filter = and(lt("imdb.votes", 10000),eq("year", 2011),exists("tomato.consensus"),eq("tomato.consensus", null));
		List<Document> all =  collection.find().filter(filter).into(new ArrayList<Document>());
		System.out.println("Count:" + all.size());
		for (Document document : all) {
			JSONUtil.printJson(document);
		}
		
		Bson update = unset("tomato.consensus");
		collection.updateMany(filter, update);
		
		client.close();
		System.out.println("Done");

	}
}
