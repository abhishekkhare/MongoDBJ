package com.abhi.edu.mongodb.week2.update;

import static com.mongodb.client.model.Filters.*;

import static com.mongodb.client.model.Updates.*;

import java.util.ArrayList;
import java.util.List;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.abhi.edu.mongodb.util.JSONUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


/**
 * 
 * @author abhishekkhare This class uses the MovieDetails collection,
 *         boxOffice collection and fruitVegetable, the json for the same can be found in
 *         resources.
 *         
 *         mongoimport --db m101 --collection movieDetails --file contacts.json
 */
public class UpdateArrayTest {


	
	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("m101");
		MongoDatabase db1 = client.getDatabase("test");
		MongoCollection<Document> collection = db.getCollection("movieDetails");
		//MongoCollection<Document> boxOffice = db.getCollection("boxOffice");
		MongoCollection<Document> fruitVeg = db1.getCollection("fruitVeg");
		
		/**
		 * db.movieDetails.updateOne({_id:ObjectId("569190cd24de1e0ce2dfcd61"), actors:"Jonathan Frakes"},{$set:{"actors.$":"Abhishek Khare"}})
		 */
		{
			Bson filter = and(eq("_id",new ObjectId("569190cd24de1e0ce2dfcd62")),eq("writers","Harve Bennett"));
			Bson update = set("writers.$", "Abhishek Khare");
			collection.updateOne(filter, update);
			List<Document> all = collection.find().filter(eq("_id",new ObjectId("569190cd24de1e0ce2dfcd62"))).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}	
		}
		
		/**
		 * db.movieDetails.updateOne({_id:ObjectId("569190cf24de1e0ce2dfcd76")},{$addToSet:{actors:"Abhishek khare"}})
		 */

		{
			Bson filter = eq("_id",new ObjectId("569190cf24de1e0ce2dfcd76"));
			Bson update = addToSet("actors", "Abhishek Khare");
			collection.updateOne(filter, update);
			List<Document> all = collection.find().filter(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}	
		}
		
		/**
		 * db.movieDetails.updateOne({_id:ObjectId("569190cf24de1e0ce2dfcd76")},{$pop:{actors:1}})
		 * db.movieDetails.updateOne({_id:ObjectId("569190cf24de1e0ce2dfcd76")},{$pop:{actors:-1}})
		 */
		
		
		{
			Bson filter = eq("_id",new ObjectId("569190cf24de1e0ce2dfcd76"));
			Bson update = popFirst("actors");
			//Bson update = popLast("actors");
			collection.updateOne(filter, update);
			List<Document> all = collection.find().filter(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}	
		}
		
		/**
		 * Query:
		 * db.fruitVeg.update({ },{ $pull: { fruits: { $in: [ "apples", "oranges" ] }, vegetables: "carrots" } },{ multi: true })
		 */
		{
			Bson filter = new Bson() {
				
				public <TDocument> BsonDocument toBsonDocument(Class<TDocument> documentClass, CodecRegistry codecRegistry) {
					return new BsonDocument();
				}
			};
			Bson filter1 = in("fruits", "apples","oranges");
			Bson update = pullByFilter(filter1);
			fruitVeg.updateMany(filter, update);
			
			update = pull("vegetables", "carrots");
			fruitVeg.updateMany(filter, update);
		}
		
		/**
		 * Query::
		 * db.fruitVeg.update({ },{ $pullAll: { fruits: [ "plums", "bananas" ] , vegetables: ["zucchini"] } },{ multi: true })
		 */
		
		Bson filter = new Bson() {
			
			public <TDocument> BsonDocument toBsonDocument(Class<TDocument> documentClass, CodecRegistry codecRegistry) {
				return new BsonDocument();
			}
		};
		List<String> fruitList = new ArrayList<String>();
		fruitList.add("plums");
		fruitList.add("bananas");
		Bson update = pullAll("fruits", fruitList);
		fruitVeg.updateMany(filter, update);
		List<String> vegList = new ArrayList<String>();
		vegList.add("zucchini");
		update = pullAll("vegetables", vegList);
		fruitVeg.updateMany(filter, update);
		
		client.close();
		System.out.println("Done");
	}

}
