package com.abhi.edu.mongodb.week2.update;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import static com.mongodb.client.model.Filters.*;

import static com.mongodb.client.model.Updates.*;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.PushOptions;


public class UpdateArrayPushTest {

	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		MongoDatabase db1 = client.getDatabase("test");
		MongoCollection<Document> fruitVeg = db1.getCollection("fruitVeg");
		/**
		 * db.fruitVeg.update({ _id: 1 },{ $push: { fruits: "Mango" } })
		 */
		{
			Bson filter = eq("_id", 1);
			Bson update = push("vegetables", "cabbage");
			fruitVeg.updateOne(filter, update);	
		}
		
		
		/**
		 * db.fruitVeg.update({ _id: 3 },{ $push: { fruits: { $each: [ "Mango", "Leechi", "Cherry" ] } } })
		 */
		
		{
			Bson filter = eq("_id", 3);
			List<String> vegList = new ArrayList<String>();
			vegList.add("bitter mellon");
			vegList.add("Beetroot");
			vegList.add("Betel leaf");

			Bson update = pushEach("vegetables", vegList);
			fruitVeg.updateOne(filter, update);	
		}
		
		/**
		 * db.fruitVeg.update({ },{ $push: { fruits: { $each: [ "Mango", "Leechi", "Cherry" ] ,$sort: -1 }}},{ multi: true })
		 * if the Array contains documents with multiple fields, then we mention the field name we want to sort with.
		 */
		{
			Bson filter = new Bson() {
				
				public <TDocument> BsonDocument toBsonDocument(Class<TDocument> documentClass, CodecRegistry codecRegistry) {
					return new BsonDocument();
				}
			};
			List<String> vegList = new ArrayList<String>();
			vegList.add("bitter mellon");
			vegList.add("Beetroot");
			vegList.add("Betel leaf");
			PushOptions po = new PushOptions();
			po.sort(-1);
			Bson update =  pushEach("vegetables", vegList,po);
			fruitVeg.updateMany(filter, update);
		}
		
		
		/**
		 * db.fruitVeg.update({ },{ $push: { fruits: { $each: [ "Mango", "Leechi", "Cherry" ] ,$sort: -1, $slice:3 }}},{ multi: true })
		 */
		
		{
			Bson filter = new Bson() {
				
				public <TDocument> BsonDocument toBsonDocument(Class<TDocument> documentClass, CodecRegistry codecRegistry) {
					return new BsonDocument();
				}
			};
			List<String> vegList = new ArrayList<String>();
			vegList.add("bitter mellon");
			vegList.add("Beetroot");
			vegList.add("Betel leaf");
			PushOptions po = new PushOptions();
			po.sort(-1);
			po.slice(3);
			Bson update =  pushEach("vegetables", vegList,po);
			fruitVeg.updateMany(filter, update);
		}
		
		/**
		 * db.fruitVeg.update({ },{ $push: { fruits: { $each: [ "Mango", "Leechi", "Cherry" ] ,$position: 2}}},{ multi: true })
		 */
		
		{
			Bson filter = new Bson() {
				
				public <TDocument> BsonDocument toBsonDocument(Class<TDocument> documentClass, CodecRegistry codecRegistry) {
					return new BsonDocument();
				}
			};
			List<String> vegList = new ArrayList<String>();
			vegList.add("bitter mellon");
			vegList.add("Beetroot");
			vegList.add("Betel leaf");
			PushOptions po = new PushOptions();
			po.position(2);
			Bson update =  pushEach("vegetables", vegList,po);
			fruitVeg.updateMany(filter, update);
		}
		client.close();
		System.out.println("Done");
	}

}
