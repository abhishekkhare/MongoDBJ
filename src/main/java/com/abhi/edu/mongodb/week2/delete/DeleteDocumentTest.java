package com.abhi.edu.mongodb.week2.delete;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.*;

import java.util.ArrayList;
import java.util.List;

import com.abhi.edu.mongodb.util.JSONUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class DeleteDocumentTest {

	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("test");
		MongoCollection<Document> fruitVeg = db.getCollection("fruitVeg");
		
		
		/**
		 * Remove with filter
		 * db.fruitVeg.remove({_id:1})
		 */
		{
			Bson filter = eq("_id", 3);
			fruitVeg.deleteMany(filter);	
			List<Document> all = fruitVeg.find().filter(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}	
		}
		
		/**
		 * Remove Single Document
		 * db.fruitVeg.remove({},1)
		 */
		
		{
			Bson filter = new Bson() {

				public <TDocument> BsonDocument toBsonDocument(Class<TDocument> documentClass,
						CodecRegistry codecRegistry) {
					return new BsonDocument();
				}
			};
			fruitVeg.deleteOne(filter);
			List<Document> all = fruitVeg.find().filter(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
			
		}
		/**
		 * Remove all documents. 
		 * db.fruitVeg.remove({})
		 */
		{
			Bson filter = new Bson() {

				public <TDocument> BsonDocument toBsonDocument(Class<TDocument> documentClass,
						CodecRegistry codecRegistry) {
					return new BsonDocument();
				}
			};
			
			fruitVeg.deleteMany(filter);
			List<Document> all = fruitVeg.find().filter(filter).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJson(document);
			}
		}
		
		
		client.close();
		System.out.println("Done");
	}

}
