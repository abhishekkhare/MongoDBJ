package com.abhi.edu.mongodb.week2.read;

import static com.mongodb.client.model.Filters.lt;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import  com.abhi.edu.mongodb.util.JSONUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

/**
 * 
 * @author abhishekkhare
 *
 */
public class FindWithSortSkipLimitTest {

	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("course");
		MongoCollection<Document> collection = db.getCollection("FindWithSortSkipLimitTest");
		collection.drop();
		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < 10; j++) {
				collection.insertOne(new Document().append("i", i).append("j", j));	
			}
		}
		{
			Bson projection = Projections.fields(Projections.include("i","j"),Projections.excludeId());
			List<Document> all = collection.find().projection(projection).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJsonNonFormated(document);
			}
			System.out.println("Count::"+collection.count());	
		}
		
		/**
		 * db.FindWithSortSkipLimitTest.find({j:{$lt:5}},{i:1,j:1,_id:0}).sort({i:1})
		 */
		
		{
			System.out.println("######## Sort Ascending i ########");
			Bson projection = Projections.fields(Projections.include("i","j"),Projections.excludeId());
			Bson sort = new Document("i",1);
			Bson filter = lt("j",5);
			List<Document> all = collection.find().filter(filter).projection(projection).sort(sort).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJsonNonFormated(document);
			}
			System.out.println("Count::"+all.size());	
		}
		/**
		 * db.FindWithSortSkipLimitTest.find({j:{$lt:5}},{i:1,j:1,_id:0}).sort({i:1,j:-1})
		 */
		{
			System.out.println("######## Sort Ascending i and decending j ########");
			Bson projection = Projections.fields(Projections.include("i","j"),Projections.excludeId());
			Bson sort = new Document("i",1).append("j", -1);
			Bson filter = lt("j",5);
			List<Document> all = collection.find().filter(filter).projection(projection).sort(sort).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJsonNonFormated(document);
			}
			System.out.println("Count::"+collection.count());	
		}
		/**
		 * db.FindWithSortSkipLimitTest.find({j:{$lt:5}},{i:1,j:1,_id:0}).sort({i:1})
		 */
		{
			System.out.println("######## Sort Ascending i with builder ########");
			Bson projection = Projections.fields(Projections.include("i","j"),Projections.excludeId());
			Bson sort = Sorts.ascending("i");
			Bson filter = lt("j",5);
			List<Document> all = collection.find().filter(filter).projection(projection).sort(sort).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJsonNonFormated(document);
			}
			System.out.println("Count::"+collection.count());	
		}
		
		/**
		 * db.FindWithSortSkipLimitTest.find({j:{$lt:5}},{i:1,j:1,_id:0}).sort({i:1,j:-1})
		 */
		{
			System.out.println("######## Sort Ascending i and decending j  with builder and order by ########");
			Bson projection = Projections.fields(Projections.include("i","j"),Projections.excludeId());
			Bson sort = Sorts.orderBy(Sorts.ascending("i"),Sorts.descending("j"));
			Bson filter = lt("j",5);
			List<Document> all = collection.find().filter(filter).projection(projection).sort(sort).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJsonNonFormated(document);
			}
			System.out.println("Count::"+collection.count());	
		}
		
		/**
		 * db.FindWithSortSkipLimitTest.find({j:{$lt:5}},{i:1,j:1,_id:0}).sort({i:1,j:-1})
		 */
		{
			System.out.println("######## Sort decending j and decending i with builder ########");
			Bson projection = Projections.fields(Projections.include("i","j"),Projections.excludeId());
			Bson sort = Sorts.descending("j","i");
			Bson filter = lt("j",5);
			List<Document> all = collection.find().filter(filter).projection(projection).sort(sort).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJsonNonFormated(document);
			}
			System.out.println("Count::"+collection.count());	
		}
		
		/**
		 * db.FindWithSortSkipLimitTest.find({j:{$lt:5}},{i:1,j:1,_id:0}).sort({i:1,j:-1}).skip(20).limit(5)
		 */
		
		{
			System.out.println("######## Skip and Limit ########");
			Bson projection = Projections.fields(Projections.include("i","j"),Projections.excludeId());
			Bson sort = Sorts.descending("j","i");
			Bson filter = lt("j",5);
			List<Document> all = collection.find().filter(filter).projection(projection).sort(sort).skip(20).limit(5).into(new ArrayList<Document>());
			for (Document document : all) {
				JSONUtil.printJsonNonFormated(document);
			}
			System.out.println("Count::"+collection.count());	
		}
		client.close();
	}

}
