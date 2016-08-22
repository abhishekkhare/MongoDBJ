package com.abhi.edu.mongodb.week3.hw;

import static com.mongodb.client.model.Filters.eq;

import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.abhi.edu.mongodb.util.JSONUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;

/**
 * 
 * @author abhishekkhare
 *
 */
public class Week3Homework1 {
	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("school");
		MongoCollection<Document> collection = db.getCollection("students");
		MongoCursor<Document> cursor = collection.find().iterator();
		try{
			while(cursor.hasNext()){
				Document document = cursor.next();
				int studentID = document.getInteger("_id");
				String name = document.getString("name");
				@SuppressWarnings("unchecked")
				List<Document> scores  = (List<Document>)document.get("scores");
				System.out.println("_id:" + studentID + " name:" +name);
				double minScoreValue = 100.0;
				Document scoreToRemove = null;
				for (Document scoreItem : scores) {
					String type = scoreItem.getString("type");
					double score = scoreItem.getDouble("score");
					if(!type.equals("homework")){
						 continue;
					}
					System.out.println("type:" + type +" score:" + score);
					double curScoreValue = score;
					if (curScoreValue < minScoreValue) {
                        scoreToRemove = scoreItem;
                        minScoreValue = curScoreValue;
                    }
				}
				
				if (scoreToRemove != null) {
					scores.remove(scoreToRemove);
					System.out.println("scoreToRemove:" + scoreToRemove);
					Bson filter = eq("_id",studentID);
					System.out.println(document);
					UpdateResult result =  collection.replaceOne(filter, document);
					System.out.println("count: "+result.getModifiedCount()  + " _id:" + result.getUpsertedId());
				}
						
				JSONUtil.printJson(document);
			}
		}finally{
			cursor.close();
			client.close();
		}
	}
	/**
	 * Query : db.students.aggregate( [ { '$unwind': '$scores' },{'$group':{ '_id': '$_id','average': { $avg: '$scores.score' }}},{ '$sort': { 'average' : -1 } },{ '$limit': 1 } ] )
	 * Answer : { "_id" : 13, "average" : 91.98315917172745 }
	 */
}
