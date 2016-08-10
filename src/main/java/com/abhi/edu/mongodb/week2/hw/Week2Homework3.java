package com.abhi.edu.mongodb.week2.hw;

import static com.mongodb.client.model.Filters.eq;

import java.net.UnknownHostException;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.abhi.edu.mongodb.util.JSONUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
Write a program in the language of your choice that will remove the grade
of type "homework" with the lowest score for each student from the dataset
that you imported in HW 2.1. Since each document is one grade, it should
remove one document per student.
 */
public class Week2Homework3 {
    public static void main(String[] args) throws UnknownHostException {
		MongoClient client = new MongoClient();
		MongoDatabase db = client.getDatabase("students");
		MongoCollection<Document> collection = db.getCollection("grades");
		Bson filter = eq("type", "homework");
		Bson sort = new Document("student_id",1).append("score", 1);
		MongoCursor<Document> cursor = collection.find(filter).sort(sort).iterator();
		int curStudentId=-1;
		try{
			while(cursor.hasNext()){
				Document document = cursor.next();
				JSONUtil.printJson(document);
				int studentId = document.getInteger("student_id");
				if (studentId != curStudentId) {
				System.out.println("Deleting..........");	
                  collection.deleteOne(document);
                  curStudentId = studentId;
              }
			}
		}finally{
			cursor.close();
		}
		client.close();
    }
}
