package com.abhi.edu.mongodb.finalexam;

import static com.mongodb.client.model.Filters.eq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class Question7 {

    public static void main(String[] args) throws IOException {

        // db.albums.ensureIndex({"images" : 1})

        MongoClient client =  new MongoClient(new MongoClientURI("mongodb://localhost"));
        MongoDatabase db = client.getDatabase("test");
        MongoCollection<Document> images = db.getCollection("images");
        MongoCollection<Document> albums = db.getCollection("albums");
        
        MongoCursor<Document> cursor = images.find().iterator();
        

        long totalCount = images.count();
        long processedCount = 0;
        long removedCount = 0;

        try {
        	while(cursor.hasNext()){
                Document image = cursor.next();
                Integer imageId = (Integer) image.get("_id");
                Bson filter = eq("images", imageId);
                Bson filter1 = eq("_id", imageId);
                List<Document> all = albums.find(filter).into(new ArrayList<Document>());
                if (all == null || all.size()==0) {
                	images.deleteOne(filter1);
                    ++removedCount;
                    System.out.println("Removed image[" + image + "]");
                }
                ++processedCount;
               
            }
        } finally {
            cursor.close();
            client.close();
        }
        System.out.println("Processed[" + processedCount + "] of [" + totalCount
                + "] images, removed[" + removedCount + "] images");
    }
}
