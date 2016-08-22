package com.abhi.edu.mongodb.week3.hw.course;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;

public class BlogPostDAO {
	MongoCollection<Document> postsCollection;

	public BlogPostDAO(final MongoDatabase blogDatabase) {
		postsCollection = blogDatabase.getCollection("posts");
	}

	// Return a single post corresponding to a permalink
	public Document findByPermalink(String permalink) {
		Document post = postsCollection.find(eq("permalink", permalink)).first();
		return post;
	}

	// Return a list of posts in descending order. Limit determines
	// how many posts are returned.
	public List<Document> findByDateDescending(int limit) {
		return postsCollection.find().sort(Sorts.descending("date")).limit(limit).into(new ArrayList<Document>());
	}

	@SuppressWarnings("rawtypes")
	public String addPost(String title, String body,  List tags, String username) {

		System.out.println("inserting blog entry " + title + " " + body);

		String permalink = title.replaceAll("\\s", "_"); // whitespace becomes _
		permalink = permalink.replaceAll("\\W", ""); // get rid of non
														// alphanumeric
		permalink = permalink.toLowerCase();

		String permLinkExtra = String.valueOf(GregorianCalendar.getInstance().getTimeInMillis());
		permalink += permLinkExtra;

		Document post = new Document("title", title);
		post.append("author", username);
		post.append("body", body);
		post.append("permalink", permalink);
		post.append("tags", tags);
		post.append("comments", new java.util.ArrayList());
		post.append("date", new java.util.Date());

		try {
			postsCollection.insertOne(post);
			System.out.println("Inserting blog post with permalink " + permalink);
		} catch (Exception e) {
			System.out.println("Error inserting post");
			return null;
		}

		return permalink;
	}

	public void addPostComment(final String name, final String email, final String body, final String permalink) {
		Document comment = new Document("author", name).append("body", body);
		if (email != null && !email.equals("")) {
			comment.append("email", email);
		}

		postsCollection.updateOne(Filters.eq("permalink", permalink), Updates.push("comments", comment));
	}
}
