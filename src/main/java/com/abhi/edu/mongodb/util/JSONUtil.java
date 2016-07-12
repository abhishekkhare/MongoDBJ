package com.abhi.edu.mongodb.util;

import java.io.StringWriter;

import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

public class JSONUtil {
	public static void printJson(Document document){
		JsonWriter jsonWriter = new JsonWriter(new StringWriter(),new JsonWriterSettings(JsonMode.SHELL,true));
		new DocumentCodec().encode(jsonWriter, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
		System.out.println(jsonWriter.getWriter());
		System.out.flush();
	}
	
	public static void printJsonNonFormated(Document document){
		JsonWriter jsonWriter = new JsonWriter(new StringWriter(),new JsonWriterSettings(JsonMode.SHELL,false));
		new DocumentCodec().encode(jsonWriter, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
		System.out.println(jsonWriter.getWriter());
		System.out.flush();
	}

}
