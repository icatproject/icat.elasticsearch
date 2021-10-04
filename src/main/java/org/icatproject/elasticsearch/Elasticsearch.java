package org.icatproject.elasticsearch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.ejb.Stateless;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import org.icatproject.elasticsearch.exceptions.ElasticsearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/* Mapped name is to avoid name clashes */
@Path("/")
@Stateless
public class Elasticsearch {

    enum AttributeName {
        type, name, value, date, store
    }

    enum When {
        Now, Sometime
    }

    private static final Logger logger = LoggerFactory.getLogger(Elasticsearch.class);
    private static final Marker fatal = MarkerFactory.getMarker("FATAL");

    private String required_property;
    private String message;

    private final String index;
    private final String host;
    private final int port;
    private final String protocol;
    private final RestHighLevelClient esClient;

    public Elasticsearch() {
        this.index = "investigations";
        this.protocol = "http";
        this.host = "localhost";
        this.port = 9200;
        esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, port, protocol)));
    }

    @PostConstruct
    private void init() {
//		CheckedProperties props = new CheckedProperties();
//		try {
//			props.loadFromResource("run.properties");
//
//			required_property = props.getString("required_property");
//
//			if (props.has("message")) {
//				message = props.getString("message");
//			} else {
//				message = null;
//			}
//
//		} catch (CheckedPropertyException e) {
//			logger.error(fatal, e.getMessage());
//			throw new IllegalStateException(e.getMessage());
//		}
//
//		logger.info("Initialized Elasticsearch with required_property: " + required_property);
    }

    @GET
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public String HelloWorld() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonGenerator gen = Json.createGenerator(baos);
        gen.writeStartObject().write("Message", "Hello, World!!!").writeEnd();
        gen.close();
        return baos.toString();
    }

    @GET
    @Path("version")
    @Produces(MediaType.APPLICATION_JSON)
    public String getVersion() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonGenerator gen = Json.createGenerator(baos);
        gen.writeStartObject().write("version", Constants.API_VERSION).writeEnd();
        gen.close();
        return baos.toString();
    }

    @GET
    @Path("hello")
    @Produces(MediaType.APPLICATION_JSON)
    public String hello() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonGenerator gen = Json.createGenerator(baos);
        gen.writeStartObject().write("Hello", "World").writeEnd();
        gen.close();
        return baos.toString();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("modify")
    public void modify(@Context HttpServletRequest request) throws ElasticsearchException {

        logger.debug("Requesting modify");
        int count = 0;

        try (JsonParser parser = Json.createParser(request.getInputStream())) {
            Event ev = parser.next();
            if (ev != Event.START_ARRAY) {
                throw new ElasticsearchException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Unexpected " + ev.name());
            }
            ev = parser.next();
            while (true) {
                if (ev == Event.END_ARRAY) {
                    break;
                }
                if (ev != Event.START_ARRAY) {
                    throw new ElasticsearchException(HttpURLConnection.HTTP_INTERNAL_ERROR, "Unexpected " + ev.name());
                }
                ev = parser.next();
                String entityName = parser.getString();
                ev = parser.next();
                Long id = (ev == Event.VALUE_NULL) ? null : parser.getLong();
                ev = parser.next();
                if (ev == Event.VALUE_NULL) {
//                    try {
//                        IndexBucket bucket = indexBuckets.computeIfAbsent(entityName, k -> createBucket(k));
//                        if (bucket.locked.get()) {
//                            throw new ElasticsearchException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
//                                    "Lucene locked for " + entityName);
//                        }
//                        bucket.indexWriter.deleteDocuments(new Term("id", Long.toString(id)));
//                    } catch (IOException e) {
//                        throw new ElasticsearchException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
//                    }
                } else {
                    System.out.println("Modify call");
                    System.out.println(entityName);
                    System.out.println(When.Sometime);
                    System.out.println(id);
                    add(request, entityName, When.Sometime, parser, id);
                }
                ev = parser.next(); // end of triple
                count++;
                ev = parser.next(); // either end of input or start of new
                // triple
            }

        } catch (IOException e) {
            throw new ElasticsearchException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
        }
        logger.debug("Modified {} documents", count);

    }

}
