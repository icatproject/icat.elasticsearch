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

    /**
     * The Java High Level REST Client works on top of the Java Low Level REST client. 
     * Its main goal is to expose API specific methods, that accept request objects as an argument and return response objects, 
     * so that request marshalling and response un-marshalling is handled by the client itself.
     */
    public Elasticsearch() {
        // Elasticsearch constants - to be moved into a config file
        this.index = "investigations";
        this.protocol = "http";
        this.host = "localhost";
        this.port = 9200;
        esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(host, port, protocol)));
    }
    
    /**
     * Note: Disabled due to the required_property check. 
     * Can be bypassed by adding the property with a dummy value in run.properties
     * Permanent fix: To be investigated
     */
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
    
    /**
     * Test "Welcome to Hello world" end point
     * @return String "Hello, World!!!"
     */
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

    /**
     * Version end point
     * @return icat.elasticsearch version
     */
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
                    add(request, entityName, When.Sometime, parser, id);
                }
                ev = parser.next();
                count++;
                ev = parser.next();
            }

        } catch (IOException e) {
            throw new ElasticsearchException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
        }
        logger.debug("Modified {} documents", count);

    }

    private void add(HttpServletRequest request, String entityName, When when, JsonParser parser, Long id) throws ElasticsearchException, IOException {

        AttributeName attName = null;
        String name = null;
        String value = null;
        Double dvalue = null;

        Map<String, Object> jsonMap = new HashMap<>();;
        parser.next();
        while (parser.hasNext()) {
            Event ev = parser.next();
            if (ev == Event.KEY_NAME) {
                try {
                    attName = AttributeName.valueOf(parser.getString());
                } catch (Exception e) {
                    throw new ElasticsearchException(HttpURLConnection.HTTP_BAD_REQUEST,
                            "Found unknown field type " + e.getMessage());
                }
            } else if (ev == Event.VALUE_STRING) {
                if (attName == AttributeName.type) {
                    try {
                        // TODO
                    } catch (Exception e) {
                        throw new ElasticsearchException(HttpURLConnection.HTTP_BAD_REQUEST,
                                "Found unknown field type " + e.getMessage());
                    }
                } else if (attName == AttributeName.name) {
                    name = parser.getString();
                } else if (attName == AttributeName.value) {
                    value = parser.getString();
                } else {
                    throw new ElasticsearchException(HttpURLConnection.HTTP_BAD_REQUEST, "Bad VALUE_STRING " + attName);
                }
            } else if (ev == Event.VALUE_NUMBER) {
                // TODO
            } else if (ev == Event.VALUE_TRUE) {
                // TODO
            } else if (ev == Event.START_OBJECT) {
                // TODO
            } else if (ev == Event.END_OBJECT) {
                jsonMap.put(name, value);

            } else if (ev == Event.END_ARRAY) {
                System.out.println(jsonMap.toString());
                if (id == null) {
                    esClient.index(
                            new IndexRequest(entityName.toLowerCase())
                                    .source(jsonMap), RequestOptions.DEFAULT);
                } else {
                    esClient.update(
                            new UpdateRequest(entityName.toLowerCase(), id.toString())
                                    .doc(jsonMap), RequestOptions.DEFAULT);
                }
                return;
            } else {
                throw new ElasticsearchException(HttpURLConnection.HTTP_BAD_REQUEST, "Unexpected token in Json: " + ev);
            }
        }
    }

    private String searchResult(String entityType, String text, int maxResults) {

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("text", text))
                .from(0)
                .size(maxResults)
                .timeout(new TimeValue(60, TimeUnit.SECONDS));

        SearchRequest searchRequest = new SearchRequest()
                .indices(entityType)
                .source(sourceBuilder);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JsonGenerator gen = Json.createGenerator(baos)) {
            gen.writeStartObject().writeStartArray("results");
            Map<String, Object> map = null;
            try {
                SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
                if (searchResponse.getHits().getTotalHits().value > 0) {
                    for (SearchHit hit : searchResponse.getHits().getHits()) {
                        gen.writeStartObject();
                        map = hit.getSourceAsMap();
                        for (Map.Entry<String, Object> entry : map.entrySet()) {
                            gen.write(entry.getKey(), entry.getValue().toString());
                        }
                        gen.writeEnd();
                    }
                    gen.writeEnd().writeEnd();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return baos.toString();
    }

    /**
     * 
     * @param request Json formatted input parameter
     * @param maxResults max number of results to be returned by ES
     * @return json formatted search results
     * @throws ElasticsearchException
     * @throws IOException 
     * End point to search and query datafiles index
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("datafiles")
    public String datafiles(@Context HttpServletRequest request, @QueryParam("maxResults") int maxResults)
            throws ElasticsearchException, IOException {

        String text = Json.createReader(request.getInputStream())
                .readObject()
                .getString("text");
        return searchResult("datafile", text, maxResults);

    }
    
    /**
     * 
     * @param request Json formatted input parameter
     * @param maxResults max number of results to be returned by ES
     * @return json formatted search results
     * @throws ElasticsearchException
     * @throws IOException 
     * End point to search and query datasets index
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("datasets")
    public String datasets(@Context HttpServletRequest request, @QueryParam("maxResults") int maxResults)
            throws ElasticsearchException, IOException {

        String text = Json.createReader(request.getInputStream())
                .readObject()
                .getString("text");
        return searchResult("dataset", text, maxResults);

    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("investigations")
    public String investigations(@Context HttpServletRequest request, @QueryParam("maxResults") int maxResults)
            throws ElasticsearchException, IOException {

        String text = Json.createReader(request.getInputStream())
                .readObject()
                .getString("text");
        return searchResult("investigation", text, maxResults);

    }

}
