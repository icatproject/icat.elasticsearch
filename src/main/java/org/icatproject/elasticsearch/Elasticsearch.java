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

    private void add(HttpServletRequest request, String entityName, When when, JsonParser parser, Long id) throws ElasticsearchException, IOException {
        //IndexBucket bucket = indexBuckets.computeIfAbsent(entityName, k -> createBucket(k));

        AttributeName attName = null;
//        FieldType fType = null;
        String name = null;
        String value = null;
        Double dvalue = null;
//        Document doc = new Document();
        Map<String, Object> jsonMap = new HashMap<>();;
        parser.next(); // Skip the [
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
                        // fType = FieldType.valueOf(parser.getString());
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
//                long num = parser.getLong();
//                if (fType == FieldType.SortedDocValuesField) {
//                    value = Long.toString(num);
//                } else if (fType == FieldType.DoubleField) {
//                    dvalue = parser.getBigDecimal().doubleValue();
//                } else {
//                    throw new ElasticsearchException(HttpURLConnection.HTTP_BAD_REQUEST,
//                            "Bad VALUE_NUMBER " + attName + " " + fType);
//                }
            } else if (ev == Event.VALUE_TRUE) {
//                if (attName == AttributeName.store) {
//                    store = Store.YES;
//                } else {
//                    throw new ElasticsearchException(HttpURLConnection.HTTP_BAD_REQUEST, "Bad VALUE_TRUE " + attName);
//                }
            } else if (ev == Event.START_OBJECT) {
//                fType = null;
//                name = null;
//                value = null;
//                store = Store.NO;
            } else if (ev == Event.END_OBJECT) {
                System.out.println("TextField");
                System.out.println(name + ":" + value);
                jsonMap.put(name, value);

//                if (fType == FieldType.TextField) {
//                    doc.add(new TextField(name, value, store));
//                } else if (fType == FieldType.StringField) {
//                    doc.add(new StringField(name, value, store));
//                } else if (fType == FieldType.SortedDocValuesField) {
//                    doc.add(new SortedDocValuesField(name, new BytesRef(value)));
//                } else if (fType == FieldType.DoubleField) {
//                    doc.add(new DoubleField(name, dvalue, store));
//                }
            } else if (ev == Event.END_ARRAY) {
                System.out.println(jsonMap.toString());
                if (id == null) {
                    IndexRequest indexRequest = new IndexRequest(entityName.toLowerCase()).source(jsonMap);
                    esClient.index(indexRequest, RequestOptions.DEFAULT);
                } else {
                    UpdateRequest updateRequest = new UpdateRequest(entityName.toLowerCase(), id.toString()).doc(jsonMap);
                    esClient.update(updateRequest, RequestOptions.DEFAULT);
                }
                return;
            } else {
                throw new ElasticsearchException(HttpURLConnection.HTTP_BAD_REQUEST, "Unexpected token in Json: " + ev);
            }
        }
    }

    private String searchResult(String entityType, String text, int maxResults) {

        
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("datafiles")
    public String datafiles(@Context HttpServletRequest request, @QueryParam("maxResults") int maxResults)
            throws ElasticsearchException, IOException {

        JsonReader r = Json.createReader(request.getInputStream());
        JsonObject o = r.readObject();
        String text = o.getString("text");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("text", text));
        sourceBuilder.from(0);
        sourceBuilder.size(maxResults);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(entityType);
        searchRequest.source(sourceBuilder);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonGenerator gen = Json.createGenerator(baos);
        gen.writeStartObject();
        gen.writeStartArray("results");
        SearchHit[] searchHit = null;
        Map<String, Object> map = null;
        try {
            SearchResponse searchResponse = null;
            searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            if (searchResponse.getHits().getTotalHits().value > 0) {
                searchHit = searchResponse.getHits().getHits();
                for (SearchHit hit : searchHit) {
                    gen.writeStartObject();
                    map = hit.getSourceAsMap();
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        System.out.println(entry.getKey() + " => " + entry.getValue().toString());
                        gen.write(entry.getKey(), entry.getValue().toString());
                    }
                    gen.writeEnd();
                }
                gen.writeEnd(); // array results
                gen.writeEnd(); // object
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        gen.close();
        System.out.println("Hello: " + baos.toString());
        return baos.toString();

    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("datasets")
    public String datasets(@Context HttpServletRequest request, @QueryParam("maxResults") int maxResults)
            throws ElasticsearchException, IOException {

        JsonReader r = Json.createReader(request.getInputStream());
        JsonObject o = r.readObject();
        String text = o.getString("text");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("text", text));
        sourceBuilder.from(0);
        sourceBuilder.size(maxResults);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(entityType);
        searchRequest.source(sourceBuilder);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonGenerator gen = Json.createGenerator(baos);
        gen.writeStartObject();
        gen.writeStartArray("results");
        SearchHit[] searchHit = null;
        Map<String, Object> map = null;
        try {
            SearchResponse searchResponse = null;
            searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            if (searchResponse.getHits().getTotalHits().value > 0) {
                searchHit = searchResponse.getHits().getHits();
                for (SearchHit hit : searchHit) {
                    gen.writeStartObject();
                    map = hit.getSourceAsMap();
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        System.out.println(entry.getKey() + " => " + entry.getValue().toString());
                        gen.write(entry.getKey(), entry.getValue().toString());
                    }
                    gen.writeEnd();
                }
                gen.writeEnd(); // array results
                gen.writeEnd(); // object
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        gen.close();
        System.out.println("Hello: " + baos.toString());
        return baos.toString();

    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("investigations")
    public String investigations(@Context HttpServletRequest request, @QueryParam("maxResults") int maxResults)
            throws ElasticsearchException, IOException {

        JsonReader r = Json.createReader(request.getInputStream());
        JsonObject o = r.readObject();
        String text = o.getString("text");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("text", text));
        sourceBuilder.from(0);
        sourceBuilder.size(maxResults);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(entityType);
        searchRequest.source(sourceBuilder);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonGenerator gen = Json.createGenerator(baos);
        gen.writeStartObject();
        gen.writeStartArray("results");
        SearchHit[] searchHit = null;
        Map<String, Object> map = null;
        try {
            SearchResponse searchResponse = null;
            searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            if (searchResponse.getHits().getTotalHits().value > 0) {
                searchHit = searchResponse.getHits().getHits();
                for (SearchHit hit : searchHit) {
                    gen.writeStartObject();
                    map = hit.getSourceAsMap();
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        System.out.println(entry.getKey() + " => " + entry.getValue().toString());
                        gen.write(entry.getKey(), entry.getValue().toString());
                    }
                    gen.writeEnd();
                }
                gen.writeEnd(); // array results
                gen.writeEnd(); // object
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        gen.close();
        System.out.println("Hello: " + baos.toString());
        return baos.toString();

    }

}
