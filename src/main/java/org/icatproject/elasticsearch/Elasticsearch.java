package org.icatproject.elasticsearch;

import java.io.ByteArrayOutputStream;

import javax.annotation.PostConstruct;
import javax.ejb.Stateless;
import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/* Mapped name is to avoid name clashes */
@Path("/")
@Stateless
public class Elasticsearch {

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
}
