package org.icatproject.elasticsearch;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestHelloWorld {
	@Test
	public void test() throws Exception {
		Elasticsearch a = new Elasticsearch();
		assertEquals("{\"Hello\":\"World\"}", a.hello());
	}
}