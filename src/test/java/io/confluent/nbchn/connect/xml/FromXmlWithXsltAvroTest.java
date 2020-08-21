package io.confluent.nbchn.connect.xml;

import java.nio.file.Files;
import org.apache.kafka.connect.transforms.Transformation;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;



import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Schema;

public class FromXmlWithXsltAvroTest {

    Transformation<SourceRecord> transform;

    @Before
    public void before() {
        String xsltPath = "file:src/test/resources/io/confluent/nbchn/connect/xml/cd_catalog_transformer.xsl";
        String avscPath = "src/test/avro/cd_catalog_transformed.avsc";
        Map<String, Object> settings = new HashMap<>();
        settings.put("xslt.transformer.path", xsltPath);
        settings.put("schema.avro.path", avscPath);

        this.transform = new FromXml.Value<>();
        this.transform.configure(settings);
    }

    @After
    public void after() {
        this.transform.close();
    }

    @Test
    public void apply() throws Exception {
        String inputTransformerPath = "src/test/resources/io/confluent/nbchn/connect/xml/cd_catalog.xml";

        final String payload = new String(Files.readAllBytes(Paths.get(inputTransformerPath)));
        SourceRecord target = transform.apply(buildSourceRecord(payload));

        System.out.println(target.toString());
    }

    private static SourceRecord buildSourceRecord(String payload) {
        return new SourceRecord(Collections.emptyMap(), Collections.emptyMap(), "sample",
                Schema.OPTIONAL_STRING_SCHEMA, payload);
    }

}
