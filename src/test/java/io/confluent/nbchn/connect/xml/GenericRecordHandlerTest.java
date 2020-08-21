package io.confluent.nbchn.connect.xml;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;
import static org.junit.Assert.*;


import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;

public class GenericRecordHandlerTest {

    @Test
    public void flatAvroToGenericRecord() throws Exception {

        // load & parse schema
        Schema schema = new Schema.Parser().parse(new File("src/test/avro/flat_avro.avsc"));

        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser = factory.newSAXParser();

        GenericRecordHandler handler = new GenericRecordHandler(schema);
        saxParser.parse("src/test/resources/io/confluent/nbchn/connect/xml/flat_avro.xml", handler);

        GenericRecord output = handler.getGenericRecord();
        assertEquals(output, generateDataFlatAvroData(schema));
    }

    @Test
    public void nestedAvroToGenericRecord() throws Exception {

        // load & parse schema
        Schema schema = new Schema.Parser().parse(new File("src/test/avro/nested_avro.avsc"));

        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser = factory.newSAXParser();

        GenericRecordHandler handler = new GenericRecordHandler(schema);
        saxParser.parse("src/test/resources/io/confluent/nbchn/connect/xml/nested_avro.xml", handler);

        GenericRecord output = handler.getGenericRecord();
        assertEquals(output, generateDataNestedAvroData(schema));
    }

    @Test
    public void nestedAvroWithArrayToGenericRecord() throws Exception {

        // load & parse schema
        Schema schema = new Schema.Parser().parse(new File("src/test/avro/nested_avro_with_array.avsc"));

        SAXParserFactory factory = SAXParserFactory.newInstance();
        SAXParser saxParser = factory.newSAXParser();

        GenericRecordHandler handler = new GenericRecordHandler(schema);
        saxParser.parse("src/test/resources/io/confluent/nbchn/connect/xml/nested_avro_with_array.xml", handler);

        GenericRecord output = handler.getGenericRecord();
        assertEquals(output, generateDataNestedAvroWithArrayData(schema));
    }

    public GenericRecord generateDataFlatAvroData(Schema schema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("first", "nils");
        builder.set("last", "bouchardon");
        return  builder.build();
    }

    public GenericRecord generateDataNestedAvroData(Schema schema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("firstname", "nils");
        builder.set("lastname", "bouchardon");
        GenericRecordBuilder builderAddress = new GenericRecordBuilder(schema.getField("address").schema());
        builderAddress.set("streetaddress", "champs elysees");
        builderAddress.set("city", "paris");
        builder.set("address", builderAddress.build());
        return  builder.build();
    }

    public GenericRecord generateDataNestedAvroWithArrayData(Schema schema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("firstname", "nils");
        builder.set("lastname", "bouchardon");
        GenericData.Array siblings = new GenericData.Array(2, schema.getField("siblings").schema());
        siblings.add(
                new GenericRecordBuilder(
                        schema.getField("siblings").schema().getElementType()
                ).set("name", "iona").build());
        siblings.add(
                new GenericRecordBuilder(
                        schema.getField("siblings").schema().getElementType()
                ).set("name", "liz").build());
        builder.set("siblings", siblings);
        return  builder.build();
    }
}
