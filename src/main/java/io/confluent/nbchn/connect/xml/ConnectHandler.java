package io.confluent.nbchn.connect.xml;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

public class ConnectHandler extends DefaultHandler {

    private final Schema schema;
    private final GenericRecordBuilder builder;
    private List<Schema.Field> currentFields;
    private Map<String, Object> test = new HashMap<>();
    private Stack<String> stack = new Stack<>();
    private Object currentValue;
    private int depth = 0;

    public ConnectHandler(Schema schema) throws ClassNotFoundException {
        this.schema = schema;
        this.builder = new GenericRecordBuilder(schema);
    }

    @Override
    public void startDocument() throws SAXException {
        System.out.println("Start building :"+schema.getName());
        currentFields = schema.getFields();
    }

    public void startElement(String uri, String localName, String qName,
                             Attributes attributes) throws SAXException {

        System.out.println("Add elemt to stack : " + qName);
        stack.add("qName");



        for(Schema.Field field : currentFields) {
            System.out.println(field.name());
            if(field.name().equals(qName)) {
//                System.out.println("matched with schema");
//                System.out.println(field.schema().getType());
            }
        }

    }

    public void endElement(String uri, String localName, String qName) throws SAXException {
        System.out.println("End Element :" + qName);
        String currentElement = stack.pop();
        if(!currentElement.equals(qName)) {
            throw new SAXException();
        }

        if( currentValue != null) {
            System.out.println(currentElement+" == "+currentValue);

            currentValue = new HashMap<String, Object>();
        } else {
            System.out.println("close object");
        }

    }

    public void characters(char ch[], int start, int length) throws SAXException {
        currentValue = new String(ch, start, length);
    }

}
