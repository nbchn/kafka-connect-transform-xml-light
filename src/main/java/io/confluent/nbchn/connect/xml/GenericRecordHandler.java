package io.confluent.nbchn.connect.xml;

import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

/**
 * XML SAX parser that transform XML to Avro GenericRecord according to the provided schema.
 */

public class GenericRecordHandler extends DefaultHandler {

    private final Schema schema;
    private final Deque<KeyValue> containers;

    private boolean isInArray;

    /**
     * @param schema Avro schema of the output
     */
    public GenericRecordHandler(Schema schema) {
        this.schema = schema;
        this.containers = new ArrayDeque<>();
        this.isInArray = false;
    }

    @Override
    public void startElement (String uri, String localName, String qName, Attributes attributes) throws SAXException {
        Schema currentSchema = containers.isEmpty() ? schema : getChildSchema(qName, containers.peekLast().getSchema());
        generateAndStackEnvelope(qName, currentSchema);
    }

    @Override
    public void endElement (String uri, String localName, String qName) throws SAXException {
        if (containers.size() > 1) {
            KeyValue childKV = containers.pollLast();
            attachChildToParent(containers.peekLast(), childKV);
        }
    }

    @Override
    public void characters (char[] ch, int start, int length) throws SAXException {
        if (!containers.isEmpty()) {
            containers.peekLast().setStringValue(new String(ch, start, length));
        } else {
            throw new SAXException();
        }
    }


    public GenericRecord getGenericRecord () throws SAXException {
        if((containers.size() == 1) && (containers.peekLast().getValue() instanceof GenericRecord)) {
            return (GenericRecord) containers.pollLast().getValue();
        }

        throw new SAXException();
    }

    public Schema getSchema () {
        return this.schema;
    }

    /**
     * Attach child to parent.
     * If parent is a Record, the child value (nested or flat) is assigned to parent as a Field.
     * If parent is an Array, the child value is added to it, if child is a flat value, it is wrapped into a Record.
     * @param parent previous element on stack
     * @param child current element
     * @throws SAXException child's value type is unsupported
     */
    private void attachChildToParent(KeyValue parent, KeyValue child) throws SAXException {
        if (parent.getValue() instanceof GenericRecord) {
            GenericRecord parentRecord = (GenericRecord) parent.getValue();
            if(child.getValue() instanceof String) {
                Schema.Type type = parentRecord.getSchema().getField(child.getKey()).schema().getType();
                parentRecord.put(child.getKey(), ensureType(type, (String) child.getValue()));
            } else if (child.getValue() instanceof GenericContainer) {
                parentRecord.put(child.getKey(), child.getValue());
            } else {
                throw new SAXException();
            }
        } else if (parent.getValue() instanceof GenericArray) {
            GenericArray parentArray = (GenericArray) parent.getValue();
            if(child.getValue() instanceof String) {
                Schema elementSchema = parentArray.getSchema().getElementType();
                GenericRecord wrappingRecord = new GenericRecordBuilder(elementSchema)
                    .set(
                        child.getKey(),
                        ensureType(elementSchema.getField(child.getKey()).schema().getType(), (String) child.getValue())
                    )
                    .build();
                parentArray.add(wrappingRecord);
            } else if (child.getValue() instanceof GenericContainer) {
                parentArray.add(child.getValue());
            } else {
                throw new SAXException();
            }
        }
    }

    /**
     * Ensure that the flat value added to the Avro Generic Container respect schema's type.
     * @param type type of the flat field
     * @param value value of the flat field
     * @return typed object
     * @throws SAXException unsupported / unimplemented type
     */
    private Object ensureType(Schema.Type type, String value) throws SAXException {

        switch(type) {
            case BOOLEAN:
                return "true".equals(value) || "1".equals(value);
            case INT:
                return Integer.parseInt(value);
            case LONG:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case STRING:
                return value;
            default:
                throw new SAXException("Unsupported type " + type);
        }
    }

    /**
     * For every new element, it adds an empty envelope (Record, Array, flat) to a stack.
     * @param key fieldName
     * @param currentSchema schema of the empty envelope
     */
    private void generateAndStackEnvelope(String key, Schema currentSchema) throws SAXException {

        switch (currentSchema.getType()) {
            case RECORD:
                containers.add(new KeyValue(key, new GenericData.Record(currentSchema), currentSchema));
                break;
            case ARRAY:
                containers.add(new KeyValue(key, new GenericData.Array<>(currentSchema, new ArrayList<>()), currentSchema));
                containers.add(new KeyValue(key, new GenericData.Record(currentSchema.getElementType()), currentSchema.getElementType()));
                isInArray = true;
                break;
            default:
                KeyValue childKV =  containers.peekLast();
                // Note for myself:
                // avoid overwriting in arrays... but dirty...
                // special treatment to single value records...
                // The problem here is that we have to wait that the next element of the array is opened to close
                // the previous element otherwise we cannot distinguished closing a field in a record and closing the
                // record itself.
                if (childKV.isSingleFieldRecord()
                        && ((GenericRecord) childKV.getValue()).get(key) != null ) {
                    endElement("", "", "");
                    generateAndStackEnvelope(childKV.getKey(), childKV.getSchema());
                }
                containers.add(new KeyValue(key, currentSchema));

                break;
        }
    }

    /**
     * Get child schema from parent schema.
     *
     * @param fieldName name of the field
     * @param parentSchema Avro Schema of the parent
     * @return Avro schema of child
     * @throws SAXException unsupported / unimplemented type
     */
    private Schema getChildSchema(String fieldName, Schema parentSchema) throws SAXException {
        switch (parentSchema.getType()) {
            case RECORD:
                return parentSchema.getField(fieldName).schema();
            case ARRAY:
                return parentSchema.getElementType();
            default:
                throw new SAXException();
        }
    }

    /**
     * Internal usage only :
     * Wrapper of the elements that will be stacked. Containing the field name, the value, and the
     * associated schema.
     */
    private static class KeyValue {
        private final String key;
        private final Schema schema;
        private Object value;
        private final boolean isSingleFieldRecord;


        public KeyValue(String key, Object value, Schema schema) {
            this.key = key;
            this.value = value;
            this.schema = schema;
            this.isSingleFieldRecord = (schema.getType() == Schema.Type.RECORD) && (schema.getFields().size() == 1);
        }

        public KeyValue(String key, Schema schema) {
            this.key = key;
            this.value = null;
            this.schema = schema;
            isSingleFieldRecord = false;
        }

        public  boolean isSingleFieldRecord() {
            return isSingleFieldRecord;
        }

        public String getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }

        public Schema getSchema() {
            return schema;
        }

        public void setStringValue(String value) {
            this.value = value;
        }
    }

}
