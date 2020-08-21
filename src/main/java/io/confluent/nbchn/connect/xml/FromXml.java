/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.nbchn.connect.xml;

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationTip;
import com.github.jcustenborder.kafka.connect.utils.config.Title;
import com.github.jcustenborder.kafka.connect.utils.transformation.BaseKeyValueTransformation;
import org.apache.avro.Schema;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import org.xml.sax.InputSource;
import java.io.*;
import java.util.Map;

@Title("FromXML")
@Description("This transformation is used to read XML data stored as bytes or a string and convert " +
        "the XML to a structure that is strongly typed in connect. This allows data to be converted from XML " +
        "and stored as AVRO in a topic for example. ")
@DocumentationTip("XML schemas can be much more complex that what can be expressed in a Kafka " +
        "Connect struct. Elements that can be expressed as an anyType or something similar cannot easily " +
        "be used to infer type information.")
public abstract class FromXml<R extends ConnectRecord<R>> extends BaseKeyValueTransformation<R> {
    private static final Logger log = LoggerFactory.getLogger(FromXml.class);
    FromXmlConfig config;
    Transformer transformer;
    SAXParser parser;
    GenericRecordHandler handler;

    protected FromXml(boolean isKey) {
        super(isKey);
    }

    @Override
    public ConfigDef config() {
        return FromXmlConfig.config();
    }

    @Override
    public void close() {
    }

    @Override
    protected SchemaAndValue processString(R record, org.apache.kafka.connect.data.Schema inputSchema, String input) {
        try (Reader reader = new StringReader(input)) {
            StreamResult result = new StreamResult(new StringWriter());
            this.transformer.transform(new StreamSource(reader), result);
            this.parser.parse(new InputSource(new StringReader(result.getWriter().toString())), this.handler);
            return new SchemaAndValue(null, this.handler.getGenericRecord());
        } catch (TransformerException | IOException | SAXException e) {
            throw new DataException("Exception thrown while processing xml", e);
        }
    }

    @Override
    protected SchemaAndValue processBytes(R record, org.apache.kafka.connect.data.Schema inputSchema, byte[] input) {
        try (InputStream inputStream = new ByteArrayInputStream(input)) {
            Reader reader = new InputStreamReader(inputStream);
            StreamResult result = new StreamResult(new StringWriter());
            this.transformer.transform(new StreamSource(reader), result);

            return new SchemaAndValue(null, null);
        } catch (IOException | TransformerException e) {
            throw new DataException("Exception thrown while processing xml", e);
        }
    }



    @Override
    public void configure(Map<String, ?> settings) {
        this.config = new FromXmlConfig(settings);

        try {
            this.transformer = this.config.transformerUrl.isEmpty() ?
                    TransformerFactory.newInstance().newTransformer() :
                    TransformerFactory.newInstance().newTransformer(new StreamSource(this.config.transformerUrl));
        } catch (TransformerConfigurationException e) {
            throw new IllegalStateException(e);
        }

        try {
            Schema schema = new Schema.Parser().parse(new File(this.config.avroSchemaUrl));
            this.parser = SAXParserFactory.newInstance().newSAXParser();
            this.handler = new GenericRecordHandler(schema);

        } catch (IOException | SAXException | ParserConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }

    public static class Key<R extends ConnectRecord<R>> extends FromXml<R> {
        public Key() {
            super(true);
        }

        @Override
        public R apply(R r) {
            final SchemaAndValue transformed = process(r, new SchemaAndValue(r.keySchema(), r.key()));

            return r.newRecord(
                    r.topic(),
                    r.kafkaPartition(),
                    transformed.schema(),
                    transformed.value(),
                    r.valueSchema(),
                    r.value(),
                    r.timestamp()
            );
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends FromXml<R> {
        public Value() {
            super(false);
        }

        @Override
        public R apply(R r) {
            final SchemaAndValue transformed = process(r, new SchemaAndValue(r.valueSchema(), r.value()));

            return r.newRecord(
                    r.topic(),
                    r.kafkaPartition(),
                    r.keySchema(),
                    r.key(),
                    transformed.schema(),
                    transformed.value(),
                    r.timestamp()
            );
        }
    }
}
