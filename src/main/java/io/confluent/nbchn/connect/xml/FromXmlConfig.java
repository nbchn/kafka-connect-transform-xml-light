package io.confluent.nbchn.connect.xml;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import com.github.jcustenborder.kafka.connect.utils.config.ConfigUtils;
import com.github.jcustenborder.kafka.connect.utils.config.validators.ValidUrl;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.net.URL;
import java.util.List;
import java.util.Map;

class FromXmlConfig extends AbstractConfig {

    public static final String TRANSFORMER_PATH_CONFIG = "xslt.transformer.path";
    // public static final String SCHEMA_PATH_CONFIG = "schema.xml.path";
    public static final String AVRO_SCHEMA_PATH_CONFIG = "schema.avro.path";
    public static final String PACKAGE_CONFIG = "package";
    static final String SCHEMA_PATH_DOC = "Urls to the schemas to load. http and https paths are supported";
    static final String TRANSFORMER_PATH_DOC = "Url to the xslt transformer file to load.";
    static final String PACKAGE_DOC = "The java package xjc will use to generate the source code in. This name will be applied to the resulting schema";
    static final String AVRO_SCHEMA_DOC = "Avro schema url";

    // public final List<URL> schemaUrls;
    public final String transformerUrl;
    public final String avroSchemaUrl;

    public FromXmlConfig(Map<?, ?> originals) {
        super(config(), originals);
        // this.schemaUrls = ConfigUtils.urls(this, SCHEMA_PATH_CONFIG);
        this.transformerUrl = getString(TRANSFORMER_PATH_CONFIG);
        this.avroSchemaUrl = getString(AVRO_SCHEMA_PATH_CONFIG);
    }

    public static ConfigDef config() {

        return new ConfigDef()
//                .define(
//                        ConfigKeyBuilder.of(SCHEMA_PATH_CONFIG, ConfigDef.Type.LIST)
//                                .documentation(SCHEMA_PATH_DOC)
//                                .importance(ConfigDef.Importance.HIGH)
//                                .validator(new ValidUrl())
//                                .build())
                .define(
                        ConfigKeyBuilder.of(PACKAGE_CONFIG, ConfigDef.Type.STRING)
                                .documentation(PACKAGE_DOC)
                                .importance(ConfigDef.Importance.HIGH)
                                .defaultValue(FromXmlConfig.class.getPackage().getName() + ".model")
                                .build()
                ).define(
                        ConfigKeyBuilder.of(TRANSFORMER_PATH_CONFIG, ConfigDef.Type.STRING)
                                .documentation(TRANSFORMER_PATH_DOC)
                                .importance(ConfigDef.Importance.LOW)
                                .defaultValue("")
                                .build()
                ).define(
                        ConfigKeyBuilder.of(AVRO_SCHEMA_PATH_CONFIG, ConfigDef.Type.STRING)
                                .documentation(AVRO_SCHEMA_DOC)
                                .importance(ConfigDef.Importance.LOW)
                                .defaultValue("")
                                .build()
                );
    }
}