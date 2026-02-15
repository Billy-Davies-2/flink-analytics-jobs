package com.homelab.flink.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration utility for setting up Apache Iceberg catalog with Nessie.
 * 
 * This class provides methods to configure the Flink Table API to work with
 * Iceberg tables stored in an S3-compatible object store, using Nessie as
 * the catalog backend for table versioning and management.
 */
public class IcebergCatalogConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogConfig.class);

    // Default configuration values
    public static final String DEFAULT_NESSIE_URI = "http://nessie.analytics.svc.cluster.local:19120/api/v2";
    public static final String DEFAULT_WAREHOUSE = "s3a://iceberg-warehouse";
    public static final String DEFAULT_CATALOG_NAME = "nessie";
    public static final String DEFAULT_BRANCH = "main";

    // ConfigOptions for type-safe configuration
    public static final ConfigOption<String> NESSIE_URI = ConfigOptions
        .key("nessie.uri")
        .stringType()
        .defaultValue(DEFAULT_NESSIE_URI)
        .withDescription("Nessie server URI");

    public static final ConfigOption<String> WAREHOUSE = ConfigOptions
        .key("iceberg.warehouse")
        .stringType()
        .defaultValue(DEFAULT_WAREHOUSE)
        .withDescription("Iceberg warehouse location");

    public static final ConfigOption<String> CATALOG_NAME = ConfigOptions
        .key("iceberg.catalog.name")
        .stringType()
        .defaultValue(DEFAULT_CATALOG_NAME)
        .withDescription("Catalog name");

    public static final ConfigOption<String> NESSIE_BRANCH = ConfigOptions
        .key("nessie.branch")
        .stringType()
        .defaultValue(DEFAULT_BRANCH)
        .withDescription("Nessie branch name");

    public static final ConfigOption<String> S3_ENDPOINT = ConfigOptions
        .key("s3.endpoint")
        .stringType()
        .noDefaultValue()
        .withDescription("S3 endpoint URL");

    public static final ConfigOption<String> S3_ACCESS_KEY = ConfigOptions
        .key("s3.access.key")
        .stringType()
        .noDefaultValue()
        .withDescription("S3 access key");

    public static final ConfigOption<String> S3_SECRET_KEY = ConfigOptions
        .key("s3.secret.key")
        .stringType()
        .noDefaultValue()
        .withDescription("S3 secret key");

    public static final ConfigOption<String> S3_PATH_STYLE = ConfigOptions
        .key("s3.path.style.access")
        .stringType()
        .noDefaultValue()
        .withDescription("Enable S3 path style access");

    private final String nessieUri;
    private final String warehouse;
    private final String catalogName;
    private final String branch;
    private final Map<String, String> s3Config;

    /**
     * Creates an IcebergCatalogConfig from Configuration.
     *
     * @param config Flink Configuration
     */
    public IcebergCatalogConfig(Configuration config) {
        this.nessieUri = config.get(NESSIE_URI);
        this.warehouse = config.get(WAREHOUSE);
        this.catalogName = config.get(CATALOG_NAME);
        this.branch = config.get(NESSIE_BRANCH);

        this.s3Config = new HashMap<>();
        
        // S3 endpoint - check config, then env var
        String s3Endpoint = config.get(S3_ENDPOINT);
        if (s3Endpoint == null || s3Endpoint.isEmpty()) {
            s3Endpoint = System.getenv("S3_ENDPOINT");
        }
        if (s3Endpoint != null && !s3Endpoint.isEmpty()) {
            s3Config.put("s3.endpoint", s3Endpoint);
        }
        
        // S3 access key - check config, then env var
        String s3AccessKey = config.get(S3_ACCESS_KEY);
        if (s3AccessKey == null || s3AccessKey.isEmpty()) {
            s3AccessKey = System.getenv("AWS_ACCESS_KEY_ID");
        }
        if (s3AccessKey != null && !s3AccessKey.isEmpty()) {
            s3Config.put("s3.access-key-id", s3AccessKey);
        }
        
        // S3 secret key - check config, then env var
        String s3SecretKey = config.get(S3_SECRET_KEY);
        if (s3SecretKey == null || s3SecretKey.isEmpty()) {
            s3SecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        }
        if (s3SecretKey != null && !s3SecretKey.isEmpty()) {
            s3Config.put("s3.secret-access-key", s3SecretKey);
        }
        
        // S3 path style access - check config, then env var, default to true for MinIO/Quobjects
        String s3PathStyle = config.get(S3_PATH_STYLE);
        if (s3PathStyle == null || s3PathStyle.isEmpty()) {
            s3PathStyle = System.getenv("S3_PATH_STYLE_ACCESS");
        }
        if (s3PathStyle == null || s3PathStyle.isEmpty()) {
            s3PathStyle = "true";  // Default for self-hosted S3
        }
        s3Config.put("s3.path-style-access", s3PathStyle);
        
        // S3 region - needed by AWS SDK even for non-AWS endpoints
        String s3Region = System.getenv("AWS_REGION");
        if (s3Region != null && !s3Region.isEmpty()) {
            s3Config.put("s3.region", s3Region);
        }
    }

    /**
     * Creates an IcebergCatalogConfig with explicit values.
     */
    public IcebergCatalogConfig(String nessieUri, String warehouse, String catalogName, String branch) {
        this.nessieUri = nessieUri;
        this.warehouse = warehouse;
        this.catalogName = catalogName;
        this.branch = branch;
        this.s3Config = new HashMap<>();
    }

    /**
     * Returns the catalog properties as a Map for use with Iceberg APIs.
     *
     * @return Map of catalog configuration properties
     */
    public Map<String, String> getCatalogProperties() {
        Map<String, String> props = new HashMap<>();

        // Nessie catalog configuration
        props.put("type", "iceberg");
        props.put("catalog-type", "nessie");
        props.put("uri", nessieUri);
        props.put("ref", branch);
        props.put("warehouse", warehouse);

        // Nessie-specific settings
        props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

        // Add S3 configuration if present
        props.putAll(s3Config);

        return props;
    }

    /**
     * Registers the Iceberg catalog with Nessie in a Flink TableEnvironment.
     *
     * @param tableEnv The Flink TableEnvironment
     */
    public void registerCatalog(TableEnvironment tableEnv) {
        LOG.info("Registering Iceberg catalog '{}' with Nessie at {}", catalogName, nessieUri);

        StringBuilder createCatalogSql = new StringBuilder();
        createCatalogSql.append("CREATE CATALOG ").append(catalogName).append(" WITH (\n");
        createCatalogSql.append("  'type' = 'iceberg',\n");
        createCatalogSql.append("  'catalog-impl' = 'org.apache.iceberg.nessie.NessieCatalog',\n");
        createCatalogSql.append("  'uri' = '").append(nessieUri).append("',\n");
        createCatalogSql.append("  'ref' = '").append(branch).append("',\n");
        createCatalogSql.append("  'warehouse' = '").append(warehouse).append("',\n");
        createCatalogSql.append("  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'");

        // Add S3 configuration
        for (Map.Entry<String, String> entry : s3Config.entrySet()) {
            createCatalogSql.append(",\n  '").append(entry.getKey())
                .append("' = '").append(entry.getValue()).append("'");
        }

        createCatalogSql.append("\n)");

        tableEnv.executeSql(createCatalogSql.toString());
        tableEnv.useCatalog(catalogName);

        LOG.info("Iceberg catalog '{}' registered and set as current catalog", catalogName);
    }

    /**
     * Creates a database (namespace) if it doesn't exist.
     *
     * @param tableEnv The Flink TableEnvironment
     * @param database The database name to create
     */
    public void createDatabaseIfNotExists(TableEnvironment tableEnv, String database) {
        LOG.info("Creating database '{}' if not exists", database);
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + database);
        tableEnv.useDatabase(database);
    }

    /**
     * Adds S3 configuration property.
     *
     * @param key The S3 property key
     * @param value The S3 property value
     * @return this for chaining
     */
    public IcebergCatalogConfig withS3Config(String key, String value) {
        this.s3Config.put(key, value);
        return this;
    }

    /**
     * Gets the Flink Configuration for the catalog.
     *
     * @return Flink Configuration object
     */
    public Configuration getFlinkConfiguration() {
        Configuration config = new Configuration();
        Map<String, String> props = getCatalogProperties();
        for (Map.Entry<String, String> entry : props.entrySet()) {
            config.setString(entry.getKey(), entry.getValue());
        }
        return config;
    }

    // Getters
    public String getNessieUri() {
        return nessieUri;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getBranch() {
        return branch;
    }
}
