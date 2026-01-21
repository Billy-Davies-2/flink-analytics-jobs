package com.homelab.flink.common;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration utility for setting up Apache Iceberg catalog with Nessie.
 * 
 * This class provides methods to configure the Flink Table API to work with
 * Iceberg tables stored in an S3-compatible object store, using Nessie as
 * the catalog backend for table versioning and management.
 */
public class IcebergCatalogConfig {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogConfig.class);

    // Default configuration values
    public static final String DEFAULT_NESSIE_URI = "http://nessie.analytics.svc.cluster.local:19120/api/v1";
    public static final String DEFAULT_WAREHOUSE = "s3a://iceberg-warehouse";
    public static final String DEFAULT_CATALOG_NAME = "nessie";
    public static final String DEFAULT_BRANCH = "main";

    // Parameter keys
    public static final String PARAM_NESSIE_URI = "nessie.uri";
    public static final String PARAM_WAREHOUSE = "iceberg.warehouse";
    public static final String PARAM_CATALOG_NAME = "iceberg.catalog.name";
    public static final String PARAM_NESSIE_BRANCH = "nessie.branch";
    public static final String PARAM_S3_ENDPOINT = "s3.endpoint";
    public static final String PARAM_S3_ACCESS_KEY = "s3.access.key";
    public static final String PARAM_S3_SECRET_KEY = "s3.secret.key";
    public static final String PARAM_S3_PATH_STYLE = "s3.path.style.access";

    private final String nessieUri;
    private final String warehouse;
    private final String catalogName;
    private final String branch;
    private final Map<String, String> s3Config;

    /**
     * Creates an IcebergCatalogConfig from ParameterTool arguments.
     *
     * @param params CLI parameters
     */
    public IcebergCatalogConfig(ParameterTool params) {
        this.nessieUri = params.get(PARAM_NESSIE_URI, DEFAULT_NESSIE_URI);
        this.warehouse = params.get(PARAM_WAREHOUSE, DEFAULT_WAREHOUSE);
        this.catalogName = params.get(PARAM_CATALOG_NAME, DEFAULT_CATALOG_NAME);
        this.branch = params.get(PARAM_NESSIE_BRANCH, DEFAULT_BRANCH);

        this.s3Config = new HashMap<>();
        if (params.has(PARAM_S3_ENDPOINT)) {
            s3Config.put("s3.endpoint", params.get(PARAM_S3_ENDPOINT));
        }
        if (params.has(PARAM_S3_ACCESS_KEY)) {
            s3Config.put("s3.access-key-id", params.get(PARAM_S3_ACCESS_KEY));
        }
        if (params.has(PARAM_S3_SECRET_KEY)) {
            s3Config.put("s3.secret-access-key", params.get(PARAM_S3_SECRET_KEY));
        }
        if (params.has(PARAM_S3_PATH_STYLE)) {
            s3Config.put("s3.path-style-access", params.get(PARAM_S3_PATH_STYLE));
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
        props.put("nessie.client-builder-impl", 
            "org.projectnessie.client.http.HttpClientBuilder");

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
