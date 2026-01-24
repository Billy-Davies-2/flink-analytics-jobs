package com.homelab.flink;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests to verify AWS SDK dependencies are correctly configured.
 * These tests ensure the required AWS SDK classes are available on the classpath,
 * particularly for S3 encryption support which requires the KMS module.
 */
class AwsSdkDependencyTest {

    @Test
    void kmsClientClassShouldBeAvailable() {
        // The KMS client is required for S3 SSE-KMS encryption
        assertThatCode(() -> {
            Class<?> kmsClientClass = Class.forName("software.amazon.awssdk.services.kms.KmsClient");
            assertThat(kmsClientClass).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    void encryptionAlgorithmSpecShouldBeAvailable() {
        // This was the missing class causing runtime errors with S3 encryption
        assertThatCode(() -> {
            Class<?> encryptionAlgoClass = Class.forName("software.amazon.awssdk.services.kms.model.EncryptionAlgorithmSpec");
            assertThat(encryptionAlgoClass).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    void kmsEncryptRequestShouldBeAvailable() {
        // Verify the KMS encrypt/decrypt request builders are available
        assertThatCode(() -> {
            Class<?> encryptRequestClass = Class.forName("software.amazon.awssdk.services.kms.model.EncryptRequest");
            assertThat(encryptRequestClass).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    void kmsDecryptRequestShouldBeAvailable() {
        assertThatCode(() -> {
            Class<?> decryptRequestClass = Class.forName("software.amazon.awssdk.services.kms.model.DecryptRequest");
            assertThat(decryptRequestClass).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    void s3ClientClassShouldBeAvailable() {
        // S3 client should also be available
        assertThatCode(() -> {
            Class<?> s3ClientClass = Class.forName("software.amazon.awssdk.services.s3.S3Client");
            assertThat(s3ClientClass).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    void stsClientClassShouldBeAvailable() {
        // STS client for assume role operations
        assertThatCode(() -> {
            Class<?> stsClientClass = Class.forName("software.amazon.awssdk.services.sts.StsClient");
            assertThat(stsClientClass).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    void kmsModelClassesShouldBeComplete() {
        // Test various KMS model classes needed for encryption operations
        String[] requiredClasses = {
            "software.amazon.awssdk.services.kms.model.EncryptionAlgorithmSpec",
            "software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest",
            "software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse",
            "software.amazon.awssdk.services.kms.model.DataKeySpec",
            "software.amazon.awssdk.services.kms.model.EncryptRequest",
            "software.amazon.awssdk.services.kms.model.EncryptResponse",
            "software.amazon.awssdk.services.kms.model.DecryptRequest",
            "software.amazon.awssdk.services.kms.model.DecryptResponse"
        };

        for (String className : requiredClasses) {
            assertThatCode(() -> {
                Class<?> clazz = Class.forName(className);
                assertThat(clazz).as("Class %s should be available", className).isNotNull();
            }).as("Failed to load class: %s", className).doesNotThrowAnyException();
        }
    }
}
