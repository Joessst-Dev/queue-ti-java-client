package de.joesst.dev.queueti.spring;

import de.joesst.dev.queueti.TlsOptions;

import java.io.IOException;
import java.io.InputStream;

/**
 * Converts {@link QueueTiProperties.TlsProperties} Spring {@code Resource} references into a
 * {@link TlsOptions} instance by reading the underlying PEM bytes.
 */
final class QueueTiTlsResourceLoader {

    private QueueTiTlsResourceLoader() {}

    /**
     * Reads each non-null {@link org.springframework.core.io.Resource} field from
     * {@code props} and builds a {@link TlsOptions}.
     *
     * @param props the TLS properties; must not be {@code null}
     * @return a fully populated {@code TlsOptions}
     * @throws IOException if any resource cannot be read
     */
    static TlsOptions toTlsOptions(final QueueTiProperties.TlsProperties props) throws IOException {
        final var builder = TlsOptions.builder();

        if (props.getRootCertificates() != null) {
            try (InputStream in = props.getRootCertificates().getInputStream()) {
                builder.rootCertificates(in.readAllBytes());
            }
        }
        if (props.getPrivateKey() != null) {
            try (InputStream in = props.getPrivateKey().getInputStream()) {
                builder.privateKey(in.readAllBytes());
            }
        }
        if (props.getCertificateChain() != null) {
            try (InputStream in = props.getCertificateChain().getInputStream()) {
                builder.certificateChain(in.readAllBytes());
            }
        }
        if (props.getServerNameOverride() != null) {
            builder.serverNameOverride(props.getServerNameOverride());
        }

        return builder.build();
    }
}
