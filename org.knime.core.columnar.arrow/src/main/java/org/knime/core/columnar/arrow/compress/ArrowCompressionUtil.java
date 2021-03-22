/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Jan 18, 2021 (benjamin): created
 */
package org.knime.core.columnar.arrow.compress;

import org.apache.arrow.flatbuf.BodyCompressionMethod;
import org.apache.arrow.flatbuf.CompressionType;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowBodyCompression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for Arrow Buffer compression. Contains constants for each supported {@link ArrowCompression}
 * implementation.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowCompressionUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArrowCompressionUtil.class);

    private static final String PROPERTY_COMPRESSION = "knime.compress.arrow";

    private static final String PROPERTY_COMPRESSION_NONE = "NONE";

    private static final String PROPERTY_COMPRESSION_LZ4 = "LZ4";

    /** Config for LZ4 buffer compression using the lz4-java library */
    public static final ArrowLz4Compression ARROW_LZ4_COMPRESSION = new ArrowLz4Compression();

    /** Config for not using any compression */
    public static final ArrowNoCompression ARROW_NO_COMPRESSION = new ArrowNoCompression();

    private static ArrowCompression defaultCompression = null;

    private ArrowCompressionUtil() {
        // Utility class
    }

    /**
     * @return get the default compression which can be changed using the system property 'knime.compress.arrow'.
     */
    public static ArrowCompression getDefaultCompression() {
        if (defaultCompression == null) {
            final String compressionName = System.getProperty(PROPERTY_COMPRESSION);
            if (compressionName == null || PROPERTY_COMPRESSION_NONE.equals(compressionName)) {
                defaultCompression = ARROW_NO_COMPRESSION;
            } else if (PROPERTY_COMPRESSION_LZ4.equals(compressionName)) {
                defaultCompression = ARROW_LZ4_COMPRESSION;
            } else {
                LOGGER.error(
                    "Invalid Arrow compression format '{}'. Valid options are ({}|{}). Using the default '{}'.",
                    compressionName, PROPERTY_COMPRESSION_LZ4, PROPERTY_COMPRESSION_NONE, PROPERTY_COMPRESSION_NONE);
                defaultCompression = ARROW_NO_COMPRESSION;
            }
        }
        return defaultCompression;
    }

    /**
     * Get the {@link ArrowCompression} to use for the given type. The type refers to the type defined in
     * {@link CompressionType}.
     *
     * @param type the type of the compression as in {@link CompressionType}
     * @return the {@link ArrowCompression} to use
     */
    public static ArrowCompression getCompressionForType(final byte type) {
        if (type == ARROW_LZ4_COMPRESSION.getCompressionType()) {
            return ARROW_LZ4_COMPRESSION;
        } else if (type == ARROW_NO_COMPRESSION.getCompressionType()) {
            return ARROW_NO_COMPRESSION;
        } else {
            throw new IllegalArgumentException("Compression type not supported: " + type);
        }
    }

    private static final class ArrowLz4Compression implements ArrowCompression {

        private ArrowLz4Compression() {
        }

        @Override
        public CompressionCodec getCompressionCodec() {
            return new Lz4CompressionCodec();
        }

        @Override
        public ArrowBodyCompression getBodyCompression() {
            return new ArrowBodyCompression(getCompressionType(), BodyCompressionMethod.BUFFER);
        }

        @Override
        public byte getCompressionType() {
            return CompressionType.LZ4_FRAME;
        }
    }

    private static final class ArrowNoCompression implements ArrowCompression {

        private ArrowNoCompression() {
        }

        @Override
        public CompressionCodec getCompressionCodec() {
            return NoCompressionCodec.INSTANCE;
        }

        @Override
        public ArrowBodyCompression getBodyCompression() {
            return NoCompressionCodec.DEFAULT_BODY_COMPRESSION;
        }

        @Override
        public byte getCompressionType() {
            return NoCompressionCodec.COMPRESSION_TYPE;
        }
    }
}
