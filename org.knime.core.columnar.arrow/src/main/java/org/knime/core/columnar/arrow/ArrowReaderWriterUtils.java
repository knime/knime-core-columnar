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
 *   Sep 24, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.columnar.arrow.mmap.MappableReadChannel;

/**
 * Utility class for Arrow.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowReaderWriterUtils {

    private ArrowReaderWriterUtils() {
        // Class holding constants
    }

    /** The Arrow magic number. These are the first and last bytes of each arrow file */
    public static final byte[] ARROW_MAGIC_BYTES = "ARROW1".getBytes(StandardCharsets.UTF_8);

    /** The length of the Arrow magic number */
    public static final int ARROW_MAGIC_LENGTH = ARROW_MAGIC_BYTES.length;

    /**
     * Key for the metadata element holding the chunk size
     *
     * @deprecated since 5.3
     * */
    @Deprecated
    public static final String ARROW_CHUNK_SIZE_KEY = "KNIME:basic:chunkSize";

    /**
     * Key for footer metadata containing the indices of batch lengths
     *
     * @since 5.3
     */
    public static final String ARROW_BATCH_BOUNDARIES_KEY = "KNIME:basic:batchBoundaries";

    /** Key for the metadata element holding the factory versions */
    public static final String ARROW_FACTORY_VERSIONS_KEY = "KNIME:basic:factoryVersions";

    /** Key for the metadata element holding the factory versions */
    public static final String ARROW_LZ4_BLOCK_FEATURE_KEY = "KNIME:basic:usingLz4Block";

    /**
     * Reads the Arrow {@link Schema} from an Arrow file.
     *
     * @param file containing Arrow data
     * @return the {@link Schema} stored in file
     */
    public static Schema readSchema(final File file) {
        try (var in = new MappableReadChannel(file, "r")) {
            checkFileSize(in);
            checkArrowMagic(in, false);
            // Magic number (6 bytes) + empty padding to 8 byte boundary
            in.setPosition(8);
            return MessageSerializer.deserializeSchema(in);
        } catch (IOException ex) {
            throw new IllegalArgumentException(String.format("Failed to read the schema from '%s'.", file), ex);
        }
    }

    /**
     * Check if the Arrow file has a valid length. Throws an exception if not.
     *
     * @param in the channel on the file
     * @throws IOException if the channel is too small
     */
    public static void checkFileSize(final SeekableReadChannel in) throws IOException {
        if (in.size() <= ARROW_MAGIC_LENGTH * 2 + 4) {
            throw new IOException("Arrow file invalid: File is too small: " + in.size());
        }
    }

    /**
     * Check if the Arrow has the arrow magic bytes at the beginning or the end. Throws an exception if not.
     *
     * @param in the channel on the file
     * @param end if true, checks if the magic number is at the end of the channel. If false, checks if the magic number
     *            is at the beginning of the channel
     * @throws IOException if the magic number is not at the expected location
     */
    public static void checkArrowMagic(final SeekableReadChannel in, final boolean end) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocate(ARROW_MAGIC_LENGTH);
        if (end) {
            in.setPosition(in.size() - buffer.remaining());
        } else {
            in.setPosition(0);
        }
        in.readFully(buffer);
        buffer.flip();
        if (!Arrays.equals(buffer.array(), ARROW_MAGIC_BYTES)) {
            throw new IOException("Arrow file invalid: Magic number missing.");
        }
    }

    /**
     * Compress the given buffers using the given compression coded. The input buffers are not closed. Also the output
     * buffers need to be closed by the caller.
     *
     * @param uncompressedBuffers a list of the buffers to compress
     * @param compressionCodec the compression to use
     * @param allocator an allocator to allocate the compressed buffers
     * @return a list of compressed buffers
     */
    public static List<ArrowBuf> compressAllBuffers(final List<ArrowBuf> uncompressedBuffers,
        final CompressionCodec compressionCodec, final BufferAllocator allocator) {
        final List<ArrowBuf> compressedBuffers = new ArrayList<>(uncompressedBuffers.size());
        for (final ArrowBuf uncompressedBuf : uncompressedBuffers) {
            uncompressedBuf.getReferenceManager().retain();
            if (uncompressedBuf.writerIndex() > 0) {
                @SuppressWarnings("resource") // Released by the caller
                final ArrowBuf compressedBuf = compressionCodec.compress(allocator, uncompressedBuf);
                compressedBuffers.add(compressedBuf);
            } else {
                compressedBuffers.add(uncompressedBuf);
            }
        }
        return compressedBuffers;
    }

    /**
     * Decompress the given buffers using the given compression coded. The input buffers are not closed. Also the output
     * buffers need to be closed by the caller.
     *
     * @param compressedBuffers a list of the buffers to decompress
     * @param compressionCodec the compression to use
     * @param allocator an allocator to allocate the decompressed buffers
     * @return a list of decompressed buffers
     */
    public static List<ArrowBuf> decompressAllBuffers(final List<ArrowBuf> compressedBuffers,
        final CompressionCodec compressionCodec, final BufferAllocator allocator) {
        final List<ArrowBuf> decompressedBuffers = new ArrayList<>(compressedBuffers.size());
        for (final ArrowBuf compressedBuf : compressedBuffers) {
            compressedBuf.getReferenceManager().retain();
            if (compressedBuf.writerIndex() > 0) {
                @SuppressWarnings("resource") // Released by the caller
                final ArrowBuf decompressedBuf = compressionCodec.decompress(allocator, compressedBuf);
                decompressedBuffers.add(decompressedBuf);
            } else {
                decompressedBuffers.add(compressedBuf);
            }
        }
        return decompressedBuffers;
    }

    public static String longArrayToString(final long[] longs) {
        StringBuilder sb = new StringBuilder();
        var first = true;
        for (var l : longs) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append(l);
        }
        return sb.toString();
    }

    public static long[] stringToLongArray(final String serializedLongs) {
        if (serializedLongs == null) {
            return new long[0];
        }
        return Arrays.stream(serializedLongs.split(",")).mapToLong(Long::valueOf).toArray();
    }

    /** A dictionary provider only holding one single dictionary */
    public static final class SingletonDictionaryProvider implements DictionaryProvider {

        private long m_id;

        private final Dictionary m_dictionary;

        /**
         * Create a dictionary provider only holding the given dictionary.
         *
         * @param dictionary the dictionary
         */
        public SingletonDictionaryProvider(final Dictionary dictionary) {
            m_dictionary = dictionary;
            m_id = dictionary.getEncoding().getId();
        }

        @Override
        public Dictionary lookup(final long id) {
            if (id == m_id) {
                return m_dictionary;
            } else {
                return null;
            }
        }

        @Override
        public Set<Long> getDictionaryIds() {
            return Collections.singleton(m_id);
        }
    }

    /**
     * A dictionary provider holding a list of dictionary providers. On {@link #lookup(long)} the lookup is performed on
     * the children dictionary providers until the id is found.
     */
    public static final class NestedDictionaryProvider implements DictionaryProvider {

        private final List<DictionaryProvider> m_providers;

        /**
         * Create a dictionary provider with the given children
         *
         * @param providers the providers to lookup the ids
         */
        public NestedDictionaryProvider(final List<DictionaryProvider> providers) {
            m_providers = providers;
        }

        @Override
        public Dictionary lookup(final long id) {
            for (int i = 0; i < m_providers.size(); i++) {
                final Dictionary dictionary = m_providers.get(i).lookup(id);
                if (dictionary != null) {
                    return dictionary;
                }
            }
            return null;
        }

        @Override
        public Set<Long> getDictionaryIds() {
            return m_providers.stream() //
                .flatMap(p -> p.getDictionaryIds().stream()) //
                .collect(Collectors.toSet());
        }
    }

    /** A provider of byte offsets of record batches and dictionary batches in an Arrow file. */
    public interface OffsetProvider {

        /**
         * @param index the index of the record batch
         * @return the offset of the record batch with the given index
         * @throws IndexOutOfBoundsException if there is no record batch with the given index in the file (yet)
         */
        long getRecordBatchOffset(int index);

        /**
         * @param index the index of the dictionary batches
         * @return the offsets of all dictionary batches for the given index
         * @throws IndexOutOfBoundsException if there are no dictionaries for the given index in the file (yet)
         */
        long[] getDictionaryBatchOffsets(int index);
    }
}
