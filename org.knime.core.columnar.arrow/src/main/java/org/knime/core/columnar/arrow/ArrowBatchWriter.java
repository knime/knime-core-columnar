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
 */
package org.knime.core.columnar.arrow;

import static org.knime.core.columnar.arrow.ArrowReaderWriterUtils.ARROW_CHUNK_SIZE_KEY;
import static org.knime.core.columnar.arrow.ArrowReaderWriterUtils.ARROW_FACTORY_VERSIONS_KEY;
import static org.knime.core.columnar.arrow.ArrowReaderWriterUtils.ARROW_MAGIC_BYTES;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowBodyCompression;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowFooter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.io.FileUtils;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils.OffsetProvider;
import org.knime.core.columnar.arrow.compress.ArrowCompression;
import org.knime.core.columnar.batch.BatchWriter;
import org.knime.core.columnar.batch.DefaultWriteBatch;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.WriteBatch;
import org.knime.core.columnar.data.NullableReadData;
import org.knime.core.columnar.data.NullableWriteData;
import org.knime.core.columnar.store.FileHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ArrowColumnDataWriter writes batches of columns to an Arrow file.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
class ArrowBatchWriter implements BatchWriter {

	private static final Logger LOGGER = LoggerFactory.getLogger(ArrowBatchWriter.class);

    private final FileHandle m_fileHandle;

    /** Factories used to get the vectors and dicts from the columns */
    private final ArrowColumnDataFactory[] m_factories;

    private final ArrowCompression m_compression;

    private final BufferAllocator m_allocator;

    private boolean m_firstWrite;

    private boolean m_closed;

    final AtomicInteger m_chunkSize = new AtomicInteger(-1);

    final AtomicInteger m_numBatches = new AtomicInteger(0);

    // Initialized on first #write
    private ArrowWriter m_writer;

    /**
     * Create an ArrowColumnDataWriter.
     *
     * @param file the file to write to
     * @param chunkSize the max size of the individual chunks
     * @param factories factories to get the vectors and dictionaries from the data. Must be able to handle the data at
     *            their index.
     */
    ArrowBatchWriter(final FileHandle file, final ArrowColumnDataFactory[] factories, final ArrowCompression compression,
        final BufferAllocator allocator) {
        m_fileHandle = file;
        m_factories = factories;
        m_compression = compression;
        m_allocator = allocator;
        m_firstWrite = true;
        m_closed = false;
    }

    @Override
    public WriteBatch create(final int capacity) {
        final NullableWriteData[] chunk = new NullableWriteData[m_factories.length];
        for (int i = 0; i < m_factories.length; i++) {
            chunk[i] = ArrowColumnDataFactory.createWrite(m_factories[i], String.valueOf(i), m_allocator, capacity);
        }
        return new DefaultWriteBatch(chunk);
    }

    @Override
    public synchronized void write(final ReadBatch batch) throws IOException {
        final List<Field> fields = new ArrayList<>(m_factories.length);
        final List<FieldVector> vectors = new ArrayList<>(m_factories.length);
        final List<FieldVector> allDictionaries = new ArrayList<>();

        // Loop and collect fields, vectors, dictionaries
        for (int i = 0; i < m_factories.length; i++) {
            final NullableReadData data = batch.get(i);
            final ArrowColumnDataFactory factory = m_factories[i];
            @SuppressWarnings("resource") // Vector resource is handled by the ColumnData
            final FieldVector vector = factory.getVector(data);
            final DictionaryProvider dictionaries = factory.getDictionaries(data);
            final Field field = vector.getField();

            if (m_firstWrite) {
                // Get the field for the schema and collect dictionaries
                fields.add(mapDictionariesAndField(field, dictionaries, allDictionaries));
            } else {
                // Collect the dictionaries
                mapDictionaries(field, dictionaries, allDictionaries);
            }

            // Collect the vector
            vectors.add(vector);
        }

        // If this is the first call we need to create the writer and write the schema to the file
        if (m_firstWrite) {
            m_chunkSize.set(batch.length());
            m_firstWrite = false;
            final Schema schema = new Schema(fields, getMetadata());

            m_writer = new ArrowWriter(m_fileHandle.asFile(), schema);
        }

        // Write the dictionaries
        writeDictionaries(m_writer, allDictionaries, m_compression, m_allocator);

        // Write the vectors
        writeVectors(m_writer, vectors, batch.length(), m_compression, m_allocator);

        m_numBatches.incrementAndGet();
    }

    /**
     * @return an offset provider that can return the offset of each record batch and dictionary batch once it is
     *         written to the file
     */
    OffsetProvider getOffsetProvider() {
        return new OffsetProvider() {

            @Override
            public long getRecordBatchOffset(final int index) {
                if (numBatches() <= index) {
                    throw new IndexOutOfBoundsException("Record batch with index " + index + " not yet written.");
                }
                return m_writer.m_recordBlocks.get(index).getOffset();
            }

            @Override
            public long[] getDictionaryBatchOffsets(final int index) {
                if (numBatches() <= index) {
                    throw new IndexOutOfBoundsException("Dictionary batch with index " + index + " not yet written.");
                }
                return Arrays.stream(m_writer.m_dictionaryBlocks.get(index)) //
                    .mapToLong(ArrowBlock::getOffset) //
                    .toArray();
            }
        };
    }

    /**
     * @return the number of batches already written
     */
    int numBatches() {
        return m_numBatches.get();
    }

    /**
     * @return the length of the batches (recorded by observing the length of the first batch written)
     */
    int batchLength() {
        final int bl = m_chunkSize.get();
        if (bl == -1) {
            throw new IllegalStateException("The batch length is not yet know. No batch has been written.");
        }
        return bl;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!m_closed) {
            if (!m_firstWrite) {
                m_writer.writeFooter();
                m_writer.close();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Closing file {} ({})", m_fileHandle.asFile().getAbsolutePath(),
                        FileUtils.byteCountToDisplaySize(FileUtils.sizeOf(m_fileHandle.asFile())));
                }
            }
            m_closed = true;
        }
    }

    /** Create and return the metadata for this writer */
    private Map<String, String> getMetadata() {
        final Map<String, String> metadata = new HashMap<>();

        // Max chunk size
        metadata.put(ARROW_CHUNK_SIZE_KEY, Integer.toString(m_chunkSize.get()));

        // Factory versions
        final String factoryVersions = Arrays.stream(m_factories) //
            .map(f -> f.getVersion().toString()) //
            .collect(Collectors.joining(","));
        metadata.put(ARROW_FACTORY_VERSIONS_KEY, factoryVersions);

        return metadata;
    }

    /** Map the dictionary ids in the given field to new unique ids and convert the type to the message format type */
    private static Field mapDictionariesAndField(final Field field, final DictionaryProvider dictionaries,
        final List<FieldVector> allDictionaries) {
        final DictionaryEncoding encoding = field.getDictionary();
        final DictionaryEncoding mappedEncoding;
        final ArrowType mappedType;
        final List<Field> children;
        if (encoding == null) {
            // No dictionary encoding: Nothing to do
            mappedEncoding = null;
            mappedType = field.getType();
            children = field.getChildren();
        } else {
            // Map the id of this dictionary encoding
            final long id = encoding.getId();
            final long mappedId = allDictionaries.size();
            final Dictionary dictionary = dictionaries.lookup(id);
            @SuppressWarnings("resource") // Vector resource is handled by the ColumnData
            final FieldVector vector = dictionary.getVector();
            allDictionaries.add(vector);

            // Create a mapped DictionaryEncoding with the new id
            mappedEncoding = new DictionaryEncoding(mappedId, encoding.isOrdered(), encoding.getIndexType());
            mappedType = dictionary.getVectorType();
            // The children of this field are the children of the dictionary field
            children = vector.getField().getChildren();
        }

        // Call recursively for the children
        final List<Field> mappedChildren = new ArrayList<>(field.getChildren().size());
        for (final Field child : children) {
            mappedChildren.add(mapDictionariesAndField(child, dictionaries, allDictionaries));
        }

        // Create the Field
        final FieldType fieldType = new FieldType(field.isNullable(), mappedType, mappedEncoding, field.getMetadata());
        return new Field(field.getName(), fieldType, mappedChildren);
    }

    /** Get the dictionaries used in the fields and add them to allDictionaries in the correct order */
    private static void mapDictionaries(final Field field, final DictionaryProvider dictionaries,
        final List<FieldVector> allDictionaries) {
        final DictionaryEncoding encoding = field.getDictionary();
        final List<Field> children;
        if (encoding == null) {
            children = field.getChildren();
        } else {
            // Map the id of this dictionary encoding
            final long id = encoding.getId();
            @SuppressWarnings("resource") // Vector resource is handled by the ColumnData
            final FieldVector vector = dictionaries.lookup(id).getVector();
            allDictionaries.add(vector);
            // The children of this field are the children of the dictionary field
            children = vector.getField().getChildren();
        }
        // Call recursively for the children
        for (final Field child : children) {
            mapDictionaries(child, dictionaries, allDictionaries);
        }
    }

    private static void writeDictionaries(final ArrowWriter writer, final List<FieldVector> dictionaries,
        final ArrowCompression compression, final BufferAllocator allocator) throws IOException {
        final ArrowDictionaryBatch[] batches = new ArrowDictionaryBatch[dictionaries.size()];
        try { // NOSONAR: Arrays are not AutoCloseable (and creating a custom collection would be overkill)

            // Collect the batches
            for (int id = 0; id < dictionaries.size(); id++) {
                @SuppressWarnings("resource") // Vector resource is handled by the ColumnData
                final FieldVector vector = dictionaries.get(id);
                @SuppressWarnings("resource") // Record batch closed with the dictionary batch
                final ArrowRecordBatch data = createRecordBatch(Collections.singletonList(vector),
                    vector.getValueCount(), compression, allocator);
                batches[id] = new ArrowDictionaryBatch(id, data, false);
            }

            // Write the batches to the file
            writer.writeDictionaryBatches(batches);

        } finally {
            for (final ArrowDictionaryBatch b : batches) {
                if (b != null) {
                    b.close();
                }
            }
        }
    }

    /** Write the vectors to the writer */
    private static void writeVectors(final ArrowWriter writer, final List<FieldVector> vectors, final int length,
        final ArrowCompression compression, final BufferAllocator allocator) throws IOException {
        try (final ArrowRecordBatch recordBatch = createRecordBatch(vectors, length, compression, allocator)) {
            writer.writeRecordBatch(recordBatch);
        }
    }

    /** Create a record batch to load the given vectors */
    private static ArrowRecordBatch createRecordBatch(final List<FieldVector> vectors, final int length,
        final ArrowCompression compression, final BufferAllocator allocator) {
        // Extract field nodes and buffers
        final List<ArrowFieldNode> nodes = new ArrayList<>();
        final List<ArrowBuf> buffers = new ArrayList<>();
        for (final FieldVector v : vectors) {
            appendFieldNodes(v, nodes, buffers);
        }

        // Compress
        final ArrowBodyCompression bodyCompression = compression.getBodyCompression();
        final List<ArrowBuf> compressedBuffers =
            ArrowReaderWriterUtils.compressAllBuffers(buffers, compression.getCompressionCodec(), allocator);

        // Create the record batch
        final ArrowRecordBatch recordBatch = new ArrowRecordBatch(length, nodes, compressedBuffers, bodyCompression);
        compressedBuffers.forEach(ArrowBuf::close);
        return recordBatch;
    }

    /** Append the nodes and buffers for the given vector. Recursive for child vectors */
    private static void appendFieldNodes(final FieldVector vector, final List<ArrowFieldNode> nodes,
        final List<ArrowBuf> buffers) {
        nodes.add(new ArrowFieldNode(vector.getValueCount(), vector.getNullCount()));
        buffers.addAll(getFieldBuffers(vector));
        for (final FieldVector child : vector.getChildrenFromFields()) {
            appendFieldNodes(child, nodes, buffers);
        }
    }

    /** Get the field buffers for the given vector and check if the number of buffers is expected */
    private static List<ArrowBuf> getFieldBuffers(final FieldVector vector) {
        final List<ArrowBuf> buffers = vector.getFieldBuffers();
        // Check if the number of buffers is expected
        if (buffers.size() != TypeLayout.getTypeBufferCount(vector.getField().getType())) {
            throw new IllegalStateException("Wrong number of buffers for field " + vector.getField() + " in vector "
                + vector.getClass().getSimpleName() + ". Found: " + buffers);
        }
        return buffers;
    }

    /**
     * An Arrow writer. {@link ArrowFileWriter} has the following problems:
     * <ul>
     * <li>#writeDictionaryBatch(ArrowDictionaryBatch) is not exposed but we need to write dictionaries before each
     * batch.</li>
     * <li>Fields for dictionary encoded data get converted in the constructor using a DictionaryProvider. We don't need
     * the DictionaryProvider holding all Dictionaries and it would be inconvenient to create it. We can convert the
     * Field ourself because we need to recreate them to fix the dictionary ids.</li>
     * <li>A VectorSchemaRoot does not provide useful functionality for us but makes stuff more complicated.</li>
     * </ul>
     */
    private static final class ArrowWriter implements AutoCloseable {

        private final WriteChannel m_out;

        private final Schema m_schema;

        private final IpcOption m_option;

        private final List<ArrowBlock[]> m_dictionaryBlocks;

        private final List<ArrowBlock> m_recordBlocks;

        private ArrowWriter(final File file, final Schema schema) throws IOException {
            Files.deleteIfExists(file.toPath());
            @SuppressWarnings("resource") // Channel is closed by close of m_out. The channel closes the file
            final FileChannel channel = new RandomAccessFile(file, "rw").getChannel(); // NOSONAR: See comment above
            m_out = new WriteChannel(channel);
            m_schema = schema;
            m_option = new IpcOption();
            m_dictionaryBlocks = new ArrayList<>();
            m_recordBlocks = new ArrayList<>();

            // Write the magic number
            m_out.write(ARROW_MAGIC_BYTES);
            m_out.align();

            // Write the schema
            MessageSerializer.serialize(m_out, m_schema, m_option);
        }

        /** Write the given dictionary batch */
        private void writeDictionaryBatches(final ArrowDictionaryBatch[] batches) throws IOException {
            final ArrowBlock[] blocks = new ArrowBlock[batches.length];
            for (int i = 0; i < batches.length; i++) {
                blocks[i] = MessageSerializer.serialize(m_out, batches[i], m_option);
            }
            m_dictionaryBlocks.add(blocks);
        }

        /** Write the given data batch */
        private void writeRecordBatch(final ArrowRecordBatch batch) throws IOException {
            final ArrowBlock block = MessageSerializer.serialize(m_out, batch, m_option);
            m_recordBlocks.add(block);
        }

        /** Write the arrow file footer. Call before close to create a valid arrow file */
        private void writeFooter() throws IOException {
            // Write EOS
            m_out.writeIntLittleEndian(MessageSerializer.IPC_CONTINUATION_TOKEN);
            m_out.writeIntLittleEndian(0);

            // Write the footer
            final List<ArrowBlock> dictBlocks =
                m_dictionaryBlocks.stream().flatMap(Arrays::stream).collect(Collectors.toList());
            final ArrowFooter footer =
                new ArrowFooter(m_schema, dictBlocks, m_recordBlocks, Collections.emptyMap(), m_option.metadataVersion);
            final long footerStart = m_out.getCurrentPosition();
            m_out.write(footer, false);

            // Write the footer length
            m_out.writeIntLittleEndian((int)(m_out.getCurrentPosition() - footerStart));

            // Write the magic number
            m_out.write(ARROW_MAGIC_BYTES);
        }

        @Override
        public void close() throws IOException {
            m_out.close();
        }
    }
}
