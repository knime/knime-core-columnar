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
package org.knime.core.columnar.arrow.onheap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.arrow.flatbuf.DictionaryBatch;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowBodyCompression;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.columnar.arrow.ArrowColumnDataFactoryVersion;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils;
import org.knime.core.columnar.arrow.compress.ArrowCompression;
import org.knime.core.columnar.arrow.compress.ArrowCompressionUtil;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.filter.ColumnSelection;

/**
 * An abstract {@link RandomAccessBatchReader} for Arrow. Using an {@link ArrowReader} object provided by
 * #createReader() which controls how the data is read.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
abstract class AbstractOnHeapArrowBatchReader {

    private final BufferAllocator m_allocator;

    private final OnHeapArrowColumnDataFactory[] m_factories;

    private final ColumnSelection m_columnSelection;

    // Initialized on first #readRetained
    protected ArrowReader m_reader;

    // Initialized on first #readRetained
    private Schema m_schema;

    // Initialized on first #readRetained
    private Map<String, String> m_metadata;

    // Initialized on first #readRetained
    private Map<Long, DictionaryDescription> m_dictionaryDescriptions;

    // Initialized on first #readRetained
    private ArrowColumnDataFactoryVersion[] m_factoryVersions;

    private boolean m_closed;

    AbstractOnHeapArrowBatchReader(final BufferAllocator allocator, final OnHeapArrowColumnDataFactory[] factories,
        final ColumnSelection columnSelection) {
        m_allocator = allocator;
        m_factories = factories;
        m_columnSelection = columnSelection;
        m_closed = false;
    }

    protected synchronized void initializeReader() throws IOException {
        // Check if another thread already initialized the reader
        if (m_reader == null) {
            m_reader = createReader();
            m_dictionaryDescriptions = new HashMap<>();
            m_schema = mapFileSchemaToMemoryFormat(m_reader.getSchema(), m_columnSelection, m_dictionaryDescriptions);
            m_metadata = getMetadata();
            m_factoryVersions = Arrays.stream( //
                m_metadata.get(ArrowReaderWriterUtils.ARROW_FACTORY_VERSIONS_KEY).split(",")) //
                .map(ArrowColumnDataFactoryVersion::version) //
                .toArray(ArrowColumnDataFactoryVersion[]::new);
        }
    }

    protected abstract ArrowReader createReader() throws IOException;

    protected Map<String, String> getMetadata() {
        return m_reader.getSchema().getCustomMetadata();
    }

    protected ReadBatch readRetained(final int index) throws IOException {
        // Initialize the reader when reading the first batch
        if (m_reader == null) {
            initializeReader();
        }

        // Read the data
        // TODO(AP-23857) create child allocator, use if for all allocations here, and close it after moving the data
        // to the on-heap objects
        final FieldVectorAndNullCount[] vectors = readVectors(index);
        final DictionaryProvider dictionaries = readDictionaries(index);

        // Create the ColumnData
        try {
            return m_columnSelection.createBatch(i -> {
                try {
                    return m_factories[i].createRead(vectors[i].vector, vectors[i].nullCount, dictionaries,
                        m_factoryVersions[i]);
                } catch (IOException e) {
                    throw new IllegalStateException("Exception while reading column data.", e);
                }
            });
        } finally {
            for (FieldVectorAndNullCount vector : vectors) {
                if (vector != null) {
                    vector.vector.close();
                }
            }
            for (long id : m_dictionaryDescriptions.keySet()) {
                dictionaries.lookup(id).getVector().close();
            }
        }
    }

    /** Read the vectors at the given batch index using the reader */
    private FieldVectorAndNullCount[] readVectors(final int index) throws IOException {
        try (final ArrowRecordBatch recordBatch = m_reader.readRecordBatch(index)) {
            // The data from the record batch
            final Iterator<ArrowFieldNode> nodes = recordBatch.getNodes().iterator();
            final Iterator<ArrowBuf> buffers = recordBatch.getBuffers().iterator();

            // Loop over the schema and load the data into new vectors
            final List<Field> fields = m_schema.getFields();
            final FieldVectorAndNullCount[] vectors = new FieldVectorAndNullCount[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                final Field field = fields.get(i);
                if (m_columnSelection.isSelected(i)) {
                    @SuppressWarnings("resource") // Resource handled by caller (the vector is returned)
                    final FieldVector vector = field.createVector(m_allocator); // NOSONAR: See SuppressWarnings
                    final ArrowVectorNullCount nullCount =
                        loadVector(vector, nodes, buffers, getCompressionCodec(recordBatch), m_allocator);
                    vectors[i] = new FieldVectorAndNullCount(vector, nullCount);
                } else {
                    skipVector(field, nodes, buffers);
                }
            }
            return vectors;
        }
    }

    private DictionaryProvider readDictionaries(final int index) throws IOException {
        // Read the dictionary batches
        final ArrowDictionaryBatch[] batches = m_reader.readDictionaryBatches(index);
        try {
            // The provider to hold the dictionaries
            final MapDictionaryProvider provider = new MapDictionaryProvider();

            // Loop over the batches, create the Dictionary objects and add them to the provider
            for (final ArrowDictionaryBatch batch : batches) {
                // Create the vector for this dictionary
                final long id = batch.getDictionaryId();
                final DictionaryDescription description = m_dictionaryDescriptions.get(id);
                if (description != null) {
                    @SuppressWarnings("resource") // Resource handled by caller (the vector is returned)
                    final FieldVector vector = description.field.createVector(m_allocator); // NOSONAR: See SuppressWarnings

                    // Load the data into the vector
                    @SuppressWarnings("resource") // Closed by the DictionaryBatch
                    final ArrowRecordBatch data = batch.getDictionary();
                    loadVector(vector, data.getNodes().iterator(), data.getBuffers().iterator(),
                        getCompressionCodec(data), m_allocator);

                    // Create the dictionary and add it to the provider
                    provider.put(new Dictionary(vector, description.encoding));
                }
            }
            return provider;
        } finally {
            for (final ArrowDictionaryBatch b : batches) {
                b.close();
            }
        }
    }

    public synchronized void close() throws IOException {
        if (m_reader != null && !m_closed) {
            m_reader.close();
            m_closed = true;
        }
    }

    /**
     * Map the given schema from the file format to the memory format (different type of dictionary encoded vectors).
     * The fields for the dictionaries are added to the map (filtered by the column selection).
     */
    private static Schema mapFileSchemaToMemoryFormat(final Schema schema, final ColumnSelection columnSelection,
        final Map<Long, DictionaryDescription> dictionaryDescriptions) {
        final List<Field> messageFields = schema.getFields();
        final List<Field> fields = new ArrayList<>();
        for (int i = 0; i < messageFields.size(); i++) {
            fields.add(
                mapFieldToMemoryFormat(messageFields.get(i), columnSelection.isSelected(i), dictionaryDescriptions));
        }
        return new Schema(fields, schema.getCustomMetadata());
    }

    /**
     * Map the given field from the file format to the memory format (different type of dictionary encoded vectors). The
     * fields for the dictionaries are added to the map.
     */
    private static Field mapFieldToMemoryFormat(final Field field, final boolean addToDictionaryDescriptions,
        final Map<Long, DictionaryDescription> dictionaryDescriptions) {

        // Get the memory type
        final DictionaryEncoding encoding = field.getDictionary();
        final ArrowType type;
        final List<Field> mappedChildren;
        if (encoding == null) {
            type = field.getType();
            mappedChildren = mapChildrenToMemoryFormat(field, addToDictionaryDescriptions, dictionaryDescriptions);
        } else {
            // Remember a field for the dictionary (to create the vectors of the correct type later)
            if (addToDictionaryDescriptions) {
                dictionaryDescriptions.put(encoding.getId(),
                    getDictionaryDescription(encoding, field, dictionaryDescriptions));
            }

            // The type of this field
            type = Optional.ofNullable(encoding.getIndexType()).orElseGet(() -> new ArrowType.Int(32, true));

            // Dictionary encoded vectors have no children (The children belong to the dictionary type)
            mappedChildren = null;
        }

        // Create the field
        final FieldType fieldType = new FieldType(field.isNullable(), type, encoding, field.getMetadata());
        return new Field(field.getName(), fieldType, mappedChildren);
    }

    /** Get a description of the dictionary with the encoding and the type of the field (in message format) */
    private static DictionaryDescription getDictionaryDescription(final DictionaryEncoding encoding,
        final Field messageField, final Map<Long, DictionaryDescription> dictionaryDescriptions) {
        final long id = encoding.getId();
        final FieldType fieldType = new FieldType(false, messageField.getType(), null, null);
        final List<Field> mappedChildren = mapChildrenToMemoryFormat(messageField, true, dictionaryDescriptions);
        final Field field = new Field("DICT" + id, fieldType, mappedChildren);
        return new DictionaryDescription(field, encoding);
    }

    /** Map the children of the field to the memory format and return them */
    private static List<Field> mapChildrenToMemoryFormat(final Field field, final boolean addToDictionaryDescriptions,
        final Map<Long, DictionaryDescription> dictionaryDescriptions) {
        final List<Field> children = field.getChildren();
        final List<Field> mappedChildren = new ArrayList<>(children.size());
        for (final Field child : children) {
            mappedChildren.add(mapFieldToMemoryFormat(child, addToDictionaryDescriptions, dictionaryDescriptions));
        }
        return mappedChildren;
    }

    /** Load the given vector from the next nodes and buffers */
    private static ArrowVectorNullCount loadVector(final FieldVector vector, final Iterator<ArrowFieldNode> nodes,
        final Iterator<ArrowBuf> buffers, final CompressionCodec compressionCodec, final BufferAllocator allocator) {
        final Field field = vector.getField();
        // Load and decompress the buffers of this vector
        final ArrowFieldNode fieldNode = nodes.next();
        final List<ArrowBuf> compressedBuffers = getFieldBuffers(field, buffers);
        final List<ArrowBuf> ownBuffers =
            ArrowReaderWriterUtils.decompressAllBuffers(compressedBuffers, compressionCodec, allocator);
        vector.loadFieldBuffers(fieldNode, ownBuffers);
        // TODO(benjamin) NB: this is a bug in Arrow. The NullVector implementation of #loadFieldBuffers should set the value count
        if (vector instanceof NullVector) {
            vector.setValueCount(fieldNode.getLength());
        }
        ownBuffers.forEach(ArrowBuf::close);

        // Load the buffers for the children
        final List<Field> children = field.getChildren();
        final ArrowVectorNullCount[] childrenNullCount = new ArrowVectorNullCount[children.size()];
        if (!children.isEmpty()) {
            final List<FieldVector> childrenVectors = vector.getChildrenFromFields();
            for (int i = 0; i < children.size(); i++) {
                @SuppressWarnings("resource") // Resource handled by the parent which is handled by the created ColumnData
                final FieldVector cv = childrenVectors.get(i);
                childrenNullCount[i] = loadVector(cv, nodes, buffers, compressionCodec, allocator);
            }
        }
        return new ArrowVectorNullCount(fieldNode.getNullCount(), childrenNullCount);
    }

    /** Skip the given vector from the next nodes and buffers */
    private static void skipVector(final Field field, final Iterator<ArrowFieldNode> nodes,
        final Iterator<ArrowBuf> buffers) {
        // Skip the buffers of this vector
        nodes.next();
        skipFieldBuffers(field, buffers);

        // Skip the buffers for the children
        final List<Field> children = field.getChildren();
        if (!children.isEmpty()) {
            for (int i = 0; i < children.size(); i++) {
                skipVector(children.get(i), nodes, buffers);
            }
        }
    }

    /** Get the buffers for the given field from the iterator */
    private static List<ArrowBuf> getFieldBuffers(final Field field, final Iterator<ArrowBuf> buffers) {
        final int bufferCount = TypeLayout.getTypeBufferCount(field.getType());
        final List<ArrowBuf> bs = new ArrayList<>(bufferCount);
        for (int i = 0; i < bufferCount; i++) {
            @SuppressWarnings("resource") // Closed with RecordBatch
            final ArrowBuf b = buffers.next();
            bs.add(b);
        }
        return bs;
    }

    /** Skip the buffers for the given field from the iterator */
    @SuppressWarnings("resource") // Buffers closed by caller
    private static void skipFieldBuffers(final Field field, final Iterator<ArrowBuf> buffers) {
        final int bufferCount = TypeLayout.getTypeBufferCount(field.getType());
        for (int i = 0; i < bufferCount; i++) {
            buffers.next();
        }
    }

    /** Get the CompressionCodec used by the given record batch */
    private static CompressionCodec getCompressionCodec(final ArrowRecordBatch recordBatch) {
        final ArrowBodyCompression bodyCompression = recordBatch.getBodyCompression();
        final ArrowCompression compressionConfig =
            ArrowCompressionUtil.getCompressionForType(bodyCompression.getCodec());
        return compressionConfig.getCompressionCodec();
    }

    /**
     * A simple Arrow reader. {@link org.apache.arrow.vector.ipc.ArrowFileReader} has the following problems:
     * <ul>
     * <li>VectorSchemaRoot holds vectors that are filled over and over again. Copying of data is required to get the
     * vectors.</li>
     * <li>Cannot filter which vectors are allocated</li>
     * <li>Cannot read specific dictionaries for a batch.</li>
     * </ul>
     */
    protected interface ArrowReader extends AutoCloseable {

        /**
         * @return the schema
         */
        Schema getSchema();

        /**
         * @param index the index
         * @return the record batch at the index
         * @throws IOException if reading failed
         */
        ArrowRecordBatch readRecordBatch(int index) throws IOException;

        /**
         * @param index the index
         * @return all dictionary batches associated with the batch at the given index
         * @throws IOException
         */
        ArrowDictionaryBatch[] readDictionaryBatches(int index) throws IOException;

        @Override
        void close() throws IOException;
    }

    /**
     * Structure holding a {@link Field} and {@link DictionaryEncoding}. Both needed to create a {@link Dictionary} from
     * a {@link DictionaryBatch}.
     */
    private record DictionaryDescription(Field field, DictionaryEncoding encoding) {
    }

    /** Structure holding a {@link FieldVector vector} and a {@link ArrowVectorNullCount} for this vector. */
    private record FieldVectorAndNullCount(FieldVector vector, ArrowVectorNullCount nullCount) {
    }
}
