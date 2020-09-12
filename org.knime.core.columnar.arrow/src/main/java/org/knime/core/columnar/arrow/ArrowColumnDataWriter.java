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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.arrow.data.ArrowData;
import org.knime.core.columnar.arrow.data.ArrowDictionaryHolder;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.data.NullableWithCauseData;

class ArrowColumnDataWriter implements ColumnDataWriter {

    private final File m_file;

    private final FieldVectorWriter m_writer;

    private final BufferAllocator m_allocator;

    private final int m_chunkSize;

    public ArrowColumnDataWriter(final File file, final BufferAllocator allocator, final int chunkSize) {
        m_file = file;
        m_allocator = allocator;
        m_writer = new FieldVectorWriter(m_file);
        m_chunkSize = chunkSize;
    }

    @Override
    public void close() {
        m_writer.close();
    }

    @Override
    public void write(final ColumnData[] data) throws IOException {
        final List<FieldVector> vectors = new ArrayList<FieldVector>(data.length);
        final ArrowDictProvider dicts = new ArrowDictProvider();
        for (int i = 0; i < data.length; i++) {

            final ArrowData<?> arrowData = (ArrowData<?>)data[i];
            vectors.add(arrowData.get());
            if (arrowData instanceof ArrowDictionaryHolder) {
                dicts.add(new Dictionary(((ArrowDictionaryHolder<?>)arrowData).getDictionary(),
                    arrowData.get().getField().getFieldType().getDictionary()));
            } else if (arrowData instanceof NullableWithCauseData) {
                /*
                 * TODO recursive parsing for dictionaries for all nested types (especially if
                 * we have struct types different to BinarySupplData later).
                 *
                 * This implementation doesn't work for level-2 nested dictionaries.
                 */
                final ArrowData<?> chunk = (ArrowData<?>)((NullableWithCauseData<?>)arrowData).getColumnData();
                if (chunk instanceof ArrowDictionaryHolder) {
                    dicts.add(new Dictionary(((ArrowDictionaryHolder<?>)chunk).getDictionary(),
                        chunk.get().getField().getFieldType().getDictionary()));
                }
            }
        }

        m_writer.write(vectors, dicts);
    }

    class FieldVectorWriter implements AutoCloseable {

        private final File m_file;

        private CustomArrowFileWriter m_writer;

        private VectorLoader m_vectorLoader;

        private VectorSchemaRoot m_root;

        public FieldVectorWriter(final File file) {
            m_file = file;
        }

        @SuppressWarnings("resource")
        public void write(final List<FieldVector> vecs, final ArrowDictProvider dicts) throws IOException {
            if (m_writer == null) {
                final ArrayList<Field> fields = new ArrayList<>(vecs.size());
                for (final FieldVector v : vecs) {
                    fields.add(v.getField());
                }
                final Map<String, String> metadata = new HashMap<>();
                metadata.put(ArrowColumnStore.CFG_ARROW_CHUNK_SIZE, String.valueOf(m_chunkSize));
                m_root = VectorSchemaRoot.create(new Schema(fields, metadata), m_allocator);
                m_vectorLoader = new VectorLoader(m_root);
                m_writer = new CustomArrowFileWriter(m_root, dicts, new RandomAccessFile(m_file, "rw").getChannel());
            } else {
                // Write dictionaries starting from second iteration.
                for (final Dictionary dict : dicts) {
                    DictionaryEncoding encoding = dict.getEncoding();
                    FieldVector vector = dict.getVector();
                    int count = vector.getValueCount();
                    final VectorSchemaRoot dictRoot = new VectorSchemaRoot(Collections.singletonList(vector.getField()),
                        Collections.singletonList(vector), count);
                    VectorUnloader unloader = new VectorUnloader(dictRoot);
                    try (ArrowRecordBatch batch = unloader.getRecordBatch();
                            ArrowDictionaryBatch dictBatch = new ArrowDictionaryBatch(encoding.getId(), batch, false)) {
                        m_writer.writeDictionaryBatch(dictBatch);
                    }
                    dictRoot.close();
                }
            }

            final List<ArrowFieldNode> nodes = new ArrayList<>();
            final List<ArrowBuf> buffers = new ArrayList<>();
            for (final FieldVector vec : vecs) {
                appendNodes(vec, nodes, buffers);
            }

            try (final ArrowRecordBatch batch =
                new ArrowRecordBatch(vecs.get(0).getValueCount(), nodes, buffers)) {
                m_vectorLoader.load(batch);
                m_writer.writeBatch();
            }
        }

        @Override
        public void close() {
            if (m_writer != null) {
                m_writer.close();
                m_root.close();
            }
        }

        // TODO: Copied from org.apache.arrow.vector.VectorUnloader. Is there a better
        // way to do all of this (including writing vectors in general)?
        private void appendNodes(final FieldVector vector, final List<ArrowFieldNode> nodes,
            final List<ArrowBuf> buffers) {
            nodes.add(new ArrowFieldNode(vector.getValueCount(), vector.getNullCount()));
            final List<ArrowBuf> fieldBuffers = vector.getFieldBuffers();
            final int expectedBufferCount = TypeLayout.getTypeBufferCount(vector.getField().getType());
            if (fieldBuffers.size() != expectedBufferCount) {
                throw new IllegalArgumentException(
                    String.format("wrong number of buffers for field %s in vector %s. found: %s", vector.getField(),
                        vector.getClass().getSimpleName(), fieldBuffers));
            }
            buffers.addAll(fieldBuffers);
            for (final FieldVector child : vector.getChildrenFromFields()) {
                appendNodes(child, nodes, buffers);
            }
        }
    }

    final class ArrowDictProvider implements DictionaryProvider, Iterable<Dictionary> {

        private final Map<Long, Dictionary> m_dicts = new HashMap<>();

        @Override
        public Dictionary lookup(final long index) {
            return m_dicts.get(index);
        }

        public final void add(final Dictionary dict) {
            m_dicts.put(dict.getEncoding().getId(), dict);
        }

        public long size() {
            return m_dicts.size();
        }

        @Override
        public Iterator<Dictionary> iterator() {
            return m_dicts.values().iterator();
        }
    }

    final class CustomArrowFileWriter extends ArrowFileWriter {
        CustomArrowFileWriter(final VectorSchemaRoot root, //
            final DictionaryProvider provider, //
            final WritableByteChannel out) {
            super(root, provider, out);
        }

        @Override
        public ArrowBlock writeDictionaryBatch(final ArrowDictionaryBatch batch) throws IOException {
            return super.writeDictionaryBatch(batch);
        }
    }
}
