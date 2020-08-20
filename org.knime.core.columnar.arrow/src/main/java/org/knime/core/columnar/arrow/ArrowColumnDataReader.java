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
import java.nio.channels.SeekableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.util.TransferPair;
import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnSelection;

class ArrowColumnDataReader implements ColumnDataReader {

    private final FieldVectorReader m_reader;

    private ArrowColumnDataSpec<?>[] m_arrowSchema;

    private ColumnSelection m_selection;

    public ArrowColumnDataReader(final ColumnStoreSchema schema, final File file, final BufferAllocator allocator,
        final ColumnSelection selection) {
        m_reader = new FieldVectorReader(file, allocator);
        m_selection = selection;

        // TODO mapper should actually be read from arrow for backwards compatibility.
        m_arrowSchema = new ArrowSchemaMapperV0().map(schema);

    }

    @Override
    public void close() throws Exception {
        m_reader.close();
    }

    @Override
    public ColumnData[] read(final int chunkIdx) throws IOException {
        final FieldVector[] vectors = m_reader.read(chunkIdx);
        final DictionaryProvider provider = m_reader.dictionaries(chunkIdx);
        final ColumnData[] data = new ColumnData[vectors.length];
        if (m_selection != null) {
            final int[] selected = m_selection.get();
            int j = 0;
            for (int i = 0; i < data.length; i++) {
                if (j < selected.length && selected[j] == i) {
                    // TODO transfer ownership of dictionary vector for parallel reads
                    data[i] = m_arrowSchema[i].wrap(vectors[i], provider);
                    j++;
                } else {
                    vectors[i].clear();
                }
            }
        } else {
            for (int i = 0; i < data.length; i++) {
                data[i] = m_arrowSchema[i].wrap(vectors[i], provider);
            }
        }
        return data;
    }

    @Override
    public int getNumChunks() {
        return m_reader.getRecords();
    }

    @Override
    public int getMaxDataCapacity() {
        return m_reader.getChunkSize();
    }

    // not supposed to be thread-safe
    static class FieldVectorReader implements AutoCloseable {

        // some constants
        private final BufferAllocator m_alloc;

        // Varies with each partition
        private VectorSchemaRoot m_root;

        private File m_file;

        private CustomArrowFileReader m_reader;

        private List<ArrowBlock> m_blocks;

        private BufferAllocator m_childAlloc;

        // TODO support for column filtering and row filtering ('TableFilter'), i.e.
        // only load required columns / rows from disc. Rows should be easily possible
        // by using 'ArrowBlock'
        FieldVectorReader(final File file, final BufferAllocator alloc) {
            m_alloc = alloc;
            m_childAlloc = alloc.newChildAllocator("FieldVectorReader", 0, alloc.getLimit());
            m_file = file;
        }

        DictionaryProvider dictionaries(final long index) throws IOException {
            initialize();

            // load all dictionaries for index
            m_reader.loadDictionaries(index);
            final Map<Long, Dictionary> vecs = m_reader.getDictionaryVectors();
            final Map<Long, Dictionary> copied = new HashMap<Long, Dictionary>();
            for (Entry<Long, Dictionary> entry : vecs.entrySet()) {
                final FieldVector v = entry.getValue().getVector();
                final TransferPair transferPair = v.getTransferPair(m_alloc);
                transferPair.transfer();
                copied.put(entry.getKey(),
                    new Dictionary((FieldVector)transferPair.getTo(), entry.getValue().getEncoding()));
            }

            // after return, reader doesn't know anything anymore about these dicts and
            // transfered ownership to caller
            return new DictionaryProvider() {
                @Override
                public Dictionary lookup(final long arg0) {
                    return copied.get(arg0);
                }
            };
        }

        // Assumption for this reader: sequential loading.
        FieldVector[] read(final long index) throws IOException {
            initialize();

            // load next record batch
            m_reader.loadRecordBatch(m_blocks.get((int)index));
            final List<FieldVector> fieldVectors = m_root.getFieldVectors();
            final FieldVector[] res = new FieldVector[fieldVectors.size()];
            for (int i = 0; i < res.length; i++) {
                final FieldVector v = fieldVectors.get(i);
                final TransferPair transferPair = v.getTransferPair(m_alloc);
                transferPair.transfer();
                res[i] = (FieldVector)transferPair.getTo();
            }
            return res;
        }

        @SuppressWarnings("resource")
        private void initialize() throws IOException {
            if (m_reader == null) {
                m_reader = new CustomArrowFileReader(new RandomAccessFile(m_file, "rw").getChannel(), m_childAlloc);
                m_root = m_reader.getVectorSchemaRoot();
                m_blocks = m_reader.getRecordBlocks();
            }
        }

        int getChunkSize() {
            try {
                initialize();
                return Integer
                    .valueOf(m_root.getSchema().getCustomMetadata().get(ArrowColumnStore.CFG_ARROW_CHUNK_SIZE));
            } catch (IOException e) {
                // TODO
                throw new RuntimeException(e);
            }
        }

        int getRecords() {
            try {
                initialize();
                return m_blocks.size();
            } catch (IOException e) {
                // TODO
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws Exception {
            if (m_root != null) {
                m_reader.close();
            }
            m_childAlloc.close();
        }
    }

    final static class CustomArrowFileReader extends ArrowFileReader {
        private final SeekableReadChannel m_in;

        CustomArrowFileReader(final SeekableReadChannel in, final BufferAllocator allocator) {
            super(in, allocator);
            m_in = in;
        }

        CustomArrowFileReader(final SeekableByteChannel in, final BufferAllocator allocator) {
            this(new SeekableReadChannel(in), allocator);
        }

        final void loadDictionaries(final long index) throws IOException {
            final List<ArrowBlock> dictionaryBlocks = getDictionaryBlocks();
            final int offset = (int)(getDictionaryVectors().size() * index);
            for (int i = offset; i < offset + getDictionaryVectors().size(); i++) {
                final ArrowBlock block = dictionaryBlocks.get(i);
                m_in.setPosition(block.getOffset());
                loadDictionary(MessageSerializer.deserializeDictionaryBatch(m_in, block, allocator));
            }
        }
    }
}
