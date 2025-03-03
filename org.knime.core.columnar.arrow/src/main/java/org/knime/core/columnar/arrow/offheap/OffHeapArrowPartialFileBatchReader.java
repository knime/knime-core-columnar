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
package org.knime.core.columnar.arrow.offheap;

import java.io.File;
import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils.OffsetProvider;
import org.knime.core.columnar.arrow.mmap.MappableReadChannel;
import org.knime.core.columnar.arrow.mmap.MappedMessageSerializer;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.batch.SequentialBatchReader;
import org.knime.core.columnar.filter.ColumnSelection;

/**
 * An implementation of a {@link RandomAccessBatchReader} for Arrow which reads from a file in the Arrow IPC file format
 * using an {@link OffsetProvider} to access the individual batches. The file does not have to be written completely.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
class OffHeapArrowPartialFileBatchReader extends AbstractOffHeapArrowBatchReader implements SequentialBatchReader {

    private final File m_file;

    private final OffsetProvider m_offsetProvider;

    private int m_currentBatchIndex = -1;

    OffHeapArrowPartialFileBatchReader(final File file, final BufferAllocator allocator,
        final OffHeapArrowColumnDataFactory[] factories, final ColumnSelection columnSelection,
        final OffsetProvider offsetProvider) {
        super(allocator, factories, columnSelection);
        m_file = file;
        m_offsetProvider = offsetProvider;
    }

    @Override
    protected ArrowReader createReader() throws IOException {
        return new ArrowPartialFileReader(m_file, m_offsetProvider);
    }

    /**
     * An {@link ArrowReader} that reads from a file with the IPC File format using the offsets from a
     * {@link OffsetProvider}. The file does not have to be written completely.
     */
    private static final class ArrowPartialFileReader implements ArrowReader {

        private final MappableReadChannel m_in;

        private final Schema m_schema;

        private final OffsetProvider m_offsetProvider;

        private ArrowPartialFileReader(final File file, final OffsetProvider offsetProvider) throws IOException {
            m_offsetProvider = offsetProvider;
            m_in = new MappableReadChannel(file, "r");

            ArrowReaderWriterUtils.checkFileSize(m_in);
            ArrowReaderWriterUtils.checkArrowMagic(m_in, false);
            m_schema = readSchema(m_in);
        }

        @Override
        public Schema getSchema() {
            return m_schema;
        }

        @Override
        public synchronized ArrowRecordBatch readRecordBatch(final int index) throws IOException {
            final long offset = m_offsetProvider.getRecordBatchOffset(index);
            return MappedMessageSerializer.deserializeRecordBatch(m_in, offset);
        }

        @Override
        public synchronized ArrowDictionaryBatch[] readDictionaryBatches(final int index) throws IOException {
            final long[] offsets = m_offsetProvider.getDictionaryBatchOffsets(index);
            final ArrowDictionaryBatch[] dictionaryBatches = new ArrowDictionaryBatch[offsets.length];
            try {
                for (int i = 0; i < offsets.length; i++) {
                    @SuppressWarnings("resource") // Resource closed by caller
                    final ArrowDictionaryBatch batch =
                        MappedMessageSerializer.deserializeDictionaryBatch(m_in, offsets[i]);
                    dictionaryBatches[i] = batch;
                }
            } catch (final IOException ex) {
                // Close all batches in case of an exception
                for (final ArrowDictionaryBatch b : dictionaryBatches) {
                    if (b != null) {
                        b.close();
                    }
                }
                throw ex;
            }
            return dictionaryBatches;
        }

        @Override
        public void close() throws IOException {
            m_in.close();
        }

        /** Read the schema from the beginning of the channel (after the magic number) */
        private static Schema readSchema(final MappableReadChannel in) throws IOException {
            // Magic number (6 bytes) + empty padding to 8 byte boundary
            in.setPosition(8);
            return MessageSerializer.deserializeSchema(in);
        }
    }

    @Override
    public ReadBatch forward() throws IOException {
        m_currentBatchIndex++;
        return readRetained(m_currentBatchIndex);
    }
}
