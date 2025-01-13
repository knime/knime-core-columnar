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

import static org.knime.core.columnar.arrow.ArrowReaderWriterUtils.ARROW_MAGIC_LENGTH;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.arrow.flatbuf.Footer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowFooter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.knime.core.columnar.arrow.ArrowReaderWriterUtils;
import org.knime.core.columnar.arrow.mmap.MappableReadChannel;
import org.knime.core.columnar.arrow.mmap.MappedMessageSerializer;
import org.knime.core.columnar.batch.RandomAccessBatchReader;
import org.knime.core.columnar.batch.ReadBatch;
import org.knime.core.columnar.filter.ColumnSelection;

/**
 * An implementation of a {@link RandomAccessBatchReader} for Arrow which reads from a file in the Arrow IPC file format
 * after it has been written completely.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
class OffHeapArrowBatchReader extends AbstractOffHeapArrowBatchReader implements RandomAccessBatchReader {

    private final File m_file;

    OffHeapArrowBatchReader(final File file, final BufferAllocator allocator, final OffHeapArrowColumnDataFactory[] factories,
        final ColumnSelection columnSelection) {
        super(allocator, factories, columnSelection);
        m_file = file;
    }

    @Override
    public ReadBatch readRetained(final int index) throws IOException {
        return super.readRetained(index);
    }

    @Override
    protected ArrowReader createReader() throws IOException {
        return new ArrowFileReader(m_file);
    }

    // Override getMetadata to handle legacy files
    @Override
    protected Map<String, String> getMetadata() {
        var metadata = super.getMetadata();
        var footerMetadata = ((ArrowFileReader)m_reader).getFooter().getMetaData();

        var metadataToReturn = metadata;

        if (metadata.isEmpty()) {
            // Legacy: For <4.4 the metadata was saved in the footer
            metadataToReturn = footerMetadata;
        } else if (!metadata.containsKey(ArrowReaderWriterUtils.ARROW_BATCH_BOUNDARIES_KEY)) {
            // Before 5.3 we stored the chunk size as all batches (but the last) had the same size,
            // so we can construct the batch boundaries
            metadataToReturn = new HashMap<>(metadata);
            if (footerMetadata.containsKey(ArrowReaderWriterUtils.ARROW_BATCH_BOUNDARIES_KEY))
            {
                // Copy batch boundaries from footer to metadata. They are stored in the footer because
                // we only know them once all batches have been written.
                metadataToReturn.put(ArrowReaderWriterUtils.ARROW_BATCH_BOUNDARIES_KEY,
                    footerMetadata.get(ArrowReaderWriterUtils.ARROW_BATCH_BOUNDARIES_KEY));
            } else {
                // Construct from old batchLength info
                int batchLength = readChunkSize(metadata, footerMetadata);

                var numBatches = ((ArrowFileReader)m_reader).getNumberOfBatches();
                var boundaries = new long[numBatches];
                IntStream.range(0, numBatches - 1).forEach(i -> boundaries[i] = (i + 1) * batchLength);
                try (var lastBatch = ((ArrowFileReader)m_reader).readRecordBatch(numBatches - 1)) {
                    var lastBatchNumRows = lastBatch.getLength();
                    boundaries[numBatches - 1] = (numBatches - 1) * batchLength + lastBatchNumRows;
                    metadataToReturn.put(ArrowReaderWriterUtils.ARROW_BATCH_BOUNDARIES_KEY, ArrowReaderWriterUtils.longArrayToString(boundaries));
                } catch (IOException ex) {
                    throw new IllegalStateException("Could not determine batch boundaries", ex);
                }
            }
        }

        return metadataToReturn;
    }

    @SuppressWarnings("deprecation")
    private static int readChunkSize(final Map<String, String> metadata, final Map<String, String> footerMetadata) {
        if (metadata.containsKey(ArrowReaderWriterUtils.ARROW_CHUNK_SIZE_KEY)) {
            return Integer.parseInt(metadata.get(ArrowReaderWriterUtils.ARROW_CHUNK_SIZE_KEY));
        } else if (footerMetadata.containsKey(ArrowReaderWriterUtils.ARROW_CHUNK_SIZE_KEY)) {
            return Integer.parseInt(footerMetadata.get(ArrowReaderWriterUtils.ARROW_CHUNK_SIZE_KEY));
        } else {
            throw new IllegalStateException("Could not find batch length");
        }
    }

    int numBatches() throws IOException {
        if (m_reader == null) {
            initializeReader();
        }
        return ((ArrowFileReader)m_reader).getNumberOfBatches();
    }

    /** An {@link ArrowReader} that reads from a file with the IPC File format. The file must be written completely. */
    private static final class ArrowFileReader implements ArrowReader {

        private final MappableReadChannel m_in;

        private final ArrowFooter m_footer;

        private final int m_dictionariesPerBatch;

        private ArrowFileReader(final File file) throws IOException {
            m_in = new MappableReadChannel(file, "r");

            ArrowReader.checkFileSize(m_in);
            ArrowReader.checkArrowMagic(m_in, true);
            m_footer = readFooter(m_in);
            m_dictionariesPerBatch = getDictionariesPerBatch(m_footer);
        }

        /** Get the schema of the read file */
        @Override
        public Schema getSchema() {
            return m_footer.getSchema();
        }

        /** Read the record batch for the given index */
        @Override
        public synchronized ArrowRecordBatch readRecordBatch(final int index) throws IOException {
            final ArrowBlock block = m_footer.getRecordBatches().get(index);
            return MappedMessageSerializer.deserializeRecordBatch(m_in, block.getOffset());
        }

        /** Read the dictionary batches for the given index */
        @Override
        public synchronized ArrowDictionaryBatch[] readDictionaryBatches(final int index) throws IOException {
            final ArrowDictionaryBatch[] dictionaryBatches = new ArrowDictionaryBatch[m_dictionariesPerBatch];
            final int offset = m_dictionariesPerBatch * index;
            try {
                for (int i = 0; i < m_dictionariesPerBatch; i++) {
                    final ArrowBlock block = m_footer.getDictionaries().get(i + offset);
                    @SuppressWarnings("resource") // Resource closed by caller
                    final ArrowDictionaryBatch batch =
                        MappedMessageSerializer.deserializeDictionaryBatch(m_in, block.getOffset());
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

        /** Get the footer of the read file */
        private ArrowFooter getFooter() {
            return m_footer;
        }

        /** Get the number of batches */
        private int getNumberOfBatches() {
            return m_footer.getRecordBatches().size();
        }

        /** Read the footer length from the file */
        private static final int readFooterLength(final SeekableReadChannel in) throws IOException {
            final ByteBuffer buffer = ByteBuffer.allocate(4);
            in.setPosition(in.size() - ARROW_MAGIC_LENGTH - 4);
            in.readFully(buffer);
            buffer.flip();
            final int footerLength = MessageSerializer.bytesToInt(buffer.array());
            if (footerLength <= 0 || footerLength + ARROW_MAGIC_LENGTH * 2 + 4 > in.size()) {
                throw new IOException("Arrow file invalid: Invalid footer length: " + footerLength);
            }
            return footerLength;
        }

        /** Read the footer from the file */
        private static final ArrowFooter readFooter(final SeekableReadChannel in) throws IOException {
            final int footerLength = readFooterLength(in);
            final long footerStart = in.size() - ARROW_MAGIC_LENGTH - 4 - footerLength;
            final ByteBuffer buffer = ByteBuffer.allocate(footerLength);
            in.setPosition(footerStart);
            in.readFully(buffer);
            buffer.flip();
            return new ArrowFooter(Footer.getRootAsFooter(buffer));
        }

        /** Get the number of dictionaries per batch. Throw an exception if the number of dictionaries does not fit */
        private static int getDictionariesPerBatch(final ArrowFooter footer) throws IOException {
            final int numBatches = footer.getRecordBatches().size();
            final int numDictionaries = footer.getDictionaries().size();
            if (numBatches == 0) {
                // pyarrow doesn't write any batches if a table is empty
                if (numDictionaries != 0) {
                    throw new IOException("Arrow file invalid: There are no batches but there are dictionaries.");
                }
                return 0;
            }
            if (numDictionaries % numBatches != 0) {
                throw new IOException(
                    "Arrow file invalid: There must be the same number of dictionaries for each batch.");
            }
            return numDictionaries / numBatches;
        }
    }
}
