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
 *   Sep 20, 2024 (benjamin): created
 */
package org.knime.core.columnar.onheap;

import static org.knime.core.columnar.onheap.ArrowReaderWriterUtils.ARROW_MAGIC_BYTES;
import static org.knime.core.columnar.onheap.ArrowReaderWriterUtils.ARROW_MAGIC_LENGTH;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A simple Arrow reader. {@link org.apache.arrow.vector.ipc.ArrowFileReader} has the following problems:
 * <ul>
 * <li>VectorSchemaRoot holds vectors that are filled over and over again. Copying of data is required to get the
 * vectors.</li>
 * <li>Cannot filter which vectors are allocated</li>
 * <li>Cannot read specific dictionaries for a batch.</li>
 * </ul>
 */
interface ArrowReader extends AutoCloseable {

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

    /**
     * Check if the Arrow file has a valid length. Throws an exception if not.
     *
     * @param in the channel on the file
     * @throws IOException if the channel is too small
     */
    static void checkFileSize(final SeekableReadChannel in) throws IOException {
        if (in.size() <= ARROW_MAGIC_LENGTH * 2 + 4) {
            throw new IOException("Arrow file invalid: File is too small: " + in.size());
        }
    }

    /**
     * Check if the Arrow has the arrow magic bytes at the beginning or the end. Throws an exception if not.
     *
     * @param in the channel on the file
     * @param end if true, checks if the magic number is at the end of the channel. If false, checks if the magic
     *            number is at the beginning of the channel
     * @throws IOException if the magic number is not at the expected location
     */
    static void checkArrowMagic(final SeekableReadChannel in, final boolean end) throws IOException {
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
}