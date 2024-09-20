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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowFooter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

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
final class ArrowWriter implements AutoCloseable {

    private final WriteChannel m_out;

    private final Schema m_schema;

    private final IpcOption m_option;

    final List<ArrowBlock[]> m_dictionaryBlocks;

    final List<ArrowBlock> m_recordBlocks;

    ArrowWriter(final File file, final Schema schema) throws IOException {
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
    void writeDictionaryBatches(final ArrowDictionaryBatch[] batches) throws IOException {
        final ArrowBlock[] blocks = new ArrowBlock[batches.length];
        for (int i = 0; i < batches.length; i++) {
            blocks[i] = MessageSerializer.serialize(m_out, batches[i], m_option);
        }
        m_dictionaryBlocks.add(blocks);
    }

    /** Write the given data batch */
    void writeRecordBatch(final ArrowRecordBatch batch) throws IOException {
        final ArrowBlock block = MessageSerializer.serialize(m_out, batch, m_option);
        m_recordBlocks.add(block);
    }

    /** Write the arrow file footer. Call before close to create a valid arrow file */
    void writeFooter(final long[] batchBoundaries) throws IOException {
        // Write EOS
        m_out.writeIntLittleEndian(MessageSerializer.IPC_CONTINUATION_TOKEN);
        m_out.writeIntLittleEndian(0);

        // Write the footer
        final List<ArrowBlock> dictBlocks =
            m_dictionaryBlocks.stream().flatMap(Arrays::stream).collect(Collectors.toList());
        final Map<String, String> metadata = new HashMap<>();
        metadata.put(ArrowReaderWriterUtils.ARROW_BATCH_BOUNDARIES_KEY,
            ArrowReaderWriterUtils.longArrayToString(batchBoundaries));
        final ArrowFooter footer =
            new ArrowFooter(m_schema, dictBlocks, m_recordBlocks, metadata, m_option.metadataVersion);
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