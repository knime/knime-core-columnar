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
 *   Mar 31, 2021 (benjamin): created
 */
package org.knime.core.columnar.arrow.mmap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.arrow.vector.ipc.SeekableReadChannel;

/**
 * A {@link SeekableReadChannel} which supports mapping sections of the channel into memory.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class MappableReadChannel extends SeekableReadChannel {

    private File m_file;

    private FileChannel m_in;

    /**
     * Create a new {@link MappableReadChannel}.
     *
     * @param file the file to open the channel on
     * @param mode the access mode (See {@link RandomAccessFile#RandomAccessFile(File, String)})
     * @throws FileNotFoundException (See {@link RandomAccessFile#RandomAccessFile(File, String)})
     */
    @SuppressWarnings("resource") // The file channel is closed by the parent on #close
    public MappableReadChannel(final File file, final String mode) throws FileNotFoundException {
        this(open(file, mode), file);
    }

    private MappableReadChannel(final FileChannel in, final File file) {
        super(in);
        m_file = file;
        m_in = in;
    }

    private static FileChannel open(final File file, final String mode) throws FileNotFoundException {
        @SuppressWarnings("resource") // raFile is closed by the channel (which is closed by the caller)
        final RandomAccessFile raFile = new RandomAccessFile(file, mode); // NOSONAR: See above
        return raFile.getChannel();
    }

    /**
     * Maps the following size bytes into a {@link MappedByteBuffer}.
     *
     * @param mode defines how the memory is mapped. See {@link FileChannel#map(MapMode, long, long)}.
     * @param size the size of the memory to map in bytes
     * @return the {@link MappedByteBuffer}. The memory is released when the buffer is collected by the garbage
     *         collector.
     * @throws IOException if some I/O error occurs (See {@link FileChannel#map(MapMode, long, long)}.
     */
    public MappedByteBuffer map(final MapMode mode, final long size) throws IOException {
        return m_in.map(mode, m_in.position(), size);
    }

    /**
     * @return the file this channel is pointing to
     */
    File getFile() {
        return m_file;
    }
}
