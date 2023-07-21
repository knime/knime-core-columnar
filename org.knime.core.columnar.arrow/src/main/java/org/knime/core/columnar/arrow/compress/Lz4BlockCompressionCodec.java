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
 */

package org.knime.core.columnar.arrow.compress;

import static org.apache.arrow.vector.compression.CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.bytedeco.lz4.global.lz4;

/**
 * Implementation of LZ4 Block compression.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class Lz4BlockCompressionCodec extends AbstractCompressionCodec {

    @Override
    public ArrowBuf compress(final BufferAllocator allocator, final ArrowBuf uncompressedBuffer) {
        throw new IllegalStateException("LZ4 Block compression is not supported anymore. Use LZ4 Frame compression.");
    }

    @Override
    protected ArrowBuf doCompress(final BufferAllocator allocator, final ArrowBuf uncompressedBuffer) {
        // Not called
        return null;
    }

    @Override
    @SuppressWarnings("resource")
    protected ArrowBuf doDecompress(final BufferAllocator allocator, final ArrowBuf compressedBuffer) {
        Preconditions.checkArgument(compressedBuffer.writerIndex() <= Integer.MAX_VALUE,
            "The compressed buffer size exceeds the integer limit");

        // Length of the result
        final long decompressedLength = readUncompressedLength(compressedBuffer);
        Preconditions.checkArgument(decompressedLength <= Integer.MAX_VALUE,
            "The decompressed buffer size exceeds the integer limit");

        // Allocate decompression result
        final ArrowBuf decompressedBuffer = allocator.buffer(decompressedLength);

        // Decompress
        final int decompressedBytes;
        try (final var srcPointer = new ArrowBufPointer(compressedBuffer, SIZE_OF_UNCOMPRESSED_LENGTH);
                final var dstPointer = new ArrowBufPointer(decompressedBuffer, 0)) {
            decompressedBytes = lz4.LZ4_decompress_safe(srcPointer, dstPointer,
                (int)(compressedBuffer.writerIndex() - SIZE_OF_UNCOMPRESSED_LENGTH), (int)decompressedLength);
        }

        if (decompressedBytes < 0) {
            throw new CompressionException("LZ4 decompression failed. Return code: " + decompressedBytes);
        }
        if (decompressedBytes != decompressedLength) {
            throw new CompressionException("LZ4 decompression failed. Expected " + decompressedLength
                + " bytes, decompressed " + decompressedBytes + " bytes.");
        }

        decompressedBuffer.writerIndex(decompressedLength);
        return decompressedBuffer;
    }

    @Override
    public CodecType getCodecType() {
        return CodecType.LZ4_FRAME;
    }
}