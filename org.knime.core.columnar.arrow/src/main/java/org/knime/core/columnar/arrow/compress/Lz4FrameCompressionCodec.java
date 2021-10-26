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
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.bytedeco.javacpp.Pointer;
import org.bytedeco.javacpp.SizeTPointer;
import org.bytedeco.lz4.LZ4FDecompressOptions;
import org.bytedeco.lz4.LZ4FDecompressionContext;
import org.bytedeco.lz4.LZ4FPreferences;
import org.bytedeco.lz4.global.lz4;

/**
 * Implementation of LZ4 Frame compression.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class Lz4FrameCompressionCodec extends AbstractCompressionCodec {

    /** With null the default is used */
    private static final LZ4FPreferences COMPRESSION_OPTIONS = null;

    /** With null the default is used */
    private static final LZ4FDecompressOptions DECOMPRESS_OPTIONS = null;

    /*
     * Mostly copied from AbstractCompressionCodec#compress.
     *
     * The format specifies that -1 can be used as a length to signal that the buffer is not compressed (if the
     * compression brings no advantage). The implementation in AbstractCompressionCodec implements this optimization.
     * However, the optimization is not implemented in the C++ Arrow implementation and therefore not supported by
     * pyarrow. This implementation removes this optimization.
     *
     * TODO remove when https://issues.apache.org/jira/browse/ARROW-12196 is fixed
     */
    @SuppressWarnings("resource")
    @Override
    public ArrowBuf compress(final BufferAllocator allocator, final ArrowBuf uncompressedBuffer) {
        if (uncompressedBuffer.writerIndex() == 0L) {
            // shortcut for empty buffer
            ArrowBuf compressedBuffer = allocator.buffer(SIZE_OF_UNCOMPRESSED_LENGTH);
            compressedBuffer.setLong(0, 0);
            compressedBuffer.writerIndex(SIZE_OF_UNCOMPRESSED_LENGTH);
            uncompressedBuffer.close();
            return compressedBuffer;
        }

        ArrowBuf compressedBuffer = doCompress(allocator, uncompressedBuffer);
        long uncompressedLength = uncompressedBuffer.writerIndex();

        // NOTE: THE DIFFERENCE IS HERE
        writeUncompressedLength(compressedBuffer, uncompressedLength);

        uncompressedBuffer.close();
        return compressedBuffer;
    }

    @Override
    @SuppressWarnings("resource")
    protected ArrowBuf doCompress(final BufferAllocator allocator, final ArrowBuf uncompressedBuffer) {
        final long uncompressedLength = uncompressedBuffer.writerIndex();

        // The maximum size of the compressed data
        final long maxCompressedLength = lz4.LZ4F_compressFrameBound(uncompressedLength, COMPRESSION_OPTIONS);

        // Allocated buffer for the compression result
        final ArrowBuf compressedBuffer = allocator.buffer(maxCompressedLength + SIZE_OF_UNCOMPRESSED_LENGTH);

        // Compress
        final Pointer srcPointer = new ArrowBufPointer(uncompressedBuffer, 0);
        final Pointer dstPointer = new ArrowBufPointer(compressedBuffer, SIZE_OF_UNCOMPRESSED_LENGTH);
        final long bytesWritten = lz4.LZ4F_compressFrame(dstPointer, maxCompressedLength, srcPointer,
            uncompressedLength, COMPRESSION_OPTIONS);

        compressedBuffer.writerIndex(bytesWritten + SIZE_OF_UNCOMPRESSED_LENGTH);
        return compressedBuffer;
    }

    @Override
    @SuppressWarnings("resource")
    protected ArrowBuf doDecompress(final BufferAllocator allocator, final ArrowBuf compressedBuffer) {
        // Length of the result
        final long decompressedLength = readUncompressedLength(compressedBuffer);

        // Allocate decompression result
        final ArrowBuf decompressedBuffer = allocator.buffer(decompressedLength);

        long decompressedBytes = 0;
        try (final var dctx = new LZ4FDecompressionContext()) {

            // Init the decompression context
            final long ctxError = lz4.LZ4F_createDecompressionContext(dctx, lz4.LZ4F_VERSION);
            checkForLZ4FError(ctxError);

            try (
                    // Pointers to the data
                    final var srcPointer = new ArrowBufPointer(compressedBuffer, SIZE_OF_UNCOMPRESSED_LENGTH);
                    final var dstPointer = new ArrowBufPointer(decompressedBuffer, 0);
                    // Sizes of the data
                    final var srcSize = new SizeTPointer(1);
                    final var dstSize = new SizeTPointer(1);) {

                long ret;
                do {
                    // Update the size pointers with the currently remaining sizes
                    dstSize.put(dstPointer.capacity() - dstPointer.position());
                    srcSize.put(srcPointer.capacity() - srcPointer.position());

                    // Decompress
                    ret = lz4.LZ4F_decompress(dctx, dstPointer, dstSize, srcPointer, srcSize, DECOMPRESS_OPTIONS);
                    checkForLZ4FError(ret);

                    // Update the positions of the pointers
                    srcPointer.position(srcPointer.position() + srcSize.get());
                    dstPointer.position(dstPointer.position() + dstSize.get());
                } while (ret != 0);
                decompressedBytes = dstPointer.position();
            } finally {
                // Free the compression context (needed if there was an error)
                lz4.LZ4F_freeDecompressionContext(dctx);
            }
        }

        if (decompressedBytes != decompressedLength) {
            throw new CompressionException("LZ4 decompression failed. Expected " + decompressedLength
                + " bytes, decompressed " + decompressedBytes + " bytes.");
        }

        decompressedBuffer.writerIndex(decompressedLength);
        return decompressedBuffer;
    }

    private static void checkForLZ4FError(final long errorCode) {
        if (lz4.LZ4F_isError(errorCode) != 0) {
            throw new CompressionException("LZ4 decompression failed: " + lz4.LZ4F_getErrorName(errorCode).getString());
        }
    }

    @Override
    public CodecType getCodecType() {
        return CodecType.LZ4_FRAME;
    }
}