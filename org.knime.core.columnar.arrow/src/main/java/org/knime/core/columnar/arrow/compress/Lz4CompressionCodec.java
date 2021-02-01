/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.knime.core.columnar.arrow.compress;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.arrow.flatbuf.CompressionType;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.compression.CompressionCodec;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/*
 * NOTE:
 * The following code is written by Liya Fan. See https://github.com/apache/arrow/pull/8949.
 * The code is licensed under the Apache License, Version 2.0 as noted above.
 *
 * Changes:
 * The static utilities LITTLE_ENDIAN, SIZE_OF_UNCOMPRESSD_LENGTH, NO_COMPRESSION_LENGTH,
 * compressRawBuffer and decompressRawBuffer were copied into this class.
 * Formatting changes were made.
 *
 * The code should be removed when the class is available in the Apache Arrow Release.
 */

/**
 * Compression codec for the LZ4 algorithm.
 */
public class Lz4CompressionCodec implements CompressionCodec {

    private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    private static final long SIZE_OF_UNCOMPRESSED_LENGTH = 8L;

    private static final long NO_COMPRESSION_LENGTH = -1L;

    private final LZ4Factory factory;

    private LZ4Compressor compressor;

    private LZ4FastDecompressor decompressor;

    public Lz4CompressionCodec() {
        factory = LZ4Factory.nativeInstance();
    }

    @Override
    public ArrowBuf compress(final BufferAllocator allocator, final ArrowBuf uncompressedBuffer) {
        Preconditions.checkArgument(uncompressedBuffer.writerIndex() <= Integer.MAX_VALUE,
            "The uncompressed buffer size exceeds the integer limit");

        if (uncompressedBuffer.writerIndex() == 0L) {
            // shortcut for empty buffer
            ArrowBuf compressedBuffer = allocator.buffer(SIZE_OF_UNCOMPRESSED_LENGTH);
            compressedBuffer.setLong(0, 0);
            compressedBuffer.writerIndex(SIZE_OF_UNCOMPRESSED_LENGTH);
            uncompressedBuffer.close();
            return compressedBuffer;
        }

        // create compressor lazily
        if (compressor == null) {
            compressor = factory.fastCompressor();
        }

        int maxCompressedLength = compressor.maxCompressedLength((int)uncompressedBuffer.writerIndex());

        // first 8 bytes reserved for uncompressed length, to be consistent with the
        // C++ implementation.
        ArrowBuf compressedBuffer = allocator.buffer(maxCompressedLength + SIZE_OF_UNCOMPRESSED_LENGTH);
        long uncompressedLength = uncompressedBuffer.writerIndex();
        if (!LITTLE_ENDIAN) {
            uncompressedLength = Long.reverseBytes(uncompressedLength);
        }
        compressedBuffer.setLong(0, uncompressedLength);

        ByteBuffer uncompressed =
            MemoryUtil.directBuffer(uncompressedBuffer.memoryAddress(), (int)uncompressedBuffer.writerIndex());
        ByteBuffer compressed = MemoryUtil.directBuffer(compressedBuffer.memoryAddress() + SIZE_OF_UNCOMPRESSED_LENGTH,
            maxCompressedLength);

        long compressedLength = compressor.compress(uncompressed, 0, (int)uncompressedBuffer.writerIndex(), compressed,
            0, maxCompressedLength);
        if (compressedLength > uncompressedBuffer.writerIndex()) {
            // compressed buffer is larger, send the raw buffer
            compressRawBuffer(uncompressedBuffer, compressedBuffer);
            compressedLength = uncompressedBuffer.writerIndex();
        }
        compressedBuffer.writerIndex(compressedLength + SIZE_OF_UNCOMPRESSED_LENGTH);

        uncompressedBuffer.close();
        return compressedBuffer;
    }

    @Override
    public ArrowBuf decompress(final BufferAllocator allocator, final ArrowBuf compressedBuffer) {
        Preconditions.checkArgument(compressedBuffer.writerIndex() <= Integer.MAX_VALUE,
            "The compressed buffer size exceeds the integer limit");

        Preconditions.checkArgument(compressedBuffer.writerIndex() >= SIZE_OF_UNCOMPRESSED_LENGTH,
            "Not enough data to decompress.");

        long decompressedLength = compressedBuffer.getLong(0);
        if (!LITTLE_ENDIAN) {
            decompressedLength = Long.reverseBytes(decompressedLength);
        }

        if (decompressedLength == 0L) {
            // shortcut for empty buffer
            compressedBuffer.close();
            return allocator.getEmpty();
        }

        if (decompressedLength == NO_COMPRESSION_LENGTH) {
            // no compression
            return decompressRawBuffer(compressedBuffer);
        }

        // create decompressor lazily
        if (decompressor == null) {
            decompressor = factory.fastDecompressor();
        }

        ByteBuffer compressed = MemoryUtil.directBuffer(compressedBuffer.memoryAddress() + SIZE_OF_UNCOMPRESSED_LENGTH,
            (int)(compressedBuffer.writerIndex() - SIZE_OF_UNCOMPRESSED_LENGTH));

        ArrowBuf decompressedBuffer = allocator.buffer(decompressedLength);
        ByteBuffer decompressed = MemoryUtil.directBuffer(decompressedBuffer.memoryAddress(), (int)decompressedLength);

        decompressor.decompress(compressed, decompressed);
        decompressedBuffer.writerIndex(decompressedLength);

        compressedBuffer.close();
        return decompressedBuffer;
    }

    @Override
    public String getCodecName() {
        return CompressionType.name(CompressionType.LZ4_FRAME);
    }

    /** Process compression by compressing the buffer as is. */
    private static void compressRawBuffer(final ArrowBuf inputBuffer, final ArrowBuf compressedBuffer) {
        compressedBuffer.setLong(0, NO_COMPRESSION_LENGTH);
        compressedBuffer.setBytes(SIZE_OF_UNCOMPRESSED_LENGTH, inputBuffer, 0, inputBuffer.writerIndex());
    }

    /** Process decompression by decompressing the buffer as is. */
    private static ArrowBuf decompressRawBuffer(final ArrowBuf inputBuffer) {
        return inputBuffer.slice(SIZE_OF_UNCOMPRESSED_LENGTH, inputBuffer.writerIndex() - SIZE_OF_UNCOMPRESSED_LENGTH);
    }
}