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
package org.knime.core.columnar.arrow.data;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.VarCharVector;
import org.knime.core.columnar.data.StringData;

public class ArrowVarCharData implements StringData, ArrowData<VarCharVector> {

    // Efficiency
    private final CharsetDecoder DECODER = Charset.forName("UTF-8").newDecoder()
        .onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);

    private final CharsetEncoder ENCODER = Charset.forName("UTF-8").newEncoder()
        .onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);

    private synchronized ByteBuffer encode(final String value) {
        try {
            return ENCODER.encode(CharBuffer.wrap(value.toCharArray()));
        } catch (CharacterCodingException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    private synchronized String decode(final byte[] values) {
        try {
            return DECODER.decode(ByteBuffer.wrap(values)).toString();
        } catch (CharacterCodingException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    private final AtomicInteger m_refCounter = new AtomicInteger(1);

    private final VarCharVector m_vector;

    private int m_numValues;

    private int m_maxCapacity;

    public ArrowVarCharData(final BufferAllocator allocator) {
        m_vector = new VarCharVector("VarCharVector", allocator);
    }

    public ArrowVarCharData(final VarCharVector vector) {
        m_vector = vector;
        m_numValues = vector.getValueCount();
    }

    @Override
    public String getString(final int index) {
        return decode(m_vector.get(index));
    }

    @Override
    public void setString(final int index, final String value) {
        final ByteBuffer encode = encode(value);
        m_vector.setSafe(index, encode.array(), 0, encode.limit());
    }

    @Override
    public void setMissing(final int index) {
        // TODO we can speed things likely up directly accessing validity buffer
        BitVectorHelper.unsetBit(m_vector.getValidityBuffer(), index);
    }

    @Override
    public boolean isMissing(final int index) {
        return m_vector.isNull(index);
    }

    @Override
    public int getMaxCapacity() {
        return m_maxCapacity;
    }

    @Override
    public int getNumValues() {
        return m_numValues;
    }

    @Override
    public void setNumValues(final int numValues) {
        m_vector.setValueCount(numValues);
        m_numValues = numValues;
    }

    @Override
    public VarCharVector get() {
        return m_vector;
    }

    @Override
    public void ensureCapacity(final int chunkSize) {
        if (chunkSize > m_maxCapacity) {
            m_vector.allocateNew(chunkSize);
            m_maxCapacity = chunkSize;
        }
    }

    // TODO thread safety for ref-counting
    @Override
    public synchronized void release() {
        if (m_refCounter.decrementAndGet() == 0) {
            m_vector.close();
        }
    }

    @Override
    public synchronized void retain() {
        m_refCounter.getAndIncrement();
    }

    @Override
    public int sizeOf() {
        return (int)(m_vector.getDataBuffer().capacity() + //
            m_vector.getValidityBuffer().capacity() + //
            m_vector.getOffsetBuffer().capacity());
    }

    @Override
    public String toString() {
        return m_vector.toString();
    }

}
