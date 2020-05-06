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

	private final AtomicInteger m_refCounter = new AtomicInteger(1);
	private final VarCharVector m_vector;

	private int m_numValues;
	private int m_maxCapacity;

	public ArrowVarCharData(BufferAllocator allocator) {
		m_vector = new VarCharVector("VarCharVector", allocator);
	}

	public ArrowVarCharData(VarCharVector vector) {
		m_vector = vector;
		m_numValues = vector.getValueCount();
	}

	@Override
	public String getString(int index) {
		try {
			return DECODER.decode(ByteBuffer.wrap(m_vector.get(index))).toString();
		} catch (CharacterCodingException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setString(int index, String value) {
		try {
			ByteBuffer encode = ENCODER.encode(CharBuffer.wrap(value.toCharArray()));
			m_vector.setSafe(index, encode.array(), 0, encode.limit());
		} catch (CharacterCodingException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setMissing(int index) {
		// TODO we can speed things likely up directly accessing validity buffer
		BitVectorHelper.unsetBit(m_vector.getValidityBuffer(), index);
	}

	@Override
	public boolean isMissing(int index) {
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
	public void setNumValues(int numValues) {
		m_vector.setValueCount(numValues);
		m_numValues = numValues;
	}

	@Override
	public VarCharVector get() {
		return m_vector;
	}

	@Override
	public void ensureCapacity(int chunkSize) {
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
		return (int) (m_vector.getDataBuffer().capacity() + //
				m_vector.getValidityBuffer().capacity() + //
				m_vector.getOffsetBuffer().capacity());
	}

	@Override
	public String toString() {
		return m_vector.toString();
	}

}
