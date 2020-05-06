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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.knime.core.columnar.data.StringData;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public class ArrowDictEncodedStringData
		implements StringData, ArrowDictionaryHolder<VarCharVector>, ArrowData<IntVector> {

	// TODO configurable 2048 size of dict? hm...
	private final static int INIT_DICT_SIZE = 2048;

	// Efficiency
	private final CharsetDecoder DECODER = Charset.forName("UTF-8").newDecoder()
			.onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);
	private final CharsetEncoder ENCODER = Charset.forName("UTF-8").newEncoder()
			.onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);

	private final AtomicInteger m_refCounter = new AtomicInteger(1);

	private final BiMap<String, Integer> m_inMemDict;
	private final BiMap<Integer, String> m_invInMemDict;

	private VarCharVector m_dictionary;
	private IntVector m_vector;

	private int m_numValues;

	private int m_maxCapacity;

	private int m_runningDictIndex;

	// loaded from disc
	public ArrowDictEncodedStringData(final IntVector vector, final VarCharVector dict) {
		try {
			m_inMemDict = HashBiMap.create(INIT_DICT_SIZE);
			m_invInMemDict = m_inMemDict.inverse();
			m_vector = vector;
			m_numValues = vector.getValueCount();

			if (dict != null) {
				for (int i = 0; i < dict.getValueCount(); i++) {
					final byte[] val = dict.get(i);
					if (val != null)
						m_invInMemDict.put(i, DECODER.decode(ByteBuffer.wrap(val)).toString());
				}
			}
			m_dictionary = dict;
		} catch (Exception e) {
			release();
			// TODO
			throw new RuntimeException(e);
		}
	}

	// on first create
	public ArrowDictEncodedStringData(BufferAllocator allocator, long id) {
		this(new IntVector("Indices",
				new FieldType(false, MinorType.INT.getType(), new DictionaryEncoding(id, false, null)), allocator),
				null);
	}

	@Override
	public String getString(int index) {
		return m_invInMemDict.get(m_vector.get(index));
	}

	@Override
	public void setString(int index, String value) {
		Integer dictIndex = m_inMemDict.get(value);
		if (dictIndex == null) {
			m_inMemDict.put(value, dictIndex = m_runningDictIndex);
			m_runningDictIndex++;
		}
		m_vector.set(index, dictIndex);
	}

	// NB: returns the delta
	@Override
	public VarCharVector getDictionary() {
		// newly created dictionary.
		if (m_dictionary == null) {
			try {
				m_dictionary = new VarCharVector("Dictionary", m_vector.getAllocator());
				m_dictionary.allocateNew(128 * m_invInMemDict.size(), m_invInMemDict.size());
				for (int i = 0; i < m_invInMemDict.size(); i++) {
					final ByteBuffer encode = ENCODER.encode(CharBuffer.wrap(m_invInMemDict.get(i).toCharArray()));
					m_dictionary.set(i, encode.array(), 0, encode.limit());
				}
				m_dictionary.setValueCount(m_inMemDict.size());
			} catch (CharacterCodingException e) {
				// TODO
				throw new RuntimeException(e);
			}
		}
		return m_dictionary;
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
	public IntVector get() {
		return m_vector;
	}

	@Override
	public void ensureCapacity(int chunkSize) {
		if (chunkSize > m_maxCapacity) {
			m_maxCapacity = chunkSize;
			m_vector.allocateNew(chunkSize);
		}
	}

	// TODO thread safety for ref-counting
	@Override
	public void release() {
		if (m_refCounter.decrementAndGet() == 0) {
			m_vector.close();
			if (m_dictionary != null) {
				m_dictionary.close();
			}
		}
	}

	@Override
	public void retain() {
		m_refCounter.getAndIncrement();
	}

	@Override
	public int sizeOf() {
		int sizeOf = m_vector.getBufferSize();
		if (m_dictionary != null) {
			sizeOf += m_dictionary.getBufferSize();
		}
		return sizeOf;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(m_vector.toString());
		if (m_dictionary != null) {
			sb.append(" -> ");
			sb.append(m_dictionary.toString());
		}
		return sb.toString();
	}

}
