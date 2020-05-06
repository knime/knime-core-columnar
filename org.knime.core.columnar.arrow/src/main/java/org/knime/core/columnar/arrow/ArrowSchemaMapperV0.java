package org.knime.core.columnar.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.arrow.data.ArrowBinarySupplementData;
import org.knime.core.columnar.arrow.data.ArrowData;
import org.knime.core.columnar.arrow.data.ArrowDictEncodedStringData;
import org.knime.core.columnar.arrow.data.ArrowDoubleData;
import org.knime.core.columnar.arrow.data.ArrowIntData;
import org.knime.core.columnar.arrow.data.ArrowLongData;
import org.knime.core.columnar.arrow.data.ArrowVarCharData;
import org.knime.core.columnar.data.BinarySupplData;
import org.knime.core.columnar.data.BinarySupplData.BinarySupplDataSpec;
import org.knime.core.columnar.data.DoubleData;
import org.knime.core.columnar.data.DoubleData.DoubleDataSpec;
import org.knime.core.columnar.data.IntData;
import org.knime.core.columnar.data.IntData.IntDataSpec;
import org.knime.core.columnar.data.LongData;
import org.knime.core.columnar.data.LongData.LongDataSpec;
import org.knime.core.columnar.data.StringData;
import org.knime.core.columnar.data.StringData.StringDataSpec;

final class ArrowSchemaMapperV0 implements ArrowSchemaMapper {

	private static long m_id;

	private static long nextUniqueId() {
		return m_id++;
	}

	@Override
	public ArrowColumnDataSpec<?>[] map(ColumnStoreSchema schema) {
		final ArrowColumnDataSpec<?>[] arrowSchema = new ArrowColumnDataSpec<?>[schema.getNumColumns()];
		for (int i = 0; i < schema.getNumColumns(); i++) {
			arrowSchema[i] = getMapping(schema.getColumnDataSpec(i));
		}
		return arrowSchema;
	}

	// TODO different pattern
	private static final ArrowColumnDataSpec<?> getMapping(ColumnDataSpec<?> spec) {
		if (spec instanceof DoubleDataSpec) {
			return new ArrowDoubleDataSpec();
		} else if (spec instanceof StringDataSpec) {
			return new ArrowStringDataSpec((StringDataSpec) spec);
		} else if (spec instanceof IntDataSpec) {
			return new ArrowIntDataSpec();
		} else if (spec instanceof BinarySupplDataSpec) {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			ArrowBinarySupplDataSpec<?> arrowSpec = new ArrowBinarySupplDataSpec((BinarySupplDataSpec<?>) spec);
			return arrowSpec;
		} else if (spec instanceof LongDataSpec) {
			return new ArrowLongDataSpec();
		}
		throw new IllegalArgumentException("ColumnDataSpec " + spec.getClass().getName() + " not supported.");

	}

	static class ArrowLongDataSpec implements ArrowColumnDataSpec<LongData> {
		@Override
		public LongData createEmpty(BufferAllocator allocator) {
			return new ArrowLongData(allocator);
		}

		@Override
		public LongData wrap(FieldVector vector, DictionaryProvider provider) {
			return new ArrowLongData((BigIntVector) vector);
		}
	}

	static class ArrowDoubleDataSpec implements ArrowColumnDataSpec<DoubleData> {
		@Override
		public DoubleData createEmpty(BufferAllocator allocator) {
			return new ArrowDoubleData(allocator);
		}

		@Override
		public DoubleData wrap(FieldVector vector, DictionaryProvider provider) {
			return new ArrowDoubleData((Float8Vector) vector);
		}
	}

	static class ArrowIntDataSpec implements ArrowColumnDataSpec<IntData> {
		@Override
		public IntData createEmpty(BufferAllocator allocator) {
			return new ArrowIntData(allocator);
		}

		@Override
		public IntData wrap(FieldVector vector, DictionaryProvider provider) {
			return new ArrowIntData((IntVector) vector);
		}
	}

	static class ArrowBinarySupplDataSpec<C extends ArrowData<?>> implements ArrowColumnDataSpec<BinarySupplData<C>> {

		private final BinarySupplDataSpec<C> m_spec;
		private ArrowColumnDataSpec<C> m_arrowChunkSpec;

		@SuppressWarnings("unchecked")
		public ArrowBinarySupplDataSpec(BinarySupplDataSpec<C> spec) {
			m_spec = spec;
			m_arrowChunkSpec = (ArrowColumnDataSpec<C>) ArrowSchemaMapperV0.getMapping(m_spec.getChildSpec());
		}

		@Override
		public BinarySupplData<C> createEmpty(BufferAllocator allocator) {
			return new ArrowBinarySupplementData<C>(allocator, m_arrowChunkSpec.createEmpty(allocator));
		}

		@Override
		public BinarySupplData<C> wrap(FieldVector vector, DictionaryProvider provider) {
			@SuppressWarnings("unchecked")
			final ArrowColumnDataSpec<C> arrowSpec = (ArrowColumnDataSpec<C>) getMapping(m_spec.getChildSpec());
			final C wrapped = arrowSpec.wrap((FieldVector) ((StructVector) vector).getChildByOrdinal(0), provider);
			return new ArrowBinarySupplementData<>((StructVector) vector, wrapped);
		}
	}

	static class ArrowStringDataSpec implements ArrowColumnDataSpec<StringData> {

		private final StringDataSpec m_spec;
		private final long id = nextUniqueId();

		public ArrowStringDataSpec(StringDataSpec spec) {
			m_spec = spec;
		}

		@Override
		public StringData createEmpty(BufferAllocator allocator) {
			if (m_spec.isDictEnabled()) {
				return new ArrowDictEncodedStringData(allocator, id);
			} else {
				return new ArrowVarCharData(allocator);
			}
		}

		@Override
		public StringData wrap(FieldVector vector, DictionaryProvider provider) {
			if (m_spec.isDictEnabled()) {
				return new ArrowDictEncodedStringData((IntVector) vector, (VarCharVector) provider
						.lookup(vector.getField().getFieldType().getDictionary().getId()).getVector());
			} else {
				return new ArrowVarCharData((VarCharVector) vector);
			}
		}
	}
}
