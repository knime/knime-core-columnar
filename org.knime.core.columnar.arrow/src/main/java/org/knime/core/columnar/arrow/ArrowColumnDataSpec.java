package org.knime.core.columnar.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.knime.core.columnar.ColumnData;

public interface ArrowColumnDataSpec<C extends ColumnData> {
	C createEmpty(BufferAllocator allocator);

	C wrap(FieldVector vector, DictionaryProvider provider);
}
