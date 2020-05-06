package org.knime.core.columnar.arrow.data;

import org.apache.arrow.vector.FieldVector;

public interface ArrowDictionaryHolder<F extends FieldVector> {

	F getDictionary();
}
