package org.knime.core.columnar.arrow.data;

import java.util.function.Supplier;

import org.apache.arrow.vector.FieldVector;
import org.knime.core.columnar.NullableColumnData;

public interface ArrowData<F extends FieldVector> extends Supplier<F>, NullableColumnData {
// NB: Marker interface
}
