
package org.knime.core.data.columnar.mapping;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DataValue;
import org.knime.core.data.columnar.domain.NumericDomain;

public abstract class AbstractNumericDomainMapper<V extends DataValue, T extends Number, D extends NumericDomain<T>> //
	implements DomainMapper<D>
{

	private final Class<D> m_columnarDomainType;

	private final Function<V, T> m_cellToNumberConverter;

	private final BiFunction<T, T, D> m_domainCreator;

	private final Function<T, DataCell> m_numberToCellConverter;

	public AbstractNumericDomainMapper(final Class<D> columnarDomainType, final Function<V, T> cellToNumberConverter,
		final BiFunction<T, T, D> domainCreator, final Function<T, DataCell> numberToCellConverter)
	{
		m_columnarDomainType = columnarDomainType;
		m_cellToNumberConverter = cellToNumberConverter;
		m_domainCreator = domainCreator;
		m_numberToCellConverter = numberToCellConverter;
	}

	@Override
	public Class<D> getColumnarDomainType() {
		return m_columnarDomainType;
	}

	@Override
	public final D mapToColumnarDomain(final DataColumnDomain domain) {
		@SuppressWarnings("unchecked")
		final V lower = (V) domain.getLowerBound();
		@SuppressWarnings("unchecked")
		final V upper = (V) domain.getUpperBound();
		final T convertedLower = m_cellToNumberConverter.apply(lower);
		final T convertedUpper = m_cellToNumberConverter.apply(upper);
		return m_domainCreator.apply(convertedLower, convertedUpper);
	}

	@Override
	public final DataColumnDomain mapFromColumnarDomain(final D domain) {
		final T lower = domain.getLowerBound();
		final T upper = domain.getUpperBound();
		final DataCell convertedLower = lower != null ? m_numberToCellConverter.apply(lower) : null;
		final DataCell convertedUpper = upper != null ? m_numberToCellConverter.apply(upper) : null;
		return new DataColumnDomainCreator(convertedLower, convertedUpper).createDomain();
	}
}
