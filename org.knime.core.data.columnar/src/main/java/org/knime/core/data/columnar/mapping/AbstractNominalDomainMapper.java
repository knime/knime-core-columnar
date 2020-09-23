
package org.knime.core.data.columnar.mapping;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnDomainCreator;
import org.knime.core.data.DataValue;
import org.knime.core.data.columnar.domain.NominalDomain;

public abstract class AbstractNominalDomainMapper<V extends DataValue, T, D extends NominalDomain<T>> //
	implements DomainMapper<D>
{

	private final Class<D> m_columnarDomainType;

	private final Function<V, T> m_cellToElementConverter;

	private final Function<Set<T>, D> m_domainCreator;

	private final Function<T, DataCell> m_elementToCellConverter;

	public AbstractNominalDomainMapper(final Class<D> columnarDomainType, final Function<V, T> cellToElementConverter,
		final Function<Set<T>, D> domainCreator, final Function<T, DataCell> converter)
	{
		m_columnarDomainType = columnarDomainType;
		m_cellToElementConverter = cellToElementConverter;
		m_domainCreator = domainCreator;
		m_elementToCellConverter = converter;
	}

	@Override
	public Class<D> getColumnarDomainType() {
		return m_columnarDomainType;
	}

	@Override
	public final D mapToColumnarDomain(final DataColumnDomain domain) {
		final Set<DataCell> values = domain.getValues();
		// Preserve order
		final Set<T> convertedValues = new LinkedHashSet<>(values.size());
		for (final DataCell value : values) {
			@SuppressWarnings("unchecked")
			final V cast = (V) value;
			convertedValues.add(m_cellToElementConverter.apply(cast));
		}
		return m_domainCreator.apply(convertedValues);
	}

	@Override
	public final DataColumnDomain mapFromColumnarDomain(final D domain) {
		final Set<T> values = domain.getValues();
		final Set<DataCell> convertedValues;
		if (values != null) {
			// Preserve order
			convertedValues = new LinkedHashSet<>(values.size());
			for (final T value : values) {
				convertedValues.add(m_elementToCellConverter.apply(value));
			}
		}
		else {
			convertedValues = null;
		}
		return new DataColumnDomainCreator(convertedValues).createDomain();
	}
}
