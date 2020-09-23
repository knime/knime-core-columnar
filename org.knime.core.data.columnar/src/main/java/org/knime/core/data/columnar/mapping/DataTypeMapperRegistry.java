
package org.knime.core.data.columnar.mapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DataTypeConfig;
import org.knime.core.data.RowKeyConfig;
import org.knime.core.data.columnar.ColumnarDataTableSpec;
import org.knime.core.data.columnar.domain.ColumnarDomain;
import org.knime.core.data.columnar.domain.NominalDomain;
import org.knime.core.data.columnar.domain.NumericDomain;

public final class DataTypeMapperRegistry {

	private DataTypeMapperRegistry() {
	}

	private static final Map<DataType, List<DataTypeMapper>> REGISTRY = new HashMap<>();

	static {
		// TODO: extension point
		register(new IntTypeMapper());
		register(new DoubleTypeMapper());
		register(new LongTypeMapper());
		register(new StringTypeMapper());

		// Sort from latest version to lowest version
		for (final Entry<DataType, List<DataTypeMapper>> entry : REGISTRY.entrySet()) {
			Collections.sort(entry.getValue());
		}
	}

	public static ColumnarDataTableSpec convert(final DataTableSpec spec, final RowKeyConfig rowKeyConfig,
			final Map<Integer, DataTypeConfig> configs, final Map<DataType, Integer> versions) {
		return convertInternal(spec, rowKeyConfig, configs, Objects.requireNonNull(versions));
	}

	public static ColumnarDataTableSpec convert(final DataTableSpec spec, final RowKeyConfig rowKeyConfig,
			final Map<Integer, DataTypeConfig> configs) {
		return convertInternal(spec, rowKeyConfig, configs, null);
	}

	private static ColumnarDataTableSpec convertInternal(final DataTableSpec spec, final RowKeyConfig rowKeyConfig,
			final Map<Integer, DataTypeConfig> configs, final Map<DataType, Integer> versions) {
		final Map<DataType, DataTypeMapper> mapping = new HashMap<>();
		for (int i = 0; i < spec.getNumColumns(); i++) {
			final DataColumnSpec columnSpec = spec.getColumnSpec(i);
			final DataType type = columnSpec.getType();
			if (!mapping.containsKey(type)) {
				final DataTypeMapper mapper = versions != null //
						? getColumnTypeMapper(type, versions.get(type)) //
						: getColumnTypeMapper(type);
				mapping.put(type, mapper);
			}
		}
		// Check that the provided configs match the column types.
		for (final Entry<Integer, DataTypeConfig> entry : configs.entrySet()) {
			final Integer i = entry.getKey();
			final DataColumnSpec columnSpec = spec.getColumnSpec(i);
			final DataType type = columnSpec.getType();
			final DataTypeMapper mapper = mapping.get(type);
			final DataTypeConfig config = entry.getValue();
			if (mapper instanceof ConfigurableDataTypeMapper) {
				final Class<?> configClass = ((ConfigurableDataTypeMapper<?>) mapper).getConfigClass();
				if (!configClass.isInstance(config)) {
					throw new IllegalArgumentException(
							"Column " + columnSpec.getName() + " at index " + i + " expects a configuration of type "
									+ configClass + " but was tried to be configured using a configuration of type "
									+ config.getClass() + ".");
				}
			} else {
				throw new IllegalArgumentException("Column " + columnSpec.getName() + " at index " + i
						+ " is not configurable but was tried to be configured using a configuration of type "
						+ config.getClass() + ".");
			}
		}
		return new MappedColumnarDataTableSpec(spec, rowKeyConfig, mapping, configs);
	}

	// TODO: move somewhere else, or consolidate with convert(..) and possibly
	// make domain part of columnar spec
	public static Map<Integer, ColumnarDomain> extractInitialDomains(final DataTableSpec spec) {
		// TODO: handle different row key types somewhere centralized
		final Map<DataType, DataTypeMapper> mapping = new HashMap<>();
		// FIXME: key indices off by one if custom row key
		final Map<Integer, ColumnarDomain> initialDomains = new HashMap<>();
		for (int i = 0; i < spec.getNumColumns(); i++) {
			final DataColumnSpec columnSpec = spec.getColumnSpec(i);
			final DataType type = columnSpec.getType();
			if (!mapping.containsKey(type)) {
				final DataTypeMapper mapper = getColumnTypeMapper(type);
				mapping.put(type, mapper);
			}
			final DataTypeMapper mapper = mapping.get(type);
			if (mapper instanceof DomainDataTypeMapper) {
				final DomainMapper<?> domainMapper = ((DomainDataTypeMapper<?>) mapper).getDomainMapper();
				final Class<?> columnarDomainType = domainMapper.getColumnarDomainType();
				final DataColumnDomain domain = columnSpec.getDomain();
				final boolean needsMap;
				// We only need to map non-empty domains. Empty domains will otherwise
				// be created anyway.
				if (NumericDomain.class.isAssignableFrom(columnarDomainType)) {
					needsMap = domain.hasBounds();
				} else if (NominalDomain.class.isAssignableFrom(columnarDomainType)) {
					needsMap = domain.hasValues();
				} else {
					throw new IllegalStateException("Unknown domain type: " + columnarDomainType);
				}
				if (needsMap) {
					final ColumnarDomain columnarDomain = domainMapper.mapToColumnarDomain(domain);
					initialDomains.put(i + 1, columnarDomain);
				}
			}
		}
		return initialDomains;
	}

	public static DataTypeMapper getColumnTypeMapper(final DataType type) {
		checkAvailability(type);
		return REGISTRY.get(type).get(0);
	}

	public static DataTypeMapper getColumnTypeMapper(final DataType type, final int version) {
		checkAvailability(type);
		for (final DataTypeMapper candidate : REGISTRY.get(type)) {
			if (candidate.getVersion() == version) {
				return candidate;
			}
		}
		throw new IllegalArgumentException(
				"No ColumnTypeFactory found for type " + type + " with version " + version + ".");
	}

	public static boolean supports(final DataType type) {
		return REGISTRY.containsKey(type);
	}

	private static void checkAvailability(final DataType type) {
		if (!supports(type)) {
			throw new IllegalArgumentException("No ColumnTypeFactory found for type" + type + ".");
		}
	}

	private static void register(final DataTypeMapper factory) {
		final DataType type = factory.getDataType();

		List<DataTypeMapper> factories = REGISTRY.get(type);
		if (factories == null) {
			REGISTRY.put(type, factories = new ArrayList<>());
		}

		for (final DataTypeMapper registered : factories) {
			if (registered.getVersion() == factory.getVersion()) {
				// TODO what to do in this scenario? Abort? Ignore?
				throw new IllegalStateException("ValueFactory with same version and dest type already present.");
			}
		}
		factories.add(factory);
	}
}
