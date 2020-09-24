
package org.knime.core.data.columnar.mapping;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.knime.core.columnar.data.ColumnDataSpec;
import org.knime.core.data.DataColumnDomain;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DataTypeConfig;
import org.knime.core.data.RowKeyConfig;
import org.knime.core.data.columnar.ColumnType;
import org.knime.core.data.columnar.ColumnarDataTableSpec;
import org.knime.core.data.columnar.types.RowKeyColumnType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

// TODO: used to be package private, but we need public access on the serializer
public class MappedColumnarDataTableSpec implements ColumnarDataTableSpec {

	private final DataTableSpec m_source;

	private final ColumnType<?, ?> m_rowKeyType;

	private final Map<DataType, DataTypeMapper> m_mapping;

	private final Map<Integer, DataTypeConfig> m_configs;

	private final RowKeyConfig m_rowKeyConfig;

	/**
	 * @param configs Column-wise (sparse) type configurations.
	 */
	MappedColumnarDataTableSpec(final DataTableSpec source, final RowKeyConfig rowKeyConfig,
			final Map<DataType, DataTypeMapper> mapping, final Map<Integer, DataTypeConfig> configs) {
		m_source = source;
		// TODO: auto row key
		m_rowKeyType = new RowKeyColumnType(rowKeyConfig);
		m_rowKeyConfig = rowKeyConfig;
		m_mapping = mapping;
		m_configs = configs;
	}

	@Override
	public DataTableSpec getSourceSpec() {
		return m_source;
	}

	@Override
	public int getNumColumns() {
		// +1 for rowKey.
		return m_source.getNumColumns() + 1;
	}

	@Override
	public ColumnDataSpec getColumnDataSpec(final int index) {
		return getColumnType(index).getColumnDataSpec();
	}

	@Override
	public RowKeyConfig getRowKeyConfig() {
		return m_rowKeyConfig;
	}

	@Override
	public ColumnType<?, ?> getColumnType(final int index) {
		// TODO: do we want to cache the column types for very wide tables?
		if (index != 0) {
			// Subtract offset of row key.
			final DataTypeMapper mapper = m_mapping.get(m_source.getColumnSpec(index - 1).getType());
			final DataTypeConfig config = m_configs.get(index);
			if (config != null) {
				// It must be ensured by the client that mappers and configs match.
				@SuppressWarnings("unchecked")
				final ConfigurableDataTypeMapper<DataTypeConfig> configurableMapper = (ConfigurableDataTypeMapper<DataTypeConfig>) mapper;
				return configurableMapper.createColumnType(config);
			}
			return mapper.createColumnType();
		}
		return m_rowKeyType;
	}

	@Override
	public ColumnarDataTableSpec withUpdatedSourceDomains(final Map<Integer, DataColumnDomain> updatedSourceDomains) {
		final DataColumnSpec[] updatedColumnSpecs = new DataColumnSpec[m_source.getNumColumns()];
		for (int i = 0; i < updatedColumnSpecs.length; i++) {
			final DataColumnSpec sourceColumnSpec = m_source.getColumnSpec(i);
			final DataColumnDomain updatedColumnDomain = updatedSourceDomains.get(i);
			if (updatedColumnDomain != null) {
				final DataColumnSpecCreator updatedColumnSpecCreator = new DataColumnSpecCreator(sourceColumnSpec);
				updatedColumnSpecCreator.setDomain(updatedColumnDomain);
				updatedColumnSpecs[i] = updatedColumnSpecCreator.createSpec();
			} else {
				updatedColumnSpecs[i] = sourceColumnSpec;
			}
		}
		final DataTableSpec updatedSourceSpec = new DataTableSpec(updatedColumnSpecs);
		return new MappedColumnarDataTableSpec(updatedSourceSpec, m_rowKeyConfig, m_mapping, m_configs);
	}

	public static final class Serializer {

		private static final String CFG_ROW_KEY_CONFIG = "row_key_config";

		private static final String CFG_KEY_MAPPED_DATA_TYPES = "mapped_data_types";

		private static final String CFG_KEY_MAPPER_VERSIONS = "mapper_versions";

		private static final String CFG_KEY_COLUMN_CONFIGS = "column_configs";

		private Serializer() {
		}

		public static void save(final MappedColumnarDataTableSpec spec, final NodeSettingsWO settings) {
			settings.addString(CFG_ROW_KEY_CONFIG, spec.getRowKeyConfig().name());

			final Map<DataType, DataTypeMapper> mapping = spec.m_mapping;
			// TODO make sure mapping comprises all element types of all collections in all
			// nestings
			settings.addDataTypeArray(CFG_KEY_MAPPED_DATA_TYPES,
					mapping.keySet().toArray(new DataType[mapping.size()]));
			final NodeSettingsWO mapperVersions = settings.addNodeSettings(CFG_KEY_MAPPER_VERSIONS);
			for (final Entry<DataType, DataTypeMapper> entry : mapping.entrySet()) {
				mapperVersions.addInt(entry.getKey().getName(), entry.getValue().getVersion());
			}

			final Map<Integer, DataTypeConfig> configs = spec.m_configs;
			final NodeSettingsWO allConfigsSettings = settings.addNodeSettings(CFG_KEY_COLUMN_CONFIGS);
			for (final Entry<Integer, DataTypeConfig> entry : configs.entrySet()) {
				final DataTypeConfig config = entry.getValue();
				if (config instanceof StringTypeConfig) {
					final int columnIndex = entry.getKey();
					final NodeSettingsWO singleConfigSettings = allConfigsSettings
							.addNodeSettings(Integer.toString(columnIndex));
					new StringTypeConfig.StringTypeConfigSerializer().save((StringTypeConfig) config,
							singleConfigSettings);
				} else {
					// TODO: implement in extension-based way
					throw new IllegalStateException(
							"Serialization is only implemented for string configs at the moment.");
				}
			}
		}

		public static MappedColumnarDataTableSpec load(final DataTableSpec source, final NodeSettingsRO settings)
				throws InvalidSettingsException {
			final RowKeyConfig rowKeyConfig = RowKeyConfig.valueOf(settings.getString(CFG_ROW_KEY_CONFIG));

			final Map<DataType, DataTypeMapper> mapping = new HashMap<>();
			final DataType[] mappedTypes = settings.getDataTypeArray(CFG_KEY_MAPPED_DATA_TYPES);
			final NodeSettingsRO mapperVersions = settings.getNodeSettings(CFG_KEY_MAPPER_VERSIONS);
			for (final DataType type : mappedTypes) {
				final int version = mapperVersions.getInt(type.getName());
				// TODO for each collectionn type, make sure to load the mapper with the right
				// element type version recursively
				mapping.put(type, DataTypeMapperRegistry.getColumnTypeMapper(type, version));
			}

			final NodeSettingsRO allConfigsSettings = settings.getNodeSettings(CFG_KEY_COLUMN_CONFIGS);
			final Map<Integer, DataTypeConfig> configs;
			final int numConfigs = allConfigsSettings.getChildCount();
			if (numConfigs > 0) {
				configs = new HashMap<>(numConfigs);
				for (int i = 0; i < numConfigs; i++) {
					final NodeSettingsRO singleConfigSettings = (NodeSettingsRO) allConfigsSettings.getChildAt(i);
					final int columnIndex = Integer.parseInt(singleConfigSettings.getKey());
					// TODO: implement in extension-based way
					final StringTypeConfig config = new StringTypeConfig.StringTypeConfigSerializer()
							.load(singleConfigSettings);
					configs.put(columnIndex, config);
				}
			} else {
				configs = Collections.emptyMap();
			}

			return new MappedColumnarDataTableSpec(source, rowKeyConfig, mapping, configs);
		}
	}
}
