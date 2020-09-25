package org.knime.core.data.columnar;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataTypeConfig;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.RowContainerCustomKey;
import org.knime.core.data.RowKeyConfig;
import org.knime.core.data.RowKeyValue;
import org.knime.core.data.RowWriteCursor;
import org.knime.core.data.TableBackend;
import org.knime.core.data.columnar.mapping.DataTypeMapperRegistry;
import org.knime.core.data.container.DataContainerDelegate;
import org.knime.core.data.container.DataContainerSettings;
import org.knime.core.data.container.ILocalDataRepository;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.values.RowKeyWriteValue;
import org.knime.core.data.values.WriteValue;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExtensionTable;

public class ColumnarTableBackend implements TableBackend {

	@Override
	public DataContainerDelegate create(final DataTableSpec spec, final DataContainerSettings settings,
			final IDataRepository repository, final ILocalDataRepository localRepository,
			final IWriteFileStoreHandler fileStoreHandler) {
		try {
			return new ColumnarDataContainerDelegate(spec, new ColumnarRowWriteCursor(repository.generateNewID(), spec,
					new ColumnarRowWriteCursorConfig(settings), Collections.emptyMap()));
		} catch (IOException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean supports(final DataTableSpec spec) {
		for (DataColumnSpec colSpec : spec) {
			if (!DataTypeMapperRegistry.supports(colSpec.getType())) {
				return false;
			}
		}
		return true;
	}

	@Override
	public RowContainerCustomKey create(ExecutionContext context, DataTableSpec spec, DataContainerSettings settings,
			Map<Integer, DataTypeConfig> additionalConfigs) {
		try {
			final ColumnarRowWriteCursor cursor = new ColumnarRowWriteCursor(-1, spec,
					new ColumnarRowWriteCursorConfig(settings.getInitializeDomain(), settings.getMaxDomainValues(),
							RowKeyConfig.CUSTOM),
					additionalConfigs);
			return new DefaultRowContainerCustomKey(context, cursor);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	static final class DefaultRowContainerCustomKey implements RowContainerCustomKey {

		private final RowWriteCursor<? extends ExtensionTable> m_container;

		private final ExecutionContext m_context;

		public DefaultRowContainerCustomKey(final ExecutionContext context,
				final RowWriteCursor<? extends ExtensionTable> container) {
			m_context = context;
			m_container = container;
		}

		@Override
		public void setRowKey(final String key) {
			m_container.<RowKeyWriteValue>getWriteValue(-1).setRowKey(key);
		}

		@Override
		public void setRowKey(final RowKeyValue rowKey) {
			m_container.<RowKeyWriteValue>getWriteValue(-1).setRowKey(rowKey);
		}

		@Override
		public void push() {
			m_container.push();
		}

		@Override
		public <W extends WriteValue<?>> W getWriteValue(final int index) {
			return m_container.getWriteValue(index);
		}

		@Override
		public int getNumColumns() {
			return m_container.getNumColumns();
		}

		@Override
		public void close() {
			m_container.close();
		}

		@Override
		public BufferedDataTable finish() throws IOException {
			return m_container.finish().create(m_context);
		}

		@Override
		public void setMissing(final int index) {
			m_container.setMissing(index);
		}

	}

}
