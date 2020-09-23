package org.knime.core.data.columnar;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataTypeConfig;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.RowKeyConfig;
import org.knime.core.data.RowWriteCursor;
import org.knime.core.data.TableBackend;
import org.knime.core.data.container.DataContainerDelegate;
import org.knime.core.data.container.DataContainerSettings;
import org.knime.core.data.container.ILocalDataRepository;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
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
	public RowWriteCursor<? extends ExtensionTable> create(DataTableSpec spec, RowKeyConfig rowKeyConfig,
			final DataContainerSettings settings, Map<Integer, DataTypeConfig> additionalConfigs) {
		try {
			return new ColumnarRowWriteCursor(-1, spec, new ColumnarRowWriteCursorConfig(settings.getInitializeDomain(),
					settings.getMaxDomainValues(), rowKeyConfig), additionalConfigs);
		} catch (IOException e) {
			// TODO
			throw new RuntimeException(e);
		}
	}
}
