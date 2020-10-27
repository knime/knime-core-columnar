package org.knime.core.data.columnar.table;

import java.io.IOException;

import org.knime.core.data.RowKeyValue;
import org.knime.core.data.v2.RowContainer;
import org.knime.core.data.v2.CustomKeyRowContainer;
import org.knime.core.data.v2.RowWriteCursor;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.data.v2.value.CustomRowKeyValueFactory.CustomRowKeyWriteValue;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExtensionTable;

/**
 * Implementation of a {@link RowContainer} with a CustomKey.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 * @since 4.3
 */
public final class CustomKeyColumnarRowContainer implements CustomKeyRowContainer {

    private final RowWriteCursor<? extends ExtensionTable> m_container;

    private final ExecutionContext m_context;

    /**
     * @param context context used to create the {@link RowContainer}
     * @param cursor underlying {@link RowWriteCursor}
     */
    public CustomKeyColumnarRowContainer(final ExecutionContext context,
        final RowWriteCursor<? extends ExtensionTable> cursor) {
        m_context = context;
        m_container = cursor;
    }

    @Override
    public void setRowKey(final String key) {
        m_container.<CustomRowKeyWriteValue> getWriteValue(-1).setRowKey(key);
    }

    @Override
    public void setRowKey(final RowKeyValue rowKey) {
        m_container.<CustomRowKeyWriteValue> getWriteValue(-1).setRowKey(rowKey);
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
    @SuppressWarnings("resource")
    public BufferedDataTable finish() throws IOException {
        return m_container.finish().create(m_context);
    }

    @Override
    public void setMissing(final int index) {
        m_container.setMissing(index);
    }
}