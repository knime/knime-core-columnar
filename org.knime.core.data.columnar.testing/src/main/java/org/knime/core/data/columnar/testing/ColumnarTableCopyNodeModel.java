
package org.knime.core.data.columnar.testing;

import java.io.File;
import java.io.IOException;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.v2.CustomKeyRowContainer;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.value.DoubleValueFactory.DoubleWriteValue;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

public class ColumnarTableCopyNodeModel extends NodeModel {

    protected ColumnarTableCopyNodeModel() {
        super(1, 1);
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

    }

    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {
        return inSpecs;
    }

    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
        throws Exception {
        final boolean newAPI = true;

        if (newAPI) {

            // ### New API:
            try (final RowCursor readCursor = inData[0].cursor();
                    final CustomKeyRowContainer writeCursor = exec.createRowContainer(inData[0].getDataTableSpec())) {
                // can actually be parallelized later
                while (readCursor.canPoll()) {
                    readCursor.poll();
                    writeCursor.setRowKey(readCursor.getRowKeyValue().getString());
                    for (int i = 0; i < readCursor.getNumColumns(); i++) {
                        writeCursor.<DoubleWriteValue> getWriteValue(i).setValue(readCursor.getValue(i));
                    }
                    writeCursor.push();
                }
                return new BufferedDataTable[]{writeCursor.finish()};
            }
        }

        // ### Old API:

        final int numColumns = inData[0].getDataTableSpec().getNumColumns();
        // nice and short API.
        final BufferedDataContainer container = exec.createDataContainer(inData[0].getSpec());
        final RowCursor iterator = inData[0].cursor();

        final DoubleValue[] readValue = new DoubleValue[numColumns];
        final DoubleCell[] outCells = new DoubleCell[numColumns];
        // remember value outside of loop
        for (int i = 0; i < numColumns; ++i) {
            readValue[i] = iterator.getValue(i);
        }

        while (iterator.canPoll()) {
            iterator.poll();
            for (int i = 0; i < numColumns; ++i) {
                outCells[i] = new DoubleCell(readValue[i].getDoubleValue());
            }
            container.addRowToTable(new DefaultRow(iterator.getRowKeyValue().getString(), outCells));
        }
        container.close();
        return new BufferedDataTable[]{container.getTable()};
    }

    @Override
    protected void reset() {
        // Do we have something to do here?
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {

    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {

    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {

    }

}
