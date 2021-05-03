
package org.knime.core.data.columnar.testing;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.v2.RowContainer;
import org.knime.core.data.v2.RowWrite;
import org.knime.core.data.v2.RowWriteCursor;
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
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelLongBounded;
import org.knime.core.node.util.StringFormat;

public class ColumnarTableSourceNodeModel extends NodeModel {

    private final SettingsModelLongBounded m_nrRowsModel = createNrRowsModel();

    private final SettingsModelIntegerBounded m_nrColsModel = createNrColsModel();

    private final SettingsModelBoolean m_useArrowModel = createUseArrowModel();

    private DataTableSpec m_spec;

    protected ColumnarTableSourceNodeModel() {
        super(0, 1);
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
        final DataColumnSpec[] specs = new DataColumnSpec[m_nrColsModel.getIntValue()];
        for (int i = 0; i < specs.length; i++) {
            specs[i] = new DataColumnSpecCreator("DoubleCell: " + i, DoubleCell.TYPE).createSpec();
        }
        return new DataTableSpec[]{m_spec = new DataTableSpec(specs)};
    }

    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
        throws Exception {
        final boolean arrow = m_useArrowModel.getBooleanValue();
        final long startTime = System.currentTimeMillis();
        BufferedDataTable bdt;

        final Random r = new Random(1);

        if (arrow) {
            // TODO close etc.
            final int nrCols = m_nrColsModel.getIntValue();
            final long nrRows = m_nrRowsModel.getLongValue();
            try (final RowContainer container = exec.createRowContainer(m_spec);
                    final RowWriteCursor cursor = container.createCursor()) {
                for (long i = 0; i < nrRows; i++) {
                    final RowWrite row = cursor.forward();
                    row.setRowKey("Row" + i);
                    for (int j = 0; j < nrCols; j++) {
                        row.<DoubleWriteValue> getWriteValue(j).setDoubleValue(r.nextDouble());
                    }
                }
                bdt = container.finish();
            }
        } else {
            // OLD API
            final BufferedDataContainer container = exec.createDataContainer(m_spec);
            final DataCell[] cells = new DataCell[m_nrColsModel.getIntValue()];
            for (long i = 0L, size = m_nrRowsModel.getLongValue(); i < size; i++) {
                for (int j = 0; j < cells.length; j++) {
                    cells[j] = new DoubleCell(r.nextDouble());
                }
                final RowKey key = RowKey.createRowKey(i);
                //			if ((i + 1) % 10000 == 0) {
                //				final long iFinal = i;
                //				exec.setProgress((double) i / size, () -> String.format("Row %,d/%,d (\"%s\")", iFinal, size, key.toString()));
                //				exec.checkCanceled();
                //			}
                container.addRowToTable(new DefaultRow(key, cells));
            }
            container.close();
            bdt = container.getTable();
        }
        final long runtime = System.currentTimeMillis() - startTime;
        getLogger().infoWithFormat("Table generator took %s (%,dms)", StringFormat.formatElapsedTime(runtime), runtime);
        return new BufferedDataTable[]{bdt};
    }

    @Override
    protected void reset() {
        // Do we have something to do here?
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_useArrowModel.saveSettingsTo(settings);
        m_nrRowsModel.saveSettingsTo(settings);
        m_nrColsModel.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_useArrowModel.validateSettings(settings);
        m_nrRowsModel.validateSettings(settings);
        m_nrColsModel.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_useArrowModel.loadSettingsFrom(settings);
        m_nrRowsModel.loadSettingsFrom(settings);
        m_nrColsModel.loadSettingsFrom(settings);
    }

    static SettingsModelBoolean createUseArrowModel() {
        return new SettingsModelBoolean("useArrow", true);
    }

    static SettingsModelIntegerBounded createNrColsModel() {
        return new SettingsModelIntegerBounded("nrCols", 32, 0, Integer.MAX_VALUE);
    }

    static SettingsModelLongBounded createNrRowsModel() {
        return new SettingsModelLongBounded("nrRows", 2_500_000L, 0, Long.MAX_VALUE);
    }

}
