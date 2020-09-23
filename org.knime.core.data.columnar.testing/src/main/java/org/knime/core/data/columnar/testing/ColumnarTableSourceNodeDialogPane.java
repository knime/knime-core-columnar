package org.knime.core.data.columnar.testing;

import static org.knime.core.data.columnar.testing.ColumnarTableSourceNodeModel.createNrColsModel;
import static org.knime.core.data.columnar.testing.ColumnarTableSourceNodeModel.createNrRowsModel;
import static org.knime.core.data.columnar.testing.ColumnarTableSourceNodeModel.createUseArrowModel;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumberEdit;

/**
 * @author Bernd Wiswedel, KNIME GmbH, Konstanz, Germany
 */
final class ColumnarTableSourceNodeDialogPane extends DefaultNodeSettingsPane {

	ColumnarTableSourceNodeDialogPane() {
		addDialogComponent(new DialogComponentBoolean(createUseArrowModel(), "Use Arrow Backend (vs. Use 'old' backend)"));
		addDialogComponent(new DialogComponentNumberEdit(createNrRowsModel(), "Number of Rows: "));
		addDialogComponent(new DialogComponentNumberEdit(createNrColsModel(), "Number of Columns: "));
	}
}