package org.knime.core.data.columnar.testing;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

public class ColumnarTableSourceNodeFactory extends NodeFactory<ColumnarTableSourceNodeModel> {

	@Override
	public ColumnarTableSourceNodeModel createNodeModel() {
		return new ColumnarTableSourceNodeModel();
	}

	@Override
	protected int getNrNodeViews() {
		return 0;
	}

	@Override
	public NodeView<ColumnarTableSourceNodeModel> createNodeView(final int viewIndex,
			final ColumnarTableSourceNodeModel nodeModel) {
		return null;
	}

	@Override
	protected boolean hasDialog() {
		return true;
	}

	@Override
	protected NodeDialogPane createNodeDialogPane() {
		return new ColumnarTableSourceNodeDialogPane();
	}

}
