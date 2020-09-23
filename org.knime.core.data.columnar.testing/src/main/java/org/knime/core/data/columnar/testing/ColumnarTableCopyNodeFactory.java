package org.knime.core.data.columnar.testing;

import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;

public class ColumnarTableCopyNodeFactory extends NodeFactory<ColumnarTableCopyNodeModel> {

	@Override
	public ColumnarTableCopyNodeModel createNodeModel() {
		return new ColumnarTableCopyNodeModel();
	}

	@Override
	protected int getNrNodeViews() {
		return 0;
	}

	@Override
	public NodeView<ColumnarTableCopyNodeModel> createNodeView(int viewIndex,
			ColumnarTableCopyNodeModel nodeModel) {
		return null;
	}

	@Override
	protected boolean hasDialog() {
		return false;
	}

	@Override
	protected NodeDialogPane createNodeDialogPane() {
		return null;
	}

}
