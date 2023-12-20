/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Mar 15, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable.ColumnarMapperFactory;
import org.knime.core.data.columnar.table.virtual.NullableValues.NullableReadValue;
import org.knime.core.data.columnar.table.virtual.NullableValues.NullableWriteValue;
import org.knime.core.data.columnar.table.virtual.persist.Persistor;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.filestore.internal.NotInWorkflowWriteFileStoreHandler;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.NativeNodeContainer;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;

/**
 * Creates mappers that map from a sub-type ValueFactory to a ValueFactory of a super type.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class TableCasterFactory implements ColumnarMapperFactory {

    private final List<ColumnCasterFactory> m_casts;

    private final IDataRepository m_dataRepo;

    enum CastOperation {
        /**
         * Maps via DataValue and avoids materializing the DataCell.
         */
        UPCAST((r, w) -> () -> w.setReadValue(r)),
        /**
         * Maps by materializing the DataCell. Needed for downcasts.
         */
        DOWNCAST((r, w) -> () -> w.setDataCell(r.getDataCell()));

        private final BiFunction<NullableReadValue, NullableWriteValue, Runnable> m_casterFactory;

        private CastOperation(
            final BiFunction<NullableReadValue, NullableWriteValue, Runnable> casterFactory) {
            m_casterFactory = casterFactory;
        }

        Runnable createCaster(final NullableReadValue readValue,
            final NullableWriteValue writeValue) {
            return m_casterFactory.apply(readValue, writeValue);
        }
    }

    record ColumnCasterFactory(DataColumnSpec outputSpec, UntypedValueFactory inputValueFactory,
        UntypedValueFactory outputValueFactory, CastOperation castOperation) {
        Runnable createCaster(final ReadAccess readAccess, final WriteAccess writeAccess) {
            var readValue = inputValueFactory.createNullableReadValue(readAccess);
            var writeValue = outputValueFactory.createNullableWriteValue(writeAccess);
            return castOperation.createCaster(readValue, writeValue);
        }
    }

    TableCasterFactory(final List<ColumnCasterFactory> casts, final IDataRepository dataRepo) {
        m_casts = casts;
        m_dataRepo = dataRepo;
    }


    @Override
    public Runnable createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs) {
        initOutputValueFactoriesForWriting();
        var casters = IntStream.range(0, m_casts.size())//
            .mapToObj(i -> m_casts.get(i).createCaster(inputs[i], outputs[i]))//
            .toList();
        return () -> casters.forEach(Runnable::run);
    }

    private void initOutputValueFactoriesForWriting() {
        var fsHandler = getFsHandler();
        for (var cast : m_casts) {
            cast.outputValueFactory().initFsHandler(fsHandler);
        }
    }

    @Override
    public ColumnarValueSchema getOutputSchema() {
        var spec = new DataTableSpec(//
            m_casts.stream()//
                .map(ColumnCasterFactory::outputSpec)//
                .toArray(DataColumnSpec[]::new)//
        );
        var valueFactories = m_casts.stream()//
            .map(ColumnCasterFactory::outputValueFactory)//
            .map(UntypedValueFactory::getValueFactory)//
            .toArray(ValueFactory<?, ?>[]::new);
        return ColumnarValueSchemaUtils.create(spec, valueFactories);
    }

    /**
     * Persistor for TableCasterFactory.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public static final class TableCasterFactoryPersistor implements Persistor<TableCasterFactory> {

        private static final String CFG_CAST_OPERATIONS = "castOperations";

        private static final String CFG_OUTPUT_SPEC = "outputSpec";

        private static final String CFG_OUTPUT_VALUE_FACTORIES = "outputValueFactories";

        private static final String CFG_INPUT_VALUE_FACTORIES = "inputValueFactories";

        @Override
        public TableCasterFactory load(final NodeSettingsRO settings, final LoadContext ctx)
            throws InvalidSettingsException {
            var inputValueFactorySettings = settings.getNodeSettings(CFG_INPUT_VALUE_FACTORIES);
            var outputValueFactorySettings = settings.getNodeSettings(CFG_OUTPUT_VALUE_FACTORIES);
            var outputSpec = DataTableSpec.load(settings.getNodeSettings(CFG_OUTPUT_SPEC));
            var castOperations = Stream.of(settings.getStringArray(CFG_CAST_OPERATIONS))//
                    .map(CastOperation::valueOf)//
                    .toArray(CastOperation[]::new);
            var casts = new ArrayList<ColumnCasterFactory>();
            var dataRepository = ctx.getDataRepository();
            for (int i = 0; i < outputSpec.getNumColumns(); i++) {
                var key = Integer.toString(i);
                var inputValueFactory = ValueFactoryUtils
                    .loadValueFactory(inputValueFactorySettings.getNodeSettings(key), dataRepository);
                // during load we don't have access to a IWriteFileStoreHandler, therefore we only initialize the
                // output ValueFactories during #createMapper where we might be able to obtain a filestore handler
                // from the NodeContext
                var outputValueFactory = ValueFactoryUtils
                    .loadValueFactory(outputValueFactorySettings.getNodeSettings(key), dataRepository);
                var cast =
                    new ColumnCasterFactory(outputSpec.getColumnSpec(i), new UntypedValueFactory(inputValueFactory),
                        new UntypedValueFactory(outputValueFactory), castOperations[i]);
                casts.add(cast);
            }
            return new TableCasterFactory(casts, dataRepository);
        }

        @Override
        public void save(final TableCasterFactory factory, final NodeSettingsWO settings) {
            var inputValueFactorySettings = settings.addNodeSettings(CFG_INPUT_VALUE_FACTORIES);
            var outputValueFactorySettings = settings.addNodeSettings(CFG_OUTPUT_VALUE_FACTORIES);
            int i = 0;
            factory.getOutputSchema().getSourceSpec().save(settings.addNodeSettings(CFG_OUTPUT_SPEC));
            for (var mapping : factory.m_casts) {
                String key = Integer.toString(i);
                ValueFactoryUtils.saveValueFactory(mapping.inputValueFactory().getValueFactory(),
                    inputValueFactorySettings.addNodeSettings(key));
                ValueFactoryUtils.saveValueFactory(mapping.outputValueFactory().getValueFactory(),
                    outputValueFactorySettings.addNodeSettings(key));
                i++;
            }
            settings.addStringArray(CFG_CAST_OPERATIONS,//
                factory.m_casts.stream()//
                .map(ColumnCasterFactory::castOperation)//
                .map(CastOperation::name)//
                .toArray(String[]::new));
        }
    }

    private IWriteFileStoreHandler getFsHandler() {
        var nodeContext = NodeContext.getContext();
        if (nodeContext == null) {
            // happens in a view
            return createAndRegisterNewHandler();
        }
        var nodeContainer = nodeContext.getNodeContainer();
        if (nodeContainer instanceof NativeNodeContainer nativeContainer) {
            var fsHandler = nativeContainer.getNode().getFileStoreHandler();
            if (fsHandler instanceof IWriteFileStoreHandler writeFsHandler) {
                return writeFsHandler;
            } else {
                // observed in the old TableView but it's not clear why that would happen
                return createAndRegisterNewHandler();
            }
        }
        throw new IllegalStateException(
            "The current node is not a native node and should therefore not create a table. "
                + "This is likely caused by an incorrect NodeContext.");
    }

    private IWriteFileStoreHandler createAndRegisterNewHandler() {
        var fsHandler = NotInWorkflowWriteFileStoreHandler.create();
        fsHandler.addToRepository(m_dataRepo);
        return fsHandler;
    }

}
