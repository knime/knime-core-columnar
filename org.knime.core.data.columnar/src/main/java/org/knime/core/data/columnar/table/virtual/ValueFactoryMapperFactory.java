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
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable.ColumnarMapperWithRowIndexFactory;
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
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperWithRowIndexFactory;

/**
 * Creates mappers that map from a sub-type ValueFactory to a ValueFactory of a super type.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
final class ValueFactoryMapperFactory implements ColumnarMapperWithRowIndexFactory {

    private final List<ColumnMapperFactory> m_mappings;

    private final IDataRepository m_dataRepo;

    enum CastType {
        /**
         * Maps via DataValue and avoids materializing the DataCell.
         */
        UPCAST((r, w) -> i -> w.setReadValue(r)),
        /**
         * Maps by materializing the DataCell. Needed for downcasts.
         */
        DOWNCAST((r, w) -> i -> w.setDataCell(r.getDataCell()));

        private final BiFunction<NullableReadValue, NullableWriteValue, MapperWithRowIndexFactory.Mapper> m_mapperFactory;

        private CastType(
            final BiFunction<NullableReadValue, NullableWriteValue, MapperWithRowIndexFactory.Mapper> mapperFactory) {
            m_mapperFactory = mapperFactory;
        }

        MapperWithRowIndexFactory.Mapper createMapper(final NullableReadValue readValue,
            final NullableWriteValue writeValue) {
            return m_mapperFactory.apply(readValue, writeValue);
        }
    }

    record ColumnMapperFactory(DataColumnSpec outputSpec, UntypedValueFactory inputValueFactory,
        UntypedValueFactory outputValueFactory, CastType mapPath) {
        MapperWithRowIndexFactory.Mapper createMapper(final ReadAccess readAccess, final WriteAccess writeAccess) {
            var readValue = inputValueFactory.createNullableReadValue(readAccess);
            var writeValue = outputValueFactory.createNullableWriteValue(writeAccess);
            return mapPath.createMapper(readValue, writeValue);
        }
    }

    ValueFactoryMapperFactory(final List<ColumnMapperFactory> mappings, final IDataRepository dataRepo) {
        m_mappings = mappings;
        m_dataRepo = dataRepo;
    }


    @Override
    public Mapper createMapperWithRowIndex(final ReadAccess[] inputs, final WriteAccess[] outputs) {
        initOutputValueFactoriesForWriting();
        var mappers = IntStream.range(0, m_mappings.size())//
            .mapToObj(i -> m_mappings.get(i).createMapper(inputs[i], outputs[i]))//
            .toList();
        return r -> mappers.forEach(m -> m.map(r));
    }

    private void initOutputValueFactoriesForWriting() {
        var fsHandler = getFsHandler();
        for (var mapping : m_mappings) {
            mapping.outputValueFactory().initFsHandler(fsHandler);
        }
    }

    @Override
    public ColumnarValueSchema getOutputSchema() {
        var spec = new DataTableSpec(//
            m_mappings.stream()//
                .map(ColumnMapperFactory::outputSpec)//
                .toArray(DataColumnSpec[]::new)//
        );
        var valueFactories = m_mappings.stream()//
            .map(ColumnMapperFactory::outputValueFactory)//
            .map(UntypedValueFactory::getValueFactory)//
            .toArray(ValueFactory<?, ?>[]::new);
        return ColumnarValueSchemaUtils.create(spec, valueFactories);
    }

    /**
     * Persistor for SpecMapperFactory.
     *
     * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
     */
    public static final class ValueFactoryMapperPersistor implements Persistor<ValueFactoryMapperFactory> {

        @Override
        public ValueFactoryMapperFactory load(final NodeSettingsRO settings, final LoadContext ctx)
            throws InvalidSettingsException {
            var inputValueFactorySettings = settings.getNodeSettings("inputValueFactories");
            var outputValueFactorySettings = settings.getNodeSettings("outputValueFactories");
            var outputSpec = DataTableSpec.load(settings.getNodeSettings("outputSpec"));
            var mapPaths = Stream.of(settings.getStringArray("mapPaths")).map(CastType::valueOf).toArray(CastType[]::new);
            var mappings = new ArrayList<ColumnMapperFactory>();
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
                var mapping =
                    new ColumnMapperFactory(outputSpec.getColumnSpec(i), new UntypedValueFactory(inputValueFactory),
                        new UntypedValueFactory(outputValueFactory), mapPaths[i]);
                mappings.add(mapping);
            }
            return new ValueFactoryMapperFactory(mappings, dataRepository);
        }

        @Override
        public void save(final ValueFactoryMapperFactory factory, final NodeSettingsWO settings) {
            var inputValueFactorySettings = settings.addNodeSettings("inputValueFactories");
            var outputValueFactorySettings = settings.addNodeSettings("outputValueFactories");
            int i = 0;
            factory.getOutputSchema().getSourceSpec().save(settings.addNodeSettings("outputSpec"));
            for (var mapping : factory.m_mappings) {
                String key = Integer.toString(i);
                ValueFactoryUtils.saveValueFactory(mapping.inputValueFactory().getValueFactory(),
                    inputValueFactorySettings.addNodeSettings(key));
                ValueFactoryUtils.saveValueFactory(mapping.outputValueFactory().getValueFactory(),
                    outputValueFactorySettings.addNodeSettings(key));
                i++;
            }
            settings.addStringArray("mapPaths",//
                factory.m_mappings.stream()//
                .map(ColumnMapperFactory::mapPath)//
                .map(CastType::name)//
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
