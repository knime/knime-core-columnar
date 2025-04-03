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
 *   Mar 14, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table.virtual.persist;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.knime.core.table.schema.DataSpec.booleanSpec;
import static org.knime.core.table.schema.DataSpec.doubleSpec;
import static org.knime.core.table.schema.DataSpec.intSpec;
import static org.knime.core.table.schema.DataSpec.stringSpec;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.knime.core.data.IDataRepository;
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable;
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable.ColumnarMapperWithRowIndexFactory;
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable.WrappedColumnarMapperWithRowIndexFactory;
import org.knime.core.data.columnar.table.virtual.persist.TableTransformNodeSettingsPersistor.TransformLoadContext;
import org.knime.core.data.columnar.table.virtual.reference.ReferenceTable;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.filestore.internal.NotInWorkflowDataRepository;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.schema.ValueSchema.ValueSchemaColumn;
import org.knime.core.data.v2.schema.ValueSchemaUtils;
import org.knime.core.data.v2.value.DefaultRowKeyValueFactory;
import org.knime.core.data.v2.value.DoubleValueFactory;
import org.knime.core.data.v2.value.IntValueFactory;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.table.access.IntAccess.IntReadAccess;
import org.knime.core.table.access.IntAccess.IntWriteAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.row.RowAccessible;
import org.knime.core.table.schema.DefaultColumnarSchema;
import org.knime.core.table.schema.DefaultColumnarSchema.Builder;
import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.VirtualTable;
import org.knime.core.table.virtual.spec.AppendMapTransformSpec;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformSpec;
import org.knime.core.table.virtual.spec.MapTransformUtils.MapperWithRowIndexFactory;
import org.knime.core.table.virtual.spec.RowIndexTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTableProperties.CursorType;
import org.knime.core.table.virtual.spec.SourceTransformSpec;

import com.google.common.collect.Iterables;

/**
 * Unit tests for {@link TableTransformNodeSettingsPersistor}.
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("static-method")
final class TableTransformNodeSettingsPersistorTest {

    private static final IDataRepository DATA_REPO = NotInWorkflowDataRepository.newInstance();

    @Test
    void testSaveSource() throws Exception {
        var id = UUID.randomUUID();
        var virtualTable = new VirtualTable(id,
            schemaBuilder().addColumn(stringSpec()).addColumn(intSpec()).addColumn(doubleSpec()).build());
        var sourceTransform = virtualTable.getProducingTransform();
        var settings = rootSettings();
        TableTransformNodeSettingsPersistor.save(sourceTransform, settings);
        var connectionSettings = getConnectionSettings(settings);
        assertEquals(0, Iterables.size(connectionSettings), "There aren't supposed to be any connections.");
        var transformSettings = getTransformSettings(settings);
        assertEquals(1, Iterables.size(transformSettings));
        var sourceSettings = transformSettings.getNodeSettings("0");
        assertEquals("SOURCE", sourceSettings.getString("type"));
        assertEquals(id.toString(), sourceSettings.getNodeSettings("internal").getString("identifier"),
            "Unexpected source id.");
    }

    @Test
    void testLoadSource() throws Exception {
        var id = UUID.randomUUID();
        var settings = rootSettings();
        addSourceTransform(id, addTransformSettings(settings).addNodeSettings("0"));
        addConnectionSettings(settings);

        var sourceTransform = TableTransformNodeSettingsPersistor.load(settings, createLoadContext(id));
        var sourceSpec = (SourceTransformSpec)sourceTransform.getSpec();
        assertEquals(id, sourceSpec.getSourceIdentifier());
    }

    @Test
    void testSaveAppend() throws Exception {
        var firstId = UUID.randomUUID();
        var secondId = UUID.randomUUID();
        var firstSource = new VirtualTable(firstId, schemaBuilder().addColumn(stringSpec()).build());
        var secondSource = new VirtualTable(secondId, schemaBuilder().addColumn(booleanSpec()).build());
        var appended = firstSource.append(List.of(secondSource));
        var settings = rootSettings();
        TableTransformNodeSettingsPersistor.save(appended.getProducingTransform(), settings);
        var transformSettings = getTransformSettings(settings);
        assertEquals(3, Iterables.size(transformSettings), "Expected two sources and one append.");
        // append should be last
        var appendSettings = transformSettings.getNodeSettings("2");
        assertEquals("APPEND", appendSettings.getString("type"));
        // has no internal settings
        assertEquals(0, Iterables.size(appendSettings.getNodeSettings("internal")));

        var connectionSettings = getConnectionSettings(settings);
        assertEquals(2, Iterables.size(connectionSettings), "Expected 2 connections.");
        checkConnection(connectionSettings.getNodeSettings("0"), 1, 2, 0);
        checkConnection(connectionSettings.getNodeSettings("1"), 0, 2, 1);
    }

    @Test
    void testLoadAppend() throws Exception {
        var firstId = UUID.randomUUID();
        var secondId = UUID.randomUUID();
        var settings = rootSettings();
        var transformSettings = addTransformSettings(settings);
        addSourceTransform(firstId, transformSettings.addNodeSettings("0"));
        addSourceTransform(secondId, transformSettings.addNodeSettings("1"));
        addAppendTransform(transformSettings.addNodeSettings("2"));
        var connectionSettings = addConnectionSettings(settings);
        addConnection(0, 2, 0, connectionSettings.addNodeSettings("0"));
        addConnection(1, 2, 1, connectionSettings.addNodeSettings("1"));
        var transform = TableTransformNodeSettingsPersistor.load(settings, createLoadContext(firstId, secondId));
        assertEquals(firstId,
            ((SourceTransformSpec)transform.getPrecedingTransforms().get(0).getSpec()).getSourceIdentifier(),
            "Wrong source order.");
        assertEquals(secondId,
            ((SourceTransformSpec)transform.getPrecedingTransforms().get(1).getSpec()).getSourceIdentifier(),
            "Wrong source order.");
    }

    @Test
    void testSaveColumnFilter() throws Exception {
        var id = UUID.randomUUID();
        var table = new VirtualTable(id, schemaBuilder()//
            .addColumn(stringSpec())//
            .addColumn(intSpec())//
            .addColumn(doubleSpec())//
            .build()).selectColumns(0, 2);
        var settings = rootSettings();
        TableTransformNodeSettingsPersistor.save(table.getProducingTransform(), settings);
        var transformSettings = getTransformSettings(settings);
        checkSize(transformSettings, 2);
        var filterSettings = transformSettings.getNodeSettings("1");
        assertEquals("SELECT", filterSettings.getString("type"));
        assertArrayEquals(new int[]{0, 2}, filterSettings.getNodeSettings("internal").getIntArray("included_columns"));
        var connectionSettings = getConnectionSettings(settings);
        assertEquals(1, Iterables.size(connectionSettings));
        checkConnection(connectionSettings.getNodeSettings("0"), 0, 1, 0);
    }

    @Test
    void testLoadColumnFilter() throws Exception {
        var id = UUID.randomUUID();
        var settings = rootSettings();
        var transformSettings = addTransformSettings(settings);
        addSourceTransform(id, transformSettings.addNodeSettings("0"));
        addColumnFilterTransform(transformSettings.addNodeSettings("1"), 1, 2);
        addConnection(0, 1, 0, addConnectionSettings(settings).addNodeSettings("0"));
        var transform = TableTransformNodeSettingsPersistor.load(settings, createLoadContext(id));
        assertArrayEquals(new int[]{1, 2}, ((SelectColumnsTransformSpec)transform.getSpec()).getColumnSelection());
        assertEquals(id,
            ((SourceTransformSpec)transform.getPrecedingTransforms().get(0).getSpec()).getSourceIdentifier());
    }

    @Test
    void testSaveConcatenate() throws Exception {
        var schema = schemaBuilder().addColumn(stringSpec()).addColumn(intSpec()).build();
        var firstId = UUID.randomUUID();
        var secondId = UUID.randomUUID();
        var firstSource = new VirtualTable(firstId, schema);
        var secondSource = new VirtualTable(secondId, schema);
        var concatenated = firstSource.concatenate(List.of(secondSource));
        var settings = rootSettings();
        TableTransformNodeSettingsPersistor.save(concatenated.getProducingTransform(), settings);
        var transformSettings = getTransformSettings(settings);
        assertEquals(3, Iterables.size(transformSettings), "Expected two source and one concatenate transform");
        checkConcatenateSettings(transformSettings.getNodeSettings("2"));
        var connectionSettings = getConnectionSettings(settings);
        checkSize(connectionSettings, 2);
        checkConnection(connectionSettings.getNodeSettings("0"), 1, 2, 0);
        checkConnection(connectionSettings.getNodeSettings("1"), 0, 2, 1);
    }

    @Test
    void testLoadConcatenate() throws Exception {
        var settings = rootSettings();
        var transformSettings = addTransformSettings(settings);
        var firstID = UUID.randomUUID();
        var secondID = UUID.randomUUID();
        addSourceTransform(firstID, transformSettings.addNodeSettings("0"));
        addSourceTransform(secondID, transformSettings.addNodeSettings("1"));
        addConcatenateTransform(transformSettings.addNodeSettings("2"));
        var connections = addConnectionSettings(settings);
        addConnection(0, 2, 0, connections.addNodeSettings("0"));
        addConnection(1, 2, 1, connections.addNodeSettings("1"));
        var transform = TableTransformNodeSettingsPersistor.load(settings, createLoadContext(firstID, secondID));
        assertInstanceOf(ConcatenateTransformSpec.class, transform.getSpec());
        checkSourceTransform(transform.getPrecedingTransforms().get(0), firstID);
        checkSourceTransform(transform.getPrecedingTransforms().get(1), secondID);
    }

    @Test
    void testSaveSlice() throws Exception {
        var id = UUID.randomUUID();
        var table = new VirtualTable(id, schemaBuilder()//
            .addColumn(stringSpec())//
            .build()).slice(3, 10);
        var settings = rootSettings();
        TableTransformNodeSettingsPersistor.save(table.getProducingTransform(), settings);
        var transformSettings = getTransformSettings(settings);
        checkSize(transformSettings, 2);
        checkSourceSettings(transformSettings.getNodeSettings("0"), id);
        checkSliceSettings(transformSettings.getNodeSettings("1"), 3, 10);
        var connectionSettings = getConnectionSettings(settings);
        checkSize(connectionSettings, 1);
        checkConnection(connectionSettings.getNodeSettings("0"), 0, 1, 0);
    }

    @Test
    void testLoadSlice() throws Exception {
        var settings = rootSettings();
        var transformSettings = addTransformSettings(settings);
        var id = UUID.randomUUID();
        addSourceTransform(id, transformSettings.addNodeSettings("0"));
        addSliceSettings(transformSettings.addNodeSettings("1"), 13, 42);
        addConnection(0, 1, 0, addSubSettings(addConnectionSettings(settings), 0));
        var transform = TableTransformNodeSettingsPersistor.load(settings, createLoadContext(id));
        checkSliceTransform(transform, 13, 42);
        checkSourceTransform(transform.getPrecedingTransforms().get(0), id);
    }

    @Test
    void testSaveMap() throws Exception {
        var id = UUID.randomUUID();
        var mapperFactory = new TestMapperFactory(3);
        var schema = ValueSchemaUtils.create(//
            new ValueSchemaColumn(new DefaultRowKeyValueFactory()), //
            new ValueSchemaColumn("1", IntCell.TYPE, new IntValueFactory()), //
            new ValueSchemaColumn("2", DoubleCell.TYPE, new DoubleValueFactory()));
        var table = new ColumnarVirtualTable(id, schema, CursorType.BASIC).map(mapperFactory, 1);
        var settings = rootSettings();
        TableTransformNodeSettingsPersistor.save(table.getProducingTransform(), settings);
        var transformSettings = getTransformSettings(settings);
        checkSize(transformSettings, 3);
        checkSourceSettings(getSubSettings(transformSettings, 0), id);
        checkMapSettings(getSubSettings(transformSettings, 2), mapperFactory::checkSettings, TestMapperFactory.class, 1,
            3);
        var connectionSettings = getConnectionSettings(settings);
        checkSize(connectionSettings, 2);
        checkConnection(getSubSettings(connectionSettings, 0), 0, 1, 0);
    }

    @Test
    void testLoadMap() throws Exception {
        var settings = rootSettings();
        settings.addInt("version", 1);
        var transformSettings = addTransformSettings(settings);
        var id = UUID.randomUUID();
        addSourceTransform(id, addSubSettings(transformSettings, 0));
        addMapSettings(addSubSettings(transformSettings, 1), s -> s.addInt("increment", 42), TestMapperFactory.class,
            1);
        var connectionSettings = addConnectionSettings(settings);
        addConnection(0, 1, 0, addSubSettings(connectionSettings, 0));
        var transform = TableTransformNodeSettingsPersistor.load(settings, createLoadContext(id));
        checkMapTransform(transform, s -> {
            var wrappedMapperFactory = (WrappedColumnarMapperWithRowIndexFactory)s.getMapperFactory();
            var testMapperFactory = (TestMapperFactory)wrappedMapperFactory.getMapperWithRowIndexFactory();
            assertEquals(42, testMapperFactory.m_increment, "Unexpected increment.");
        });
        checkSourceTransform(transform.getPrecedingTransforms().get(0), id);
    }

    @Test
    void testSaveAppendMap() throws Exception {
        var id = UUID.randomUUID();
        var mapperFactory = new TestMapperFactory(3);
        var schema = ValueSchemaUtils.create(//
            new ValueSchemaColumn(new DefaultRowKeyValueFactory()), //
            new ValueSchemaColumn("1", IntCell.TYPE, new IntValueFactory()), //
            new ValueSchemaColumn("2", DoubleCell.TYPE, new DoubleValueFactory()));
        var table = new ColumnarVirtualTable(id, schema, CursorType.BASIC).appendMap(mapperFactory, 1);
        var settings = rootSettings();
        TableTransformNodeSettingsPersistor.save(table.getProducingTransform(), settings);
        var transformSettings = getTransformSettings(settings);
        checkSize(transformSettings, 4);
        checkSourceSettings(getSubSettings(transformSettings, 0), id);
        checkAppendMapSettings(getSubSettings(transformSettings, 2), mapperFactory::checkSettings,
            TestMapperFactory.class, 1, 3);
        var connectionSettings = getConnectionSettings(settings);
        checkSize(connectionSettings, 3);
        checkConnection(getSubSettings(connectionSettings, 0), 0, 1, 0);
        checkConnection(getSubSettings(connectionSettings, 1), 1, 2, 0);
        checkConnection(getSubSettings(connectionSettings, 2), 2, 3, 0);
    }

    @Test
    void testLoadAppendMap() throws Exception {
        var settings = rootSettings();
        settings.addInt("version", 1);
        var transformSettings = addTransformSettings(settings);
        var id = UUID.randomUUID();
        addSourceTransform(id, addSubSettings(transformSettings, 0));
        addAppendMapSettings(addSubSettings(transformSettings, 1), s -> s.addInt("increment", 42), TestMapperFactory.class,
            1);
        var connectionSettings = addConnectionSettings(settings);
        addConnection(0, 1, 0, addSubSettings(connectionSettings, 0));
        var transform = TableTransformNodeSettingsPersistor.load(settings, createLoadContext(id));
        checkAppendMapTransform(transform, s -> {
            var wrappedMapperFactory = (WrappedColumnarMapperWithRowIndexFactory)s.getMapperFactory();
            var testMapperFactory = (TestMapperFactory)wrappedMapperFactory.getMapperWithRowIndexFactory();
            assertEquals(42, testMapperFactory.m_increment, "Unexpected increment.");
        });
        checkSourceTransform(transform.getPrecedingTransforms().get(0), id);
    }

    @Test
    void testLoadOldMap() throws Exception {
        // Maps which use a row index are saved differently since AP 5.3 (the RowIndex is a dedicated transform spec
        // now). This test checks backwards compatible loading because it doesn't set the "version" key (compare
        // with the testLoadMap() test above).
        var settings = rootSettings();
        var transformSettings = addTransformSettings(settings);
        var id = UUID.randomUUID();
        addSourceTransform(id, addSubSettings(transformSettings, 0));
        addMapSettings(addSubSettings(transformSettings, 1), s -> s.addInt("increment", 42), TestMapperFactory.class,
            1);
        var connectionSettings = addConnectionSettings(settings);
        addConnection(0, 1, 0, addSubSettings(connectionSettings, 0));
        var transform = TableTransformNodeSettingsPersistor.load(settings, createLoadContext(id));
        checkMapTransform(transform, s -> {
            var wrappedMapperFactory = (WrappedColumnarMapperWithRowIndexFactory)s.getMapperFactory();
            var testMapperFactory = (TestMapperFactory)wrappedMapperFactory.getMapperWithRowIndexFactory();
            assertEquals(42, testMapperFactory.m_increment, "Unexpected increment.");
        });

        // expecting a RowIndexTransformSpec before the map, and a column selection
        var rowIndexTransform = transform.getPrecedingTransforms().get(0);
        assertInstanceOf(RowIndexTransformSpec.class, rowIndexTransform.getSpec());

        var selectColumnsTransform = rowIndexTransform.getPrecedingTransforms().get(0);
        assertInstanceOf(SelectColumnsTransformSpec.class, selectColumnsTransform.getSpec());

        checkSourceTransform(selectColumnsTransform.getPrecedingTransforms().get(0), id);
    }

    private static void checkMapTransform(final TableTransform transform,
        final Consumer<MapTransformSpec> specChecker) {
        assertInstanceOf(MapTransformSpec.class, transform.getSpec());
        specChecker.accept((MapTransformSpec)transform.getSpec());
    }

    private static void checkAppendMapTransform(final TableTransform transform,
        final Consumer<AppendMapTransformSpec> specChecker) {
        assertInstanceOf(AppendMapTransformSpec.class, transform.getSpec());
        specChecker.accept((AppendMapTransformSpec)transform.getSpec());
    }

    private static NodeSettingsWO addSubSettings(final NodeSettingsWO settings, final int index) {
        return settings.addNodeSettings(Integer.toString(index));
    }

    private static NodeSettingsRO getSubSettings(final NodeSettingsRO settings, final int index)
        throws InvalidSettingsException {
        return settings.getNodeSettings(Integer.toString(index));
    }

    @FunctionalInterface
    interface SettingsChecker {
        void checkSettings(final NodeSettingsRO settings) throws InvalidSettingsException;
    }

    private static void checkMapSettings(final NodeSettingsRO settings,
        final SettingsChecker mapperFactorySettingsChecker,
        final Class<? extends MapperWithRowIndexFactory> mapperFactoryClass, final int... columnIndices)
        throws InvalidSettingsException {
        checkTransformType(settings, "MAP");
        var internals = settings.getNodeSettings("internal");
        assertArrayEquals(columnIndices, internals.getIntArray("column_indices"), "Unexpected column indices.");
        assertEquals(mapperFactoryClass.getName(), internals.getString("mapper_factory_class"),
            "Unexpected MapperFactory class.");
        mapperFactorySettingsChecker.checkSettings(internals.getNodeSettings("mapper_factory_settings"));
    }

    private static void checkAppendMapSettings(final NodeSettingsRO settings,
        final SettingsChecker mapperFactorySettingsChecker,
        final Class<? extends MapperWithRowIndexFactory> mapperFactoryClass, final int... columnIndices)
        throws InvalidSettingsException {
        checkTransformType(settings, "APPEND_MAP");
        var internals = settings.getNodeSettings("internal");
        assertArrayEquals(columnIndices, internals.getIntArray("column_indices"), "Unexpected column indices.");
        assertEquals(mapperFactoryClass.getName(), internals.getString("mapper_factory_class"),
            "Unexpected MapperFactory class.");
        mapperFactorySettingsChecker.checkSettings(internals.getNodeSettings("mapper_factory_settings"));
    }

    private static void addMapSettings(final NodeSettingsWO settings, final Consumer<NodeSettingsWO> mapSettingsAdder,
        final Class<? extends MapperWithRowIndexFactory> mapperFactoryClass, final int... columnIndices) {
        addTransformType(settings, "MAP");
        var internals = settings.addNodeSettings("internal");
        internals.addIntArray("column_indices", columnIndices);
        internals.addString("mapper_factory_class", mapperFactoryClass.getName());
        mapSettingsAdder.accept(internals.addNodeSettings("mapper_factory_settings"));
    }

    private static void addAppendMapSettings(final NodeSettingsWO settings,
        final Consumer<NodeSettingsWO> mapSettingsAdder,
        final Class<? extends MapperWithRowIndexFactory> mapperFactoryClass, final int... columnIndices) {
        addTransformType(settings, "APPEND_MAP");
        var internals = settings.addNodeSettings("internal");
        internals.addIntArray("column_indices", columnIndices);
        internals.addString("mapper_factory_class", mapperFactoryClass.getName());
        mapSettingsAdder.accept(internals.addNodeSettings("mapper_factory_settings"));
    }

    private static final class TestMapperFactory implements ColumnarMapperWithRowIndexFactory {

        private int m_increment;

        TestMapperFactory(final int increment) {
            m_increment = increment;
        }

        @Override
        public ValueSchema getOutputSchema() {
            return ValueSchemaUtils.create(new ValueSchemaColumn("foo", IntCell.TYPE, new IntValueFactory()));
        }

        @Override
        public MapperWithRowIndexFactory.Mapper createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs) {
            var readAccess = (IntReadAccess)inputs[0];
            var writeAccess = (IntWriteAccess)outputs[0];
            return r -> writeAccess.setIntValue(readAccess.getIntValue() + m_increment);
        }

        void checkSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
            assertEquals(m_increment, settings.getInt("increment"), "Unexpected increment.");
        }

        // it is registered at the corresponding extension point and used to persist TestMapperFactory
        @SuppressWarnings("unused")
        public static final class TestMapperFactoryPersistor implements Persistor<TestMapperFactory> {

            @Override
            public TestMapperFactory load(final NodeSettingsRO settings, final LoadContext ctx)
                throws InvalidSettingsException {
                return new TestMapperFactory(settings.getInt("increment"));
            }

            @Override
            public void save(final TestMapperFactory factory, final NodeSettingsWO settings) {
                settings.addInt("increment", factory.m_increment);
            }

        }

    }

    private static void checkSliceTransform(final TableTransform transform, final long from, final long to) {
        var spec = transform.getSpec();
        assertInstanceOf(SliceTransformSpec.class, spec, "Expected a SliceTransformSpec.");
        var rowRange = ((SliceTransformSpec)spec).getRowRangeSelection();
        assertEquals(from, rowRange.fromIndex(), "Unexpected from index.");
        assertEquals(to, rowRange.toIndex(), "Unexpected to index.");
    }

    private static void addSliceSettings(final NodeSettingsWO settings, final long from, final long to) {
        addTransformType(settings, "SLICE");
        var internals = settings.addNodeSettings("internal");
        internals.addLong("from", from);
        internals.addLong("to", to);
    }

    private static void checkSliceSettings(final NodeSettingsRO settings, final long from, final long to)
        throws InvalidSettingsException {
        checkTransformType(settings, "SLICE");
        var internals = settings.getNodeSettings("internal");
        assertEquals(from, internals.getLong("from"), "Unexpected start point (from) of slice.");
        assertEquals(to, internals.getLong("to"), "Unexpected end point (to) of slice.");
    }

    private static void checkSize(final NodeSettingsRO transformSettings, final int expectedSize) {
        assertEquals(expectedSize, Iterables.size(transformSettings));
    }

    private NodeSettings getConnectionSettings(final NodeSettings settings) throws InvalidSettingsException {
        return settings.getNodeSettings("connections");
    }

    private static NodeSettingsRO getTransformSettings(final NodeSettings settings) throws InvalidSettingsException {
        return settings.getNodeSettings("transforms");
    }

    private static void checkSourceTransform(final TableTransform sourceTransform, final UUID expectedID) {
        assertInstanceOf(SourceTransformSpec.class, sourceTransform.getSpec());
        assertEquals(expectedID, ((SourceTransformSpec)sourceTransform.getSpec()).getSourceIdentifier());
    }

    private static NodeSettingsWO addConnectionSettings(final NodeSettings settings) {
        return settings.addNodeSettings("connections");
    }

    private static NodeSettingsWO addTransformSettings(final NodeSettings settings) {
        return settings.addNodeSettings("transforms");
    }

    private static void checkConcatenateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        checkTransformType(settings, "CONCATENATE");
        assertEquals(0, Iterables.size(settings.getNodeSettings("internal")));
    }

    private static void checkTransformType(final NodeSettingsRO settings, final String type)
        throws InvalidSettingsException {
        assertEquals(type, settings.getString("type"), "Unexpected transform type.");
    }

    private static void checkSourceSettings(final NodeSettingsRO settings, final UUID expectedID)
        throws InvalidSettingsException {
        checkTransformType(settings, "SOURCE");
        assertEquals(expectedID.toString(), settings.getNodeSettings("internal").getString("identifier"),
            "Unexpected identifier.");
    }

    private static void checkConnection(final NodeSettingsRO connection, final int from, final int to, final int toPort)
        throws InvalidSettingsException {
        assertEquals(from, connection.getNodeSettings("from").getInt("transform"), "Unexpected from in connection.");
        NodeSettingsRO toSettings = connection.getNodeSettings("to");
        assertEquals(to, toSettings.getInt("transform"), "Unexpected to in connection");
        assertEquals(toPort, toSettings.getInt("port"), "Unexpected toPort in connection");
    }

    private static void addConnection(final int from, final int to, final int toPort, final NodeSettingsWO settings) {
        settings.addNodeSettings("from").addInt("transform", from);
        var toSettings = settings.addNodeSettings("to");
        toSettings.addInt("transform", to);
        toSettings.addInt("port", toPort);
    }

    private static void addTransformType(final NodeSettingsWO settings, final String type) {
        settings.addString("type", type);
    }

    private static void addConcatenateTransform(final NodeSettingsWO settings) {
        settings.addString("type", "CONCATENATE");
        settings.addNodeSettings("internal");
    }

    private static void addAppendTransform(final NodeSettingsWO settings) {
        settings.addString("type", "APPEND");
        settings.addNodeSettings("internal");
    }

    private static void addColumnFilterTransform(final NodeSettingsWO settings, final int... includedColumns) {
        settings.addString("type", "SELECT");
        settings.addNodeSettings("internal").addIntArray("included_columns", includedColumns);
    }

    private static void addSourceTransform(final UUID sourceId, final NodeSettingsWO settings) {
        settings.addString("type", "SOURCE");
        settings.addNodeSettings("internal").addString("identifier", sourceId.toString());
    }

    private static NodeSettings rootSettings() {
        return new NodeSettings("test");
    }

    private Builder schemaBuilder() {
        return DefaultColumnarSchema.builder();
    }

    private TransformLoadContext createLoadContext(final UUID... sourceIds) {
        var sourceIdsSet = Stream.of(sourceIds).collect(Collectors.toSet());
        return new TransformLoadContext() {

            @Override
            public IDataRepository getDataRepository() {
                return DATA_REPO;
            }

            @Override
            public ReferenceTable getReferenceTable(final UUID id) throws IllegalArgumentException {
                if (sourceIdsSet.contains(id)) {
                    return new DummyReferenceTable(id);
                } else {
                    throw new AssertionError("Tried to access unexpected reference table with id " + id);
                }
            }
        };
    }

    private static final class DummyReferenceTable implements ReferenceTable {

        private final UUID m_id;

        public DummyReferenceTable(final UUID id) {
            m_id = id;
        }

        @Override
        public UUID getId() {
            return m_id;
        }

        @Override
        public ValueSchema getSchema() {
            return null;
        }

        @Override
        public ColumnarVirtualTable getVirtualTable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<UUID, RowAccessible> getSources() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BufferedDataTable getBufferedTable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clearIfNecessary() {
            throw new UnsupportedOperationException();
        }
    }
}
