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
 *   29 Jan 2021 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.knime.core.data.columnar.table.ColumnarRowCursorTest.compare;
import static org.knime.core.data.columnar.table.ColumnarTableTestUtils.createColumnarRowContainer;
import static org.knime.core.data.columnar.table.ColumnarTableTestUtils.createUnsavedColumnarContainerTable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLStreamException;

import org.junit.Test;
import org.knime.core.columnar.testing.ColumnarTest;
import org.knime.core.data.AdapterCell;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.collection.SetCell;
import org.knime.core.data.collection.SparseListCell;
import org.knime.core.data.columnar.table.ColumnarTableTestUtils.RowWriteConsumer;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.v2.ReadValue;
import org.knime.core.data.v2.RowCursor;
import org.knime.core.data.v2.RowRead;
import org.knime.core.data.v2.RowWrite;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.data.v2.schema.ValueSchema;
import org.knime.core.data.v2.value.ValueInterfaces.BooleanListReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.BooleanListWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.BooleanSetReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.BooleanSetWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.BooleanSparseListReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.BooleanSparseListWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.DoubleListReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.DoubleListWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.DoubleSetReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.DoubleSetWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.DoubleSparseListReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.DoubleSparseListWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.IntListReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.IntListWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.IntReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.IntSetReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.IntSetWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.IntSparseListReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.IntSparseListWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.IntWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.LongListReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.LongListWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.LongSetReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.LongSetWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.LongSparseListReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.LongSparseListWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.StringListReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.StringListWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.StringSetReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.StringSetWriteValue;
import org.knime.core.data.v2.value.ValueInterfaces.StringSparseListReadValue;
import org.knime.core.data.v2.value.ValueInterfaces.StringSparseListWriteValue;
import org.knime.core.data.xml.XMLCell;
import org.knime.core.data.xml.XMLCellFactory;
import org.knime.core.data.xml.XMLValue;
import org.knime.core.node.ExtensionTable;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.traits.DataTrait.DictEncodingTrait;
import org.knime.core.table.schema.traits.LogicalTypeTrait;
import org.knime.core.table.schema.traits.StructDataTraits;
import org.xml.sax.SAXException;

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("javadoc")
public class ColumnarRowWriteCursorTest extends ColumnarTest {

    private static void copy(final RowCursor readCursor, final ColumnarRowWriteCursor writeCursor) {
        RowRead rowRead = null;
        RowWrite rowWrite = null;
        assertEquals(readCursor.getNumColumns(), writeCursor.getNumColumns());
        while ((rowRead = readCursor.forward()) != null) {
            writeCursor.commit(rowRead);
        }
    }

    @Test
    public void testSingleBatchRowCursorCopy() {
        // we open two stores, so we also need for both of them to close
        try (final ExtensionTable table = createUnsavedColumnarContainerTable(2);
                final RowCursor cursor = table.cursor(TableFilter.filterRangeOfRows(0, 1));
                final ColumnarRowContainer copyContainer = createColumnarRowContainer();
                final ColumnarRowWriteCursor copyWriteCursor = copyContainer.createCursor()) {
            copy(cursor, copyWriteCursor);
            try (final ExtensionTable copyTable = copyContainer.finishInternal()) {
                compare(copyTable, 0, 1);
            }
        }
    }

    @FunctionalInterface
    interface RowReadConsumer {
        void accept(RowRead row, int colIdx, int rowIdx);
    }

    @FunctionalInterface
    interface SchemaChecker {
        void accept(ValueSchema schema);
    }

    private static void testWriteReadRows(final RowWriteConsumer writer, final RowReadConsumer reader,
        final DataType type) {
        testWriteReadRows(writer, reader, type, null);
    }

    private static void testWriteReadRows(final RowWriteConsumer writer, final RowReadConsumer reader,
        final DataType type, final SchemaChecker schemaChecker) {
        try (final UnsavedColumnarContainerTable table = createUnsavedColumnarContainerTable(1, 2, type, writer, null);
                final RowCursor cursor = table.cursor()) {
            if (schemaChecker != null) {
                schemaChecker.accept(table.getSchema());
            }

            RowRead rowRead = null;
            int rowIdx = 0;
            while ((rowRead = cursor.forward()) != null) {
                // every 2nd row is missing in this test
                if (!rowRead.isMissing(0)) {
                    for (int colIdx = 0; colIdx < rowRead.getNumColumns(); colIdx++) {
                        reader.accept(rowRead, colIdx, rowIdx);
                    }
                }
                rowIdx++;
            }
        }
    }

    @Test
    public void testWriteReadInt() {
        final var type = IntCell.TYPE;
        testWriteReadRows(//
            (row, j, i) -> row.<IntWriteValue> getWriteValue(j).setIntValue(i), //
            (row, j, i) -> assertEquals(i, row.<IntReadValue> getValue(j).getIntValue()), //
            type);
    }

    // List
    @Test
    public void testWriteReadIntList() {
        final var type = DataType.getType(ListCell.class, IntCell.TYPE);
        final var values = new int[]{1, 2, 3, 4};
        testWriteReadRows(//
            (row, j, i) -> row.<IntListWriteValue> getWriteValue(j).setValue(values), //
            (row, j, i) -> assertTrue(Arrays.equals(values, row.<IntListReadValue> getValue(j).getIntArray())), //
            type);
    }

    @Test
    public void testWriteReadLongList() {
        final var type = DataType.getType(ListCell.class, LongCell.TYPE);
        final var values = new long[]{1, 2, 3, 4};
        testWriteReadRows(//
            (row, j, i) -> row.<LongListWriteValue> getWriteValue(j).setValue(values), //
            (row, j, i) -> assertTrue(Arrays.equals(values, row.<LongListReadValue> getValue(j).getLongArray())), //
            type);
    }

    @Test
    public void testWriteReadDoubleList() {
        final var type = DataType.getType(ListCell.class, DoubleCell.TYPE);
        final var values = new double[]{1, 2, 3, 4};
        testWriteReadRows(//
            (row, j, i) -> row.<DoubleListWriteValue> getWriteValue(j).setValue(values), //
            (row, j, i) -> assertTrue(Arrays.equals(values, row.<DoubleListReadValue> getValue(j).getDoubleArray())), //
            type);
    }

    @Test
    public void testWriteReadStringList() {
        final var type = DataType.getType(ListCell.class, StringCell.TYPE);
        final var values = new String[]{"1", "2", "3", "4"};
        testWriteReadRows(//
            (row, j, i) -> row.<StringListWriteValue> getWriteValue(j).setValue(values), //
            (row, j, i) -> assertTrue(Arrays.equals(values, row.<StringListReadValue> getValue(j).getStringArray())), //
            type);
    }

    @Test
    public void testWriteReadBooleanList() {
        final var type = DataType.getType(ListCell.class, BooleanCell.TYPE);
        final var values = new boolean[]{true, true, false};
        testWriteReadRows(//
            (row, j, i) -> row.<BooleanListWriteValue> getWriteValue(j).setValue(values), //
            (row, j, i) -> assertTrue(Arrays.equals(values, row.<BooleanListReadValue> getValue(j).getBooleanArray())), //
            type);
    }

    // Set
    @Test
    public void testWriteReadIntSet() {
        final var type = DataType.getType(SetCell.class, IntCell.TYPE);
        final var values = new HashSet<Integer>(Arrays.asList(1, 2, 3, 4));
        testWriteReadRows(//
            (row, j, i) -> row.<IntSetWriteValue> getWriteValue(j).setIntCollectionValue(values), //
            (row, j, i) -> assertEquals(values, row.<IntSetReadValue> getValue(j).getIntSet()), //
            type);
    }

    @Test
    public void testWriteReadLongSet() {
        final var type = DataType.getType(SetCell.class, LongCell.TYPE);
        final var values = new HashSet<Long>(Arrays.asList((long)1, (long)2, (long)3, (long)4));
        testWriteReadRows(//
            (row, j, i) -> row.<LongSetWriteValue> getWriteValue(j).setLongCollectionValue(values), //
            (row, j, i) -> assertEquals(values, row.<LongSetReadValue> getValue(j).getLongSet()), //
            type);
    }

    @Test
    public void testWriteReadDoubleSet() {
        final var type = DataType.getType(SetCell.class, DoubleCell.TYPE);
        final var values = new HashSet<Double>(Arrays.asList(1.0, 2.0, 3.0, 4.0));
        testWriteReadRows(//
            (row, j, i) -> row.<DoubleSetWriteValue> getWriteValue(j).setDoubleCollectionValue(values), //
            (row, j, i) -> assertEquals(values, row.<DoubleSetReadValue> getValue(j).getDoubleSet()), //
            type);
    }

    @Test
    public void testWriteReadStringSet() {
        final var type = DataType.getType(SetCell.class, StringCell.TYPE);
        final var values = new HashSet<String>(Arrays.asList("1", "2", "3", "4"));
        testWriteReadRows(//
            (row, j, i) -> row.<StringSetWriteValue> getWriteValue(j).setStringCollectionValue(values), //
            (row, j, i) -> assertEquals(values, row.<StringSetReadValue> getValue(j).getStringSet()), //
            type);
    }

    @Test
    public void testWriteReadBooleanSet() {
        final var type = DataType.getType(SetCell.class, BooleanCell.TYPE);
        final var values = new HashSet<Boolean>(Arrays.asList(true, true, false));
        testWriteReadRows(//
            (row, j, i) -> row.<BooleanSetWriteValue> getWriteValue(j).setBooleanCollectionValue(values), //
            (row, j, i) -> assertEquals(values, row.<BooleanSetReadValue> getValue(j).getBooleanSet()), //
            type);
    }

    // Sparse list
    @Test
    public void testWriteReadIntSparseList() {
        final var type = DataType.getType(SparseListCell.class, IntCell.TYPE);
        final var values = new int[]{1, 2, 1, 3, 1, 4};
        final var indices = new int[]{1, 3, 5};
        testWriteReadRows(//
            (row, j, i) -> row.<IntSparseListWriteValue> getWriteValue(j).setValue(values, 1), //
            (row, j, i) -> assertTrue(Arrays.equals(indices, row.<IntSparseListReadValue> getValue(j).getAllIndices())), //
            type);
    }

    @Test
    public void testWriteReadLongSparseList() {
        final var type = DataType.getType(SparseListCell.class, LongCell.TYPE);
        final var values = new long[]{1, 2, 1, 3, 1, 4};
        final var indices = new int[]{1, 3, 5};
        testWriteReadRows(//
            (row, j, i) -> row.<LongSparseListWriteValue> getWriteValue(j).setValue(values, 1), //
            (row, j,
                i) -> assertTrue(Arrays.equals(indices, row.<LongSparseListReadValue> getValue(j).getAllIndices())), //
            type);
    }

    @Test
    public void testWriteReadDoubleSparseList() {
        final var type = DataType.getType(SparseListCell.class, DoubleCell.TYPE);
        final var values = new double[]{1, 2, 1, 3, 1, 4};
        final var indices = new int[]{1, 3, 5};
        testWriteReadRows(//
            (row, j, i) -> row.<DoubleSparseListWriteValue> getWriteValue(j).setValue(values, 1.0), //
            (row, j,
                i) -> assertTrue(Arrays.equals(indices, row.<DoubleSparseListReadValue> getValue(j).getAllIndices())), //
            type);
    }

    @Test
    public void testWriteReadStringSparseList() {
        final var type = DataType.getType(SparseListCell.class, StringCell.TYPE);
        final var values = new String[]{"1", "2", "1", "3", "1", "4"};
        final var indices = new int[]{1, 3, 5};
        testWriteReadRows(//
            (row, j, i) -> row.<StringSparseListWriteValue> getWriteValue(j).setValue(values, "1"), //
            (row, j,
                i) -> assertTrue(Arrays.equals(indices, row.<StringSparseListReadValue> getValue(j).getAllIndices())), //
            type);
    }

    @Test
    public void testWriteReadBooleanSparseList() {
        final var type = DataType.getType(SparseListCell.class, BooleanCell.TYPE);
        final var values = new boolean[]{true, false, true, false, true, false};
        final var indices = new int[]{1, 3, 5};
        testWriteReadRows(//
            (row, j, i) -> row.<BooleanSparseListWriteValue> getWriteValue(j).setValue(values, true), //
            (row, j,
                i) -> assertTrue(Arrays.equals(indices, row.<BooleanSparseListReadValue> getValue(j).getAllIndices())), //
            type);
    }

    @Test
    public void testWriteReadNonValueFactoryDataCell()
        throws IOException, ParserConfigurationException, SAXException, XMLStreamException {
        final var type = MyNonValueFactoryCell.TYPE;
        final var value = new MyNonValueFactoryCell("foo");
        testWriteReadRows(//
            (row, j, i) -> row.<WriteValue<DataCell>> getWriteValue(j).setValue(value), //
            (row, j, i) -> assertEquals(value, row.<ReadValue> getValue(j).getDataCell()), //
            type, //
            (schema) -> {
                final var logicalType = schema.getTraits(1).get(LogicalTypeTrait.class);
                assertNotNull(logicalType);
                // we explicitly check for the logical type because the DictEncodedDataCellValueFactory is not visible here
                assertEquals(//
                    "{\"value_factory_class\":\"org.knime.core.data.v2.value.cell.DictEncodedDataCellValueFactory\""//
                        + ",\"data_type\":{\"cell_class\":\"org.knime.core.data.columnar.table.MyNonValueFactoryCell\"}}", //
                    logicalType.getLogicalType());
                assertTrue(schema.getSpec(1) instanceof StructDataSpec);
                final StructDataSpec structSpec = (StructDataSpec)schema.getSpec(1);
                assertTrue(structSpec.getDataSpec(0) instanceof VarBinaryDataSpec);
                assertTrue(structSpec.getDataSpec(1) instanceof StringDataSpec);
                final StructDataTraits structTraits = (StructDataTraits)schema.getTraits(1);
                assertTrue(DictEncodingTrait.isEnabled(structTraits.getDataTraits(0)));
                assertTrue(DictEncodingTrait.isEnabled(structTraits.getDataTraits(1)));
            });
    }

    @Test
    public void testWriteReadXMLDataCell()
        throws IOException, ParserConfigurationException, SAXException, XMLStreamException {
        final var type = XMLCell.TYPE;
        final var xmlString = "<dummyXML>Test</dummyXML>";
        final var value = XMLCellFactory.create(xmlString);
        testWriteReadRows(//
            (row, j, i) -> row.<WriteValue<DataCell>> getWriteValue(j).setValue(value), //
            (row, j, i) -> assertEquals(value, row.<ReadValue> getValue(j).getDataCell()), //
            type, //
            (schema) -> {
                final var logicalType = schema.getTraits(1).get(LogicalTypeTrait.class);
                assertNotNull(logicalType);
                assertEquals(//
                    "{\"value_factory_class\":\"org.knime.core.data.xml.XMLValueFactory\"}", //
                    logicalType.getLogicalType());
                assertTrue(schema.getSpec(1) instanceof StructDataSpec);
                final StructDataSpec structSpec = (StructDataSpec)schema.getSpec(1);
                assertTrue(structSpec.getDataSpec(0) instanceof StringDataSpec); // File store key
                assertTrue(structSpec.getDataSpec(1) instanceof VarBinaryDataSpec); // Blob data
                final StructDataTraits structTraits = (StructDataTraits)schema.getTraits(1);
                assertFalse(DictEncodingTrait.isEnabled(structTraits.getDataTraits(0)));
                assertFalse(DictEncodingTrait.isEnabled(structTraits.getDataTraits(1)));
            });
    }

    @Test
    public void testWriteReadAdapterXMLDataCell()
        throws IOException, ParserConfigurationException, SAXException, XMLStreamException {
        final var type = MyAdapterCell.TYPE;
        final var xmlString = "<dummyXML>Test</dummyXML>";
        final var innerValue = XMLCellFactory.create(xmlString);
        final var value = new MyAdapterCell(innerValue, XMLValue.class);

        testWriteReadRows(//
            (row, j, i) -> row.<WriteValue<DataCell>> getWriteValue(j).setValue(value), //
            (row, j, i) -> {
                var dataCell = row.<ReadValue> getValue(j).getDataCell();
                assertInstanceOf(AdapterCell.class, dataCell);
                assertTrue(((AdapterCell)dataCell).isAdaptable(XMLValue.class));
                assertEquals(value, dataCell); //
            }, //
            type, //
            (schema) -> {
                final var logicalType = schema.getTraits(1).get(LogicalTypeTrait.class);
                assertNotNull(logicalType);
                // The logical type doesn't contain the XMLValue as adapters are added per instance and are not part of the
                // column's DataSpec
                assertEquals(//
                    "{" //
                        + "\"value_factory_class\":\"org.knime.core.data.v2.value.cell.DictEncodedDataCellValueFactory\","
                        + "\"data_type\":{"//
                        + "\"cell_class\":\"org.knime.core.data.columnar.table.MyAdapterCell\"," //
                        + "\"adapter_classes\":[\"org.knime.core.data.RWAdapterValue\",\"org.knime.core.data.AdapterValue\"," //
                        + "\"org.knime.core.data.DataValue\"]" //
                        + "}}", //
                    logicalType.getLogicalType());
                assertTrue(schema.getSpec(1) instanceof StructDataSpec);
                final StructDataSpec structSpec = (StructDataSpec)schema.getSpec(1);
                assertTrue(structSpec.getDataSpec(0) instanceof VarBinaryDataSpec);
                assertTrue(structSpec.getDataSpec(1) instanceof StringDataSpec);
                final StructDataTraits structTraits = (StructDataTraits)schema.getTraits(1);
                assertTrue(DictEncodingTrait.isEnabled(structTraits.getDataTraits(0)));
                assertTrue(DictEncodingTrait.isEnabled(structTraits.getDataTraits(1)));
            });
    }
}
