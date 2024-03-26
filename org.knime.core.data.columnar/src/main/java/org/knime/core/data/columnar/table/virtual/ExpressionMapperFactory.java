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
 *   31 Jan 2024 (chaubold): created
 */
package org.knime.core.data.columnar.table.virtual;

import java.util.ArrayList;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.columnar.schema.ColumnarValueSchema;
import org.knime.core.data.columnar.schema.ColumnarValueSchemaUtils;
import org.knime.core.data.columnar.table.virtual.ColumnarVirtualTable.ColumnarMapperFactory;
import org.knime.core.data.columnar.table.virtual.persist.Persistor;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.ValueFactoryUtils;
import org.knime.core.data.v2.value.BooleanValueFactory;
import org.knime.core.data.v2.value.DoubleValueFactory;
import org.knime.core.data.v2.value.LongValueFactory;
import org.knime.core.data.v2.value.StringValueFactory;
import org.knime.core.expressions.Ast;
import org.knime.core.expressions.Computer;
import org.knime.core.expressions.Expressions;
import org.knime.core.expressions.Expressions.ExpressionError;
import org.knime.core.expressions.ValueType;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.virtual.expression.Exec;
import org.knime.core.table.virtual.spec.MapTransformSpec.MapperFactory;

/**
 * The {@link ExpressionMapperFactory} parses the expression and applies it to each row of the given data.
 *
 * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
 * @since 5.3
 */
@SuppressWarnings("restriction") // Expressions API is not yet public
public class ExpressionMapperFactory implements ColumnarMapperFactory {

    final String m_expression;

    private final MapperFactory m_mapperFactory;

    private final int[] m_columnIndices;

    final ColumnarValueSchema m_inputTableSchema;

    final String m_newColumnName;

    public ExpressionMapperFactory(final String expression, final ColumnarValueSchema inputTableSchema,
        final String newColumnName) {
        m_expression = expression;
        m_inputTableSchema = inputTableSchema;
        m_newColumnName = newColumnName;

        try {
            Function<String, OptionalInt> colNameToIdx = colName -> {
                var colIdx = inputTableSchema.getSourceSpec().findColumnIndex(colName);
                return colIdx == -1 ? OptionalInt.empty() : OptionalInt.of(colIdx + 1);
            };
            Function<Ast.ColumnAccess, Optional<ValueType>> colNameToType =
                colAccess -> colNameToIdx.apply(colAccess.name()).stream().mapToObj(inputTableSchema::getSpec)
                    .map(s -> s.accept(Exec.DATA_SPEC_TO_EXPRESSION_TYPE)).findFirst();
            var ast = Expressions.parse(expression);
            Expressions.resolveColumnIndices(ast, colNameToIdx);
            Expressions.inferTypes(ast, colNameToType);

            final var columns = Exec.RequiredColumns.of(ast);
            m_columnIndices = columns.columnIndices();

            final IntFunction<Function<ReadAccess[], ? extends Computer>> columnIndexToComputerFactory =
                columnIndex -> {
                    int inputIndex = columns.getInputIndex(columnIndex);
                    Function<ReadAccess, ? extends Computer> createComputer =
                        inputTableSchema.getSpec(columnIndex).accept(Exec.DATA_SPEC_TO_READER_FACTORY);
                    return readAccesses -> createComputer.apply(readAccesses[inputIndex]);
                };

            m_mapperFactory = Exec.createMapperFactory(ast, columnIndexToComputerFactory);

        } catch (ExpressionError ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    @Override
    public Runnable createMapper(final ReadAccess[] inputs, final WriteAccess[] outputs) {
        return m_mapperFactory.createMapper(inputs, outputs);
    }

    @Override
    public ColumnarValueSchema getOutputSchema() {
        var schema = m_mapperFactory.getOutputSchema();
        CheckUtils.checkArgument(schema.numColumns() == 1,
            "An expression must create exactly one column, but got " + schema.numColumns());
        var valueFactories = new ValueFactory[]{primitiveDataSpecToValueFactory(schema.getSpec(0))};
        var dataColumnSpecs =
            new DataColumnSpec[]{primitiveDataSpecToDataColumnSpec(schema.getSpec(0), m_newColumnName)};
        var dataTableSpec = new DataTableSpec(dataColumnSpecs);
        return ColumnarValueSchemaUtils.create(dataTableSpec, valueFactories);
    }

    int[] getInputColumnIndices() {
        return m_columnIndices;
    }

    private static ValueFactory<?, ?> primitiveDataSpecToValueFactory(final DataSpec spec) {
        // NB: These are all output types supported by expressions
        if (spec instanceof BooleanDataSpec) {
            return BooleanValueFactory.INSTANCE;
        } else if (spec instanceof LongDataSpec) {
            return LongValueFactory.INSTANCE;
        } else if (spec instanceof DoubleDataSpec) {
            return DoubleValueFactory.INSTANCE;
        } else if (spec instanceof StringDataSpec) {
            return StringValueFactory.INSTANCE;
        }
        throw new IllegalArgumentException("Cannot convert " + spec + " to ValueFactory");
    }

    /**
     * Turn a DataSpec (which we got by type inference from the AST) into a full-fledged DataColumnSpec
     *
     * @param spec
     * @param newColumnName
     * @return The corresponding DataColumnSpec
     */
    public static DataColumnSpec primitiveDataSpecToDataColumnSpec(final DataSpec spec, final String newColumnName) {
        // NB: These are all output types supported by expressions
        final DataType type;
        if (spec instanceof BooleanDataSpec) {
            type = BooleanCell.TYPE;
        } else if (spec instanceof LongDataSpec) {
            type = LongCell.TYPE;
        } else if (spec instanceof DoubleDataSpec) {
            type = DoubleCell.TYPE;
        } else if (spec instanceof StringDataSpec) {
            type = StringCell.TYPE;
        } else {
            throw new IllegalArgumentException("Cannot convert " + spec + " to DataColumnSpec");
        }
        return new DataColumnSpecCreator(newColumnName, type).createSpec();
    }

    /**
     * Persistor for ExpressionMapperFactory
     *
     * @author Carsten Haubold, KNIME GmbH, Konstanz, Germany
     */
    public static final class ExpressionMapperFactoryPersistor implements Persistor<ExpressionMapperFactory> {
        private static final String CFG_EXPRESSION = "expression";

        private static final String CFG_INPUT_SPEC = "inputSpec";

        private static final String CFG_COLUMN_NAME = "columnName";

        private static final String CFG_INPUT_VALUE_FACTORIES = "inputValueFactories";

        @Override
        public void save(final ExpressionMapperFactory factory, final NodeSettingsWO settings) {
            settings.addString(CFG_EXPRESSION, factory.m_expression);
            settings.addString(CFG_COLUMN_NAME, factory.m_newColumnName);
            factory.m_inputTableSchema.getSourceSpec().save(settings.addNodeSettings(CFG_INPUT_SPEC));

            // TODO: don't save and load the input schema but pass that in at load time
            // from the previous virtual table operation?
            var valueFactorySettings = settings.addNodeSettings(CFG_INPUT_VALUE_FACTORIES);
            for (int i = 0; i < factory.m_inputTableSchema.numColumns(); i++) {
                String key = Integer.toString(i);
                ValueFactoryUtils.saveValueFactory(factory.m_inputTableSchema.getValueFactory(i),
                    valueFactorySettings.addNodeSettings(key));
            }
        }

        @Override
        public ExpressionMapperFactory load(final NodeSettingsRO settings, final LoadContext context)
            throws InvalidSettingsException {
            var inputSpec = DataTableSpec.load(settings.getNodeSettings(CFG_INPUT_SPEC));

            var valueFactorySettings = settings.getNodeSettings(CFG_INPUT_VALUE_FACTORIES);
            var valueFactories = new ArrayList<ValueFactory<?, ?>>();
            // numColumns+1 because the DataTableSpec doesn't contain the RowID
            for (int i = 0; i < inputSpec.getNumColumns() + 1; i++) {
                String key = Integer.toString(i);
                var valueFactory = ValueFactoryUtils.loadValueFactory(valueFactorySettings.getNodeSettings(key),
                    context.getDataRepository());
                valueFactories.add(valueFactory);
            }

            var columnarValueSchema =
                ColumnarValueSchemaUtils.create(inputSpec, valueFactories.toArray(ValueFactory<?, ?>[]::new));
            return new ExpressionMapperFactory(settings.getString(CFG_EXPRESSION), columnarValueSchema,
                settings.getString(CFG_COLUMN_NAME));
        }

    }
}
