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
 *   Oct 13, 2022 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.core.data.columnar.table;

import java.util.function.BiFunction;

import org.apache.commons.lang3.NotImplementedException;
import org.knime.core.data.DataValue;
import org.knime.core.data.v2.ValueFactory;
import org.knime.core.data.v2.WriteValue;
import org.knime.core.table.access.BufferedAccesses;
import org.knime.core.table.access.DoubleAccess.DoubleReadAccess;
import org.knime.core.table.access.ReadAccess;
import org.knime.core.table.access.StringAccess.StringReadAccess;
import org.knime.core.table.access.WriteAccess;
import org.knime.core.table.schema.BooleanDataSpec;
import org.knime.core.table.schema.ByteDataSpec;
import org.knime.core.table.schema.DataSpec;
import org.knime.core.table.schema.DoubleDataSpec;
import org.knime.core.table.schema.FloatDataSpec;
import org.knime.core.table.schema.IntDataSpec;
import org.knime.core.table.schema.ListDataSpec;
import org.knime.core.table.schema.LongDataSpec;
import org.knime.core.table.schema.StringDataSpec;
import org.knime.core.table.schema.StructDataSpec;
import org.knime.core.table.schema.VarBinaryDataSpec;
import org.knime.core.table.schema.VoidDataSpec;

/**
 *
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 */
public class ValueExtractors {

    interface ValueExtractor {
        // marker
    }

    public static <W extends WriteAccess> ValueExtractor createExtractor(final ValueFactory<?, W> valueFactory) {
        var dataSpec = valueFactory.getSpec();
        var buffer = BufferedAccesses.createBufferedAccess(dataSpec);
        var writeValue = valueFactory.createWriteValue((W)buffer);
        return dataSpec.accept(ValueExtractorFactory.INSTANCE).apply(writeValue, buffer);
    }

    interface StringValueExtractor extends ValueExtractor {
        String getString(DataValue value);

        byte[] getBytes(DataValue value);
    }

    interface DoubleValueExtractor extends ValueExtractor {
        double getDouble(DataValue value);
    }

    private enum ValueExtractorFactory
        implements DataSpec.Mapper<BiFunction<WriteValue<?>, ReadAccess, ValueExtractor>> {
            INSTANCE;

        private static NotImplementedException notImplemented(final String type) {
            return new NotImplementedException(type + " extraction is not implemented.");
        }

        @Override
        public BiFunction<WriteValue<?>, ReadAccess, ValueExtractor> visit(final BooleanDataSpec spec) {
            throw notImplemented("boolean");
        }

        @Override
        public BiFunction<WriteValue<?>, ReadAccess, ValueExtractor> visit(final ByteDataSpec spec) {
            throw notImplemented("byte");
        }

        @Override
        public BiFunction<WriteValue<?>, ReadAccess, ValueExtractor> visit(final DoubleDataSpec spec) {
            return DoubleValueExtractorImpl::new;
        }

        @Override
        public BiFunction<WriteValue<?>, ReadAccess, ValueExtractor> visit(final FloatDataSpec spec) {
            throw notImplemented("float");
        }

        @Override
        public BiFunction<WriteValue<?>, ReadAccess, ValueExtractor> visit(final IntDataSpec spec) {
            throw notImplemented("int");
        }

        @Override
        public BiFunction<WriteValue<?>, ReadAccess, ValueExtractor> visit(final LongDataSpec spec) {
            throw notImplemented("long");
        }

        @Override
        public BiFunction<WriteValue<?>, ReadAccess, ValueExtractor> visit(final VarBinaryDataSpec spec) {
            throw notImplemented("varbinary");
        }

        @Override
        public BiFunction<WriteValue<?>, ReadAccess, ValueExtractor> visit(final VoidDataSpec spec) {
            throw notImplemented("void");
        }

        @Override
        public BiFunction<WriteValue<?>, ReadAccess, ValueExtractor> visit(final StructDataSpec spec) {
            throw notImplemented("struct");
        }

        @Override
        public BiFunction<WriteValue<?>, ReadAccess, ValueExtractor> visit(final ListDataSpec listDataSpec) {
            throw notImplemented("list");
        }

        @Override
        public BiFunction<WriteValue<?>, ReadAccess, ValueExtractor> visit(final StringDataSpec spec) {
            return StringValueExtractorImpl::new;
        }

    }

    private abstract static class AbstractValueExtractor<R extends ReadAccess> implements ValueExtractor {
        protected final WriteValue<?> m_writeValue;

        protected final R m_readAccess;

        public AbstractValueExtractor(final WriteValue<?> writeValue, final ReadAccess readAccess) {
            m_writeValue = writeValue;
            m_readAccess = (R)readAccess;
        }

        protected final void updateBuffer(final DataValue value) {
            setValue(m_writeValue, value);
        }

        private static <D extends DataValue> void setValue(final WriteValue<D> writeValue, final DataValue value) {
            writeValue.setValue((D)value);
        }
    }

    private static final class StringValueExtractorImpl extends AbstractValueExtractor<StringReadAccess>
        implements StringValueExtractor {

        StringValueExtractorImpl(final WriteValue<?> writeValue, final ReadAccess readAccess) {
            super(writeValue, readAccess);
        }

        @Override
        public String getString(final DataValue value) {
            updateBuffer(value);
            return m_readAccess.getStringValue();
        }

        @Override
        public byte[] getBytes(final DataValue value) {
            updateBuffer(value);
            return m_readAccess.getBytes();
        }
    }

    private static final class DoubleValueExtractorImpl extends AbstractValueExtractor<DoubleReadAccess> {

        public DoubleValueExtractorImpl(final WriteValue<?> writeValue, final ReadAccess readAccess) {
            super(writeValue, readAccess);
        }

        double getDouble(final DataValue cell) {
            updateBuffer(cell);
            return m_readAccess.getDoubleValue();
        }
    }
}
