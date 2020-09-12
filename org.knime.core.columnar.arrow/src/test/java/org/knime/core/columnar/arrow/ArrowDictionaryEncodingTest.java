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
 */
package org.knime.core.columnar.arrow;

import static org.junit.Assert.assertEquals;

import java.nio.file.Files;

import org.junit.Test;
import org.knime.core.columnar.ColumnData;
import org.knime.core.columnar.ColumnDataSpec;
import org.knime.core.columnar.ColumnStore;
import org.knime.core.columnar.ColumnStoreSchema;
import org.knime.core.columnar.chunk.ColumnDataFactory;
import org.knime.core.columnar.chunk.ColumnDataReader;
import org.knime.core.columnar.chunk.ColumnDataWriter;
import org.knime.core.columnar.data.NullableWithCauseData;
import org.knime.core.columnar.data.StringData;

public class ArrowDictionaryEncodingTest extends AbstractArrowTest {

    @Test
    public void testDictionaryEncoding() throws Exception {
        int numChunks = 10;
        int chunkSize = 1_000;

        final ArrowColumnStoreFactory factory = new ArrowColumnStoreFactory();
        final ColumnStoreSchema schema = new ColumnStoreSchema() {

            @Override
            public int getNumColumns() {
                return 1;
            }

            @Override
            public ColumnDataSpec<?> getColumnDataSpec(final int idx) {
                return new StringData.StringDataSpec(true);
            }
        };

        final ColumnStore store =
            factory.createWriteStore(schema, Files.createTempFile("test", ".knarrow").toFile(), chunkSize);

        // let's store some data
        final ColumnDataWriter writer = store.getWriter();
        ColumnDataFactory fac = store.getFactory();
        for (int c = 0; c < numChunks; c++) {
            final ColumnData[] data = fac.create();
            final StringData cast = (StringData)data[0];
            for (int i = 0; i < chunkSize; i++) {
                cast.setString(i, "Test" + i * c);
            }
            cast.setNumValues(chunkSize);
            writer.write(data);
            cast.release();
        }
        writer.close();

        // let's read some data back
        final ColumnDataReader reader = store.createReader();
        for (int c = 0; c < numChunks; c++) {
            final ColumnData[] data = reader.read(c);
            final StringData cast = (StringData)data[0];
            for (int i = 0; i < chunkSize; i++) {
                assertEquals("Test" + i * c, cast.getString(i));
            }
            cast.release();
        }

        reader.close();
        store.close();
    }

    @Test
    public void testBinarySupplementWithDictEncoding() throws Exception {
        int numChunks = 32;
        int chunkSize = 17;

        ArrowColumnStoreFactory factory = new ArrowColumnStoreFactory();

        final ColumnStoreSchema schema = new ColumnStoreSchema() {

            @Override
            public int getNumColumns() {
                return 1;
            }

            @Override
            public ColumnDataSpec<?> getColumnDataSpec(final int idx) {
                return new NullableWithCauseData.NullableWithCauseDataSpec<>(new StringData.StringDataSpec(true));
            }
        };

        final ArrowColumnStore store =
            factory.createWriteStore(schema, Files.createTempFile("test", ".knarrow").toFile(), chunkSize);

        // let's store some data
        final ColumnDataWriter writer = store.getWriter();
        ColumnDataFactory dataFac = store.getFactory();
        for (int c = 0; c < numChunks; c++) {
            final ColumnData[] data = dataFac.create();
            @SuppressWarnings("unchecked")
            final NullableWithCauseData<StringData> cast = (NullableWithCauseData<StringData>)data[0];
            for (int i = 0; i < chunkSize; i++) {
                cast.getColumnData().setString(i, "Test " + i);
            }
            cast.setNumValues(chunkSize);
            writer.write(data);
            cast.release();
        }
        writer.close();

        // let's read some data back
        final ColumnDataReader reader = store.createReader();
        for (int c = 0; c < numChunks; c++) {
            final ColumnData[] data = reader.read(c);
            @SuppressWarnings("unchecked")
            final NullableWithCauseData<StringData> cast = (NullableWithCauseData<StringData>)data[0];
            for (int i = 0; i < chunkSize; i++) {
                assertEquals("Test " + i, cast.getColumnData().getString(i));
            }
            cast.release();
        }
        reader.close();
        store.close();
    }
}
