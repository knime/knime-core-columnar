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
 *   2 Oct 2020 (Marc Bux, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.core.data.columnar.preferences;

import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.ASYNC_FLUSH_NUM_THREADS_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.CHUNK_SIZE_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.COLUMNAR_STORE;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.COLUMN_DATA_CACHE_SIZE_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.ENABLE_CACHED_STORE_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.ENABLE_PHANTOM_REFERENCE_STORE_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.ENABLE_SMALL_STORE_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.SMALL_TABLE_CACHE_SIZE_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.SMALL_TABLE_THRESHOLD_KEY;

import org.eclipse.jface.preference.BooleanFieldEditor;
import org.eclipse.jface.preference.FieldEditor;
import org.eclipse.jface.preference.FieldEditorPreferencePage;
import org.eclipse.jface.preference.IntegerFieldEditor;
import org.eclipse.jface.util.PropertyChangeEvent;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;
import org.eclipse.ui.PlatformUI;
import org.knime.core.ui.util.SWTUtilities;

@SuppressWarnings("javadoc")
public class ColumnarPreferencePage extends FieldEditorPreferencePage implements IWorkbenchPreferencePage {

    private final int m_asyncFlushNumThreads;

    private final int m_columnDataCacheSize;

    private final int m_smallTableCacheSize;

    private BooleanFieldEditor m_enableCachedStoreEditor;

    private IntegerFieldEditor m_asyncFlushNumThreadsEditor;

    private IntegerFieldEditor m_columnDataCacheSizeEditor;

    private BooleanFieldEditor m_enableSmallStoreEditor;

    private IntegerFieldEditor m_smallTableThresholdEditor;

    private IntegerFieldEditor m_smallTableCacheSizeEditor;

    private boolean m_apply;

    public ColumnarPreferencePage() {
        super(GRID);
        m_asyncFlushNumThreads = COLUMNAR_STORE.getInt(ASYNC_FLUSH_NUM_THREADS_KEY);
        m_columnDataCacheSize = COLUMNAR_STORE.getInt(COLUMN_DATA_CACHE_SIZE_KEY);
        m_smallTableCacheSize = COLUMNAR_STORE.getInt(SMALL_TABLE_CACHE_SIZE_KEY);
    }

    @Override
    public void init(final IWorkbench workbench) {
        setPreferenceStore(ColumnarPreferenceUtils.COLUMNAR_STORE);
        setDescription("Various configuration options for storing data of workflows "
            + "that are configured to use the columnar data storage.");
    }

    @Override
    protected void createFieldEditors() {
        final Composite parent = getFieldEditorParent();

        final IntegerFieldEditor chunkSizeEditor = new IntegerFieldEditor(CHUNK_SIZE_KEY, "Chunk Size", parent);
        chunkSizeEditor.setValidRange(1, Integer.MAX_VALUE);
        addField(chunkSizeEditor);

        final BooleanFieldEditor enablePhantomStoreEditor = new BooleanFieldEditor(ENABLE_PHANTOM_REFERENCE_STORE_KEY,
            "Release unreleased resources on garbage collection", parent);
        addField(enablePhantomStoreEditor);

        m_enableCachedStoreEditor =
            new BooleanFieldEditor(ENABLE_CACHED_STORE_KEY, "Hold least-recently-used data in memory", parent);
        addField(m_enableCachedStoreEditor);

        m_asyncFlushNumThreadsEditor = new IntegerFieldEditor(ASYNC_FLUSH_NUM_THREADS_KEY,
            "Number of threads for persisting data to disk", parent);
        m_asyncFlushNumThreadsEditor.setValidRange(1, Integer.MAX_VALUE);
        m_asyncFlushNumThreadsEditor.setEnabled(COLUMNAR_STORE.getBoolean(ENABLE_CACHED_STORE_KEY), parent);
        addField(m_asyncFlushNumThreadsEditor);

        m_columnDataCacheSizeEditor =
            new IntegerFieldEditor(COLUMN_DATA_CACHE_SIZE_KEY, "Size of data cache (in MB)", parent);
        m_columnDataCacheSizeEditor.setValidRange(1, Integer.MAX_VALUE);
        m_columnDataCacheSizeEditor.setEnabled(COLUMNAR_STORE.getBoolean(ENABLE_CACHED_STORE_KEY), parent);
        addField(m_columnDataCacheSizeEditor);

        m_enableSmallStoreEditor =
            new BooleanFieldEditor(ENABLE_SMALL_STORE_KEY, "Delay persistence of small tables", parent);
        addField(m_enableSmallStoreEditor);

        m_smallTableThresholdEditor =
            new IntegerFieldEditor(SMALL_TABLE_THRESHOLD_KEY, "Maximum size of small tables (in MB)", parent);
        m_smallTableThresholdEditor.setValidRange(1, Integer.MAX_VALUE / (1 << 20));
        m_smallTableThresholdEditor.setEnabled(COLUMNAR_STORE.getBoolean(ENABLE_SMALL_STORE_KEY), parent);
        addField(m_smallTableThresholdEditor);

        m_smallTableCacheSizeEditor =
            new IntegerFieldEditor(SMALL_TABLE_CACHE_SIZE_KEY, "Size of small table cache (in MB)", parent);
        m_smallTableCacheSizeEditor.setValidRange(1, Integer.MAX_VALUE);
        m_smallTableCacheSizeEditor.setEnabled(COLUMNAR_STORE.getBoolean(ENABLE_SMALL_STORE_KEY), parent);
        addField(m_smallTableCacheSizeEditor);
    }

    @Override
    public void propertyChange(final PropertyChangeEvent event) {
        super.propertyChange(event);
        if (event.getProperty().equals(FieldEditor.VALUE)) {
            final Composite parent = getFieldEditorParent();
            m_asyncFlushNumThreadsEditor.setEnabled(m_enableCachedStoreEditor.getBooleanValue(), parent);
            m_columnDataCacheSizeEditor.setEnabled(m_enableCachedStoreEditor.getBooleanValue(), parent);
            m_smallTableThresholdEditor.setEnabled(m_enableSmallStoreEditor.getBooleanValue(), parent);
            m_smallTableCacheSizeEditor.setEnabled(m_enableSmallStoreEditor.getBooleanValue(), parent);
        }
    }

    // code from here on out mostly copied from org.knime.workbench.ui.preferences.HeadlessPreferencePage
    @Override
    protected void performApply() {
        m_apply = true;
        // note that super.performApply() entails a call to performOk()
        super.performApply();
    }

    @Override
    public boolean performOk() {
        final boolean result = super.performOk();
        checkChanges();
        return result;
    }

    @Override
    public boolean performCancel() {
        final boolean result = super.performCancel();
        checkChanges();
        return result;
    }

    private void checkChanges() {
        boolean apply = m_apply;
        m_apply = false;

        // we have to return here since we do not want to proceed (yet) in case of Apply
        // we only want to proceed in case of OK or cancel
        if (apply) {
            return;
        }

        if (m_columnDataCacheSize != COLUMNAR_STORE.getInt(COLUMN_DATA_CACHE_SIZE_KEY)) {
            Display.getDefault()
                .asyncExec(() -> promptRestartWithMessage(
                    "Changes to the data cache size " + "require a restart of the workbenck to become effective. "
                        + "Do you want to restart the workbench now?"));
            return;
        }

        if (m_smallTableCacheSize != COLUMNAR_STORE.getInt(SMALL_TABLE_CACHE_SIZE_KEY)) {
            Display.getDefault()
                .asyncExec(() -> promptRestartWithMessage("Changes to the small table cache size "
                    + "require a restart of the workbenck to become effective. "
                    + "Do you want to restart the workbench now?"));
            return;
        }

        if (m_asyncFlushNumThreads != COLUMNAR_STORE.getInt(ASYNC_FLUSH_NUM_THREADS_KEY)) {
            Display.getDefault()
                .asyncExec(() -> promptRestartWithMessage("Changes to the table persistence thread pool size "
                    + "require a restart of the workbenck to become effective.\n"
                    + "Do you want to restart the workbench now?"));
            return;
        }
    }

    private static void promptRestartWithMessage(final String message) {
        final MessageBox mb = new MessageBox(SWTUtilities.getActiveShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO);
        mb.setText("Restart workbench...");
        mb.setMessage(message);
        if (mb.open() != SWT.YES) {
            return;
        }

        PlatformUI.getWorkbench().restart();
    }

}
