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

import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.COLUMN_DATA_CACHE_SIZE_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.DOMAIN_CALC_NUM_THREADS_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.HEAP_CACHE_NAME_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.PERSIST_NUM_THREADS_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.SERIALIZE_NUM_THREADS_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.SMALL_TABLE_CACHE_SIZE_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceUtils.SMALL_TABLE_THRESHOLD_KEY;

import java.util.function.UnaryOperator;

import org.eclipse.jface.preference.ComboFieldEditor;
import org.eclipse.jface.preference.FieldEditorPreferencePage;
import org.eclipse.jface.preference.IntegerFieldEditor;
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

    private static final UnaryOperator<String> RESTART_MESSAGE_OPERATOR =
        s -> String.format("Changes to the %s require a restart of the workbenck to become effective.%n"
            + "Do you want to restart the workbench now?", s);

    private final int m_domainCalcNumThreads;

    private final String m_heapCacheName;

    private final int m_serializeNumThreads;

    private final int m_smallTableCacheSize;

    private final int m_columnDataCacheSize;

    private final int m_persistNumThreads;

    private IntegerFieldEditor m_columnDataCacheSizeEditor;

    private IntegerFieldEditor m_smallTableCacheSizeEditor;

    private IntegerFieldEditor m_smallTableThresholdEditor;

    private boolean m_apply;

    public ColumnarPreferencePage() {
        super(GRID);
        m_domainCalcNumThreads = ColumnarPreferenceUtils.getDomainCalcNumThreads();
        m_heapCacheName = ColumnarPreferenceUtils.getHeapCacheName();
        m_serializeNumThreads = ColumnarPreferenceUtils.getSerializeNumThreads();
        m_smallTableCacheSize = ColumnarPreferenceUtils.getSmallTableCacheSize();
        m_columnDataCacheSize = ColumnarPreferenceUtils.getColumnDataCacheSize();
        m_persistNumThreads = ColumnarPreferenceUtils.getPersistNumThreads();
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
        final int numAvailableProcessors = ColumnarPreferenceUtils.getNumAvailableProcessors();
        final int usablePhysicalMemorySizeMB = ColumnarPreferenceUtils.getUsablePhysicalMemorySizeMB();

        final IntegerFieldEditor domainCalcNumThreadsEditor = new IntegerFieldEditor(DOMAIN_CALC_NUM_THREADS_KEY,
            "Number of threads for calculating domains and checking for duplicate row keys", parent) {
            @Override
            protected void valueChanged() {
                super.valueChanged();
                if (isValid() && getIntValue() > numAvailableProcessors) {
                    showErrorMessage(String.format(
                        "Number of threads should not be larger than the number of available processors (%d).",
                        numAvailableProcessors));
                }

            }
        };
        domainCalcNumThreadsEditor.setValidRange(1, Integer.MAX_VALUE);
        addField(domainCalcNumThreadsEditor);

        final ComboFieldEditor heapCacheName =
            new ComboFieldEditor(HEAP_CACHE_NAME_KEY, "Caching strategy for complex data",
                new String[][]{{"Weak-referenced", ColumnarPreferenceUtils.HeapCache.WEAK.name()},
                    {"Soft-referenced memory-aware", ColumnarPreferenceUtils.HeapCache.SOFT.name()}},
                parent);
        addField(heapCacheName);

        final IntegerFieldEditor serializeNumThreadsEditor = new IntegerFieldEditor(SERIALIZE_NUM_THREADS_KEY,
            "Number of threads for serializing complex data", parent) {
            @Override
            protected void valueChanged() {
                super.valueChanged();
                if (isValid() && getIntValue() > numAvailableProcessors) {
                    showErrorMessage(String.format(
                        "Number of threads should not be larger than the number of available processors (%d).",
                        numAvailableProcessors));
                }

            }
        };
        serializeNumThreadsEditor.setValidRange(1, Integer.MAX_VALUE);
        addField(serializeNumThreadsEditor);

        m_smallTableCacheSizeEditor =
            new IntegerFieldEditor(SMALL_TABLE_CACHE_SIZE_KEY, "Size of small table cache (in MB)", parent) {
                @Override
                protected void valueChanged() {
                    super.valueChanged();
                    if (m_smallTableThresholdEditor.isValid() && isValid()
                        && m_smallTableThresholdEditor.getIntValue() > getIntValue()) {
                        showErrorMessage(
                            "Maximum small table size should not be larger than the size of the small table cache.");
                    }
                    if (cacheSizeExceeded(usablePhysicalMemorySizeMB)) {
                        showErrorMessage(String.format(
                            "Combined size of caches should not be larger than usable physical memory size (%d).",
                            usablePhysicalMemorySizeMB));
                    }
                }
            };
        addField(m_smallTableCacheSizeEditor);

        m_smallTableThresholdEditor =
            new IntegerFieldEditor(SMALL_TABLE_THRESHOLD_KEY, "Maximum size of small tables (in MB)", parent) {
                @Override
                protected void valueChanged() {
                    super.valueChanged();
                    if (isValid() && m_smallTableCacheSizeEditor.isValid()
                        && getIntValue() > m_smallTableCacheSizeEditor.getIntValue()) {
                        showErrorMessage(
                            "Maximum small table size should not be larger than the size of the small table cache.");
                    }
                }
            };
        m_smallTableThresholdEditor.setValidRange(0, Integer.MAX_VALUE / (1 << 20));
        addField(m_smallTableThresholdEditor);

        m_columnDataCacheSizeEditor =
            new IntegerFieldEditor(COLUMN_DATA_CACHE_SIZE_KEY, "Size of data cache (in MB)", parent) {
                @Override
                protected void valueChanged() {
                    super.valueChanged();
                    if (cacheSizeExceeded(usablePhysicalMemorySizeMB)) {
                        showErrorMessage(String.format(
                            "Combined size of caches should not be larger than usable physical memory size (%d).",
                            usablePhysicalMemorySizeMB));
                    }
                }
            };
        addField(m_columnDataCacheSizeEditor);

        final IntegerFieldEditor persistNumThreadsEditor =
            new IntegerFieldEditor(PERSIST_NUM_THREADS_KEY, "Number of threads for persisting data to disk", parent) {
                @Override
                protected void valueChanged() {
                    super.valueChanged();
                    if (isValid() && getIntValue() > numAvailableProcessors) {
                        showErrorMessage(String.format(
                            "Number of threads should not be larger than the number of available processors (%d).",
                            numAvailableProcessors));
                    }

                }
            };
        persistNumThreadsEditor.setValidRange(1, Integer.MAX_VALUE);
        addField(persistNumThreadsEditor);
    }

    private boolean cacheSizeExceeded(final int usablePhysicalMemorySizeMB) {
        return m_columnDataCacheSizeEditor.isValid() && m_smallTableCacheSizeEditor.isValid()
            && m_columnDataCacheSizeEditor.getIntValue()
                + m_smallTableCacheSizeEditor.getIntValue() > usablePhysicalMemorySizeMB;
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

        if (m_domainCalcNumThreads != ColumnarPreferenceUtils.getDomainCalcNumThreads()) {
            Display.getDefault().asyncExec(
                () -> promptRestartWithMessage(RESTART_MESSAGE_OPERATOR.apply("domain calculation thread pool size")));
            return;
        }

        if (!m_heapCacheName.equals(ColumnarPreferenceUtils.getHeapCacheName())) {
            Display.getDefault().asyncExec(
                () -> promptRestartWithMessage(RESTART_MESSAGE_OPERATOR.apply("heap cache")));
            return;
        }

        if (m_serializeNumThreads != ColumnarPreferenceUtils.getSerializeNumThreads()) {
            Display.getDefault().asyncExec(
                () -> promptRestartWithMessage(RESTART_MESSAGE_OPERATOR.apply("serialization thread pool size")));
            return;
        }

        if (m_smallTableCacheSize != ColumnarPreferenceUtils.getSmallTableCacheSize()) {
            Display.getDefault()
                .asyncExec(() -> promptRestartWithMessage(RESTART_MESSAGE_OPERATOR.apply("small table cache size")));
            return;
        }

        if (m_columnDataCacheSize != ColumnarPreferenceUtils.getColumnDataCacheSize()) {
            Display.getDefault()
                .asyncExec(() -> promptRestartWithMessage(RESTART_MESSAGE_OPERATOR.apply("data cache size")));
            return;
        }

        if (m_persistNumThreads != ColumnarPreferenceUtils.getPersistNumThreads()) {
            Display.getDefault().asyncExec(
                () -> promptRestartWithMessage(RESTART_MESSAGE_OPERATOR.apply("table persistence thread pool size")));
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
