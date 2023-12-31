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

import static org.knime.core.data.columnar.preferences.ColumnarPreferenceInitializer.OFF_HEAP_MEM_LIMIT_KEY;
import static org.knime.core.data.columnar.preferences.ColumnarPreferenceInitializer.OFF_HEAP_MEM_LIMIT_MIN;

import java.util.function.UnaryOperator;

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

/**
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class ColumnarPreferencePage extends FieldEditorPreferencePage implements IWorkbenchPreferencePage {

    private static final UnaryOperator<String> RESTART_MESSAGE_OPERATOR =
        s -> String.format("Changes to the %s require a restart of the workbenck to become effective.%n"
            + "Do you want to restart the workbench now?", s);

    private final int m_activeOffHeapMemoryLimit;

    private boolean m_apply;

    /**
     * Constructor.
     */
    public ColumnarPreferencePage() {
        super(GRID);
        m_activeOffHeapMemoryLimit = ColumnarPreferenceUtils.getOffHeapMemoryLimitMB();
    }

    @Override
    public void init(final IWorkbench workbench) {
        setPreferenceStore(ColumnarPreferenceUtils.COLUMNAR_STORE);
        setDescription("Advanced configuration options for storing data of workflows "
            + "that are configured to use the columnar table backend:");
    }

    @Override
    protected void createFieldEditors() {
        final Composite parent = getFieldEditorParent();
        var offHeapMemoryLimitEditor =
            new IntegerFieldEditor(OFF_HEAP_MEM_LIMIT_KEY, "Off-Heap Memory Limit (in MB)", parent);
        offHeapMemoryLimitEditor.setValidRange(OFF_HEAP_MEM_LIMIT_MIN, Integer.MAX_VALUE);
        addField(offHeapMemoryLimitEditor);
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

        if (m_activeOffHeapMemoryLimit != ColumnarPreferenceUtils.getOffHeapMemoryLimitMB()) {
            Display.getDefault()
                .asyncExec(() -> promptRestartWithMessage(RESTART_MESSAGE_OPERATOR.apply("off-heap memory limit")));
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
