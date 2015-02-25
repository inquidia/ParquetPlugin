/*! ******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.openbi.kettle.plugins.parquetoutput;

import org.apache.avro.Schema;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Inquidia Consulting
 */
public class ParquetOutputDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = ParquetOutputMeta.class; // for i18n purposes, needed by Translator2!!

  private static final String[] YES_NO_COMBO = new String[] { BaseMessages.getString( PKG, "System.Combo.No" ),
    BaseMessages.getString( PKG, "System.Combo.Yes" ) };

  private CTabFolder wTabFolder;
  private FormData fdTabFolder;

  private CTabItem wFileTab, wContentTab, wFieldsTab;

  private FormData fdFileComp, fdContentComp, fdFieldsComp;

  private Label wlFilename;
  private Button wbFilename;
  private TextVar wFilename;
  private FormData fdlFilename, fdbFilename, fdFilename;

  private Label wlFilenameInField;
  private Button wFilenameInField;
  private FormData fdlFilenameInField, fdFilenameInField;

  private Label wlFilenameField;
  private ComboVar wFilenameField;
  private FormData fdlFilenameField, fdFilenameField;

  private Label wlCreateParentFolder;
  private Button wCreateParentFolder;
  private FormData fdlCreateParentFolder, fdCreateParentFolder;

  private Label wlCleanOutput;
  private Button wCleanOutput;
  private FormData fdlCleanOutput, fdCleanOutput;

  private Label wlAddStepnr;
  private Button wAddStepnr;
  private FormData fdlAddStepnr, fdAddStepnr;

  private Label wlAddPartnr;
  private Button wAddPartnr;
  private FormData fdlAddPartnr, fdAddPartnr;

  private Label wlAddDate;
  private Button wAddDate;
  private FormData fdlAddDate, fdAddDate;

  private Label wlAddTime;
  private Button wAddTime;
  private FormData fdlAddTime, fdAddTime;

  private Label wlSpecifyFormat;
  private Button wSpecifyFormat;
  private FormData fdlSpecifyFormat, fdSpecifyFormat;

  private Label wlDateTimeFormat;
  private ComboVar wDateTimeFormat;
  private FormData fdlDateTimeFormat, fdDateTimeFormat;

  private Label wlAddToResult;
  private Button wAddToResult;
  private FormData fdlAddToResult, fdAddToResult;

  private Label wlBlockSize;
  private TextVar wBlockSize;
  private FormData fdlBlockSize, fdBlockSize;

  private Label wlPageSize;
  private TextVar wPageSize;
  private FormData fdlPageSize, fdPageSize;

  private Label wlCompression;
  private ComboVar wCompression;
  private FormData fdlCompression, fdCompression;

  private Label wlDictionaryCompression;
  private Button wDictionaryCompression;
  private FormData fdlDictionaryCompression, fdDictionaryCompression;

  private TableView wFields;
  private FormData fdFields;

  private ParquetOutputMeta input;

  private ColumnInfo[] colinf;

  private Link wDevelopedBy;
  private FormData fdDevelopedBy;

  private Map<String, Integer> inputFields;

  private Schema avroSchema;
  private boolean validSchema = false;
  private String[] avroFieldNames = null;

  private boolean gotPreviousFields = false;

  public ParquetOutputDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (ParquetOutputMeta) in;
    inputFields = new HashMap<String, Integer>();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "System.Label.StepName" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.top = new FormAttachment( 0, margin );
    fdlStepname.right = new FormAttachment( middle, -margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );

    // ////////////////////////
    // START OF FILE TAB///
    // /
    wFileTab = new CTabItem( wTabFolder, SWT.NONE );
    wFileTab.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.FileTab.TabTitle" ) );

    Composite wFileComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFileComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout( fileLayout );

    // Filename line
    wlFilename = new Label( wFileComp, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Filename.Label" ) );
    props.setLook( wlFilename );
    fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( 0, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData( fdlFilename );

    wbFilename = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename );
    wbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( 0, 0 );
    wbFilename.setLayoutData( fdbFilename );

    wFilename = new TextVar( transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( 0, margin );
    fdFilename.right = new FormAttachment( wbFilename, -margin );
    wFilename.setLayoutData( fdFilename );
    

    // Create Schema File
    //
    wlFilenameInField = new Label( wFileComp, SWT.RIGHT );
    wlFilenameInField.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.FilenameInField.Label" ) );
    props.setLook( wlFilenameInField );
    fdlFilenameInField = new FormData();
    fdlFilenameInField.left = new FormAttachment( 0, 0 );
    fdlFilenameInField.top = new FormAttachment( wFilename, margin );
    fdlFilenameInField.right = new FormAttachment( middle, -margin );
    wlFilenameInField.setLayoutData( fdlFilenameInField );

    wFilenameInField = new Button( wFileComp, SWT.CHECK );
    props.setLook( wFilenameInField );
    fdFilenameInField = new FormData();
    fdFilenameInField.left = new FormAttachment( middle, 0 );
    fdFilenameInField.top = new FormAttachment( wFilename, margin );
    fdFilenameInField.right = new FormAttachment( 100, 0 );
    wFilenameInField.setLayoutData( fdFilenameInField );
    wFilenameInField.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setFilenameInField();
      }
    } );

    // input Field Line
    wlFilenameField = new Label( wFileComp, SWT.RIGHT );
    wlFilenameField.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.FilenameField.Label" ) );
    props.setLook( wlFilenameField );
    fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment( 0, 0 );
    fdlFilenameField.right = new FormAttachment( middle, -margin );
    fdlFilenameField.top = new FormAttachment( wFilenameInField, margin );
    wlFilenameField.setLayoutData( fdlFilenameField );

    wFilenameField = new ComboVar( transMeta, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilenameField );
    wFilenameField.addModifyListener( lsMod );
    fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment( middle, 0 );
    fdFilenameField.top = new FormAttachment( wFilenameInField, margin );
    fdFilenameField.right = new FormAttachment( 100, 0 );
    wFilenameField.setLayoutData( fdFilenameField );
    wFilenameField.setEnabled( false );
    wFilenameField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        getFields();
        shell.setCursor( null );
        busy.dispose();
      }
    } );
    /* End */

    // Clean Output
    //
    wlCleanOutput = new Label( wFileComp, SWT.RIGHT );
    wlCleanOutput.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.CleanOutput.Label" ) );
    props.setLook( wlCleanOutput );
    fdlCleanOutput = new FormData();
    fdlCleanOutput.left = new FormAttachment( 0, 0 );
    fdlCleanOutput.top = new FormAttachment( wFilenameField, margin );
    fdlCleanOutput.right = new FormAttachment( middle, -margin );
    wlCleanOutput.setLayoutData( fdlCleanOutput );

    wCleanOutput = new Button( wFileComp, SWT.CHECK );
    wCleanOutput.setToolTipText( BaseMessages.getString( PKG, "ParquetOutputDialog.CleanOutput.Tooltip" ) );
    props.setLook( wCleanOutput );
    fdCleanOutput = new FormData();
    fdCleanOutput.left = new FormAttachment( middle, 0 );
    fdCleanOutput.top = new FormAttachment( wFilenameField, margin );
    fdCleanOutput.right = new FormAttachment( 100, 0 );
    wCleanOutput.setLayoutData( fdCleanOutput );
    wCleanOutput.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Create Parent Folder?
    //
    wlCreateParentFolder = new Label( wFileComp, SWT.RIGHT );
    wlCreateParentFolder.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.CreateParentFolder.Label" ) );
    props.setLook( wlCreateParentFolder );
    fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment( 0, 0 );
    fdlCreateParentFolder.top = new FormAttachment( wCleanOutput, margin );
    fdlCreateParentFolder.right = new FormAttachment( middle, -margin );
    wlCreateParentFolder.setLayoutData( fdlCreateParentFolder );

    wCreateParentFolder = new Button( wFileComp, SWT.CHECK );
    wCreateParentFolder.setToolTipText( BaseMessages.getString( PKG, "ParquetOutputDialog.CreateParentFolder.Tooltip" ) );
    props.setLook( wCreateParentFolder );
    fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment( middle, 0 );
    fdCreateParentFolder.top = new FormAttachment( wCleanOutput, margin );
    fdCreateParentFolder.right = new FormAttachment( 100, 0 );
    wCreateParentFolder.setLayoutData( fdCreateParentFolder );
    wCreateParentFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Create multi-part file?
    wlAddStepnr = new Label( wFileComp, SWT.RIGHT );
    wlAddStepnr.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.AddStepnr.Label" ) );
    props.setLook( wlAddStepnr );
    fdlAddStepnr = new FormData();
    fdlAddStepnr.left = new FormAttachment( 0, 0 );
    fdlAddStepnr.top = new FormAttachment( wCreateParentFolder, margin );
    fdlAddStepnr.right = new FormAttachment( middle, -margin );
    wlAddStepnr.setLayoutData( fdlAddStepnr );

    wAddStepnr = new Button( wFileComp, SWT.CHECK );
    props.setLook( wAddStepnr );
    fdAddStepnr = new FormData();
    fdAddStepnr.left = new FormAttachment( middle, 0 );
    fdAddStepnr.top = new FormAttachment( wCreateParentFolder, margin );
    fdAddStepnr.right = new FormAttachment( 100, 0 );
    wAddStepnr.setLayoutData( fdAddStepnr );
    wAddStepnr.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Create multi-part file?
    wlAddPartnr = new Label( wFileComp, SWT.RIGHT );
    wlAddPartnr.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.AddPartnr.Label" ) );
    props.setLook( wlAddPartnr );
    fdlAddPartnr = new FormData();
    fdlAddPartnr.left = new FormAttachment( 0, 0 );
    fdlAddPartnr.top = new FormAttachment( wAddStepnr, margin );
    fdlAddPartnr.right = new FormAttachment( middle, -margin );
    wlAddPartnr.setLayoutData( fdlAddPartnr );

    wAddPartnr = new Button( wFileComp, SWT.CHECK );
    props.setLook( wAddPartnr );
    fdAddPartnr = new FormData();
    fdAddPartnr.left = new FormAttachment( middle, 0 );
    fdAddPartnr.top = new FormAttachment( wAddStepnr, margin );
    fdAddPartnr.right = new FormAttachment( 100, 0 );
    wAddPartnr.setLayoutData( fdAddPartnr );
    wAddPartnr.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Create multi-part file?
    wlAddDate = new Label( wFileComp, SWT.RIGHT );
    wlAddDate.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.AddDate.Label" ) );
    props.setLook( wlAddDate );
    fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment( 0, 0 );
    fdlAddDate.top = new FormAttachment( wAddPartnr, margin );
    fdlAddDate.right = new FormAttachment( middle, -margin );
    wlAddDate.setLayoutData( fdlAddDate );

    wAddDate = new Button( wFileComp, SWT.CHECK );
    props.setLook( wAddDate );
    fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment( middle, 0 );
    fdAddDate.top = new FormAttachment( wAddPartnr, margin );
    fdAddDate.right = new FormAttachment( 100, 0 );
    wAddDate.setLayoutData( fdAddDate );
    wAddDate.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        // System.out.println("wAddDate.getSelection()="+wAddDate.getSelection());
      }
    } );
    // Create multi-part file?
    wlAddTime = new Label( wFileComp, SWT.RIGHT );
    wlAddTime.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.AddTime.Label" ) );
    props.setLook( wlAddTime );
    fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment( 0, 0 );
    fdlAddTime.top = new FormAttachment( wAddDate, margin );
    fdlAddTime.right = new FormAttachment( middle, -margin );
    wlAddTime.setLayoutData( fdlAddTime );

    wAddTime = new Button( wFileComp, SWT.CHECK );
    props.setLook( wAddTime );
    fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment( middle, 0 );
    fdAddTime.top = new FormAttachment( wAddDate, margin );
    fdAddTime.right = new FormAttachment( 100, 0 );
    wAddTime.setLayoutData( fdAddTime );
    wAddTime.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Specify date time format?
    wlSpecifyFormat = new Label( wFileComp, SWT.RIGHT );
    wlSpecifyFormat.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.SpecifyFormat.Label" ) );
    props.setLook( wlSpecifyFormat );
    fdlSpecifyFormat = new FormData();
    fdlSpecifyFormat.left = new FormAttachment( 0, 0 );
    fdlSpecifyFormat.top = new FormAttachment( wAddTime, margin );
    fdlSpecifyFormat.right = new FormAttachment( middle, -margin );
    wlSpecifyFormat.setLayoutData( fdlSpecifyFormat );

    wSpecifyFormat = new Button( wFileComp, SWT.CHECK );
    props.setLook( wSpecifyFormat );
    wSpecifyFormat.setToolTipText( BaseMessages.getString( PKG, "ParquetOutputDialog.SpecifyFormat.Tooltip" ) );
    fdSpecifyFormat = new FormData();
    fdSpecifyFormat.left = new FormAttachment( middle, 0 );
    fdSpecifyFormat.top = new FormAttachment( wAddTime, margin );
    fdSpecifyFormat.right = new FormAttachment( 100, 0 );
    wSpecifyFormat.setLayoutData( fdSpecifyFormat );
    wSpecifyFormat.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setDateTimeFormat();
      }
    } );

    // DateTimeFormat
    wlDateTimeFormat = new Label( wFileComp, SWT.RIGHT );
    wlDateTimeFormat.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.DateTimeFormat.Label" ) );
    props.setLook( wlDateTimeFormat );
    fdlDateTimeFormat = new FormData();
    fdlDateTimeFormat.left = new FormAttachment( 0, 0 );
    fdlDateTimeFormat.top = new FormAttachment( wSpecifyFormat, margin );
    fdlDateTimeFormat.right = new FormAttachment( middle, -margin );
    wlDateTimeFormat.setLayoutData( fdlDateTimeFormat );

    wDateTimeFormat = new ComboVar( transMeta, wFileComp, SWT.BORDER | SWT.READ_ONLY );
    wDateTimeFormat.setEditable( true );
    props.setLook( wDateTimeFormat );
    wDateTimeFormat.addModifyListener( lsMod );
    fdDateTimeFormat = new FormData();
    fdDateTimeFormat.left = new FormAttachment( middle, 0 );
    fdDateTimeFormat.top = new FormAttachment( wSpecifyFormat, margin );
    fdDateTimeFormat.right = new FormAttachment( 75, 0 );
    wDateTimeFormat.setLayoutData( fdDateTimeFormat );
    String[] dats = Const.getDateFormats();
    for ( String dat : dats ) {
      wDateTimeFormat.add( dat );
    }

    // Add File to the result files name
    wlAddToResult = new Label( wFileComp, SWT.RIGHT );
    wlAddToResult.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.AddFileToResult.Label" ) );
    props.setLook( wlAddToResult );
    fdlAddToResult = new FormData();
    fdlAddToResult.left = new FormAttachment( 0, 0 );
    fdlAddToResult.top = new FormAttachment( wDateTimeFormat, 2 * margin );
    fdlAddToResult.right = new FormAttachment( middle, -margin );
    wlAddToResult.setLayoutData( fdlAddToResult );

    wAddToResult = new Button( wFileComp, SWT.CHECK );
    wAddToResult.setToolTipText( BaseMessages.getString( PKG, "ParquetOutputDialog.AddFileToResult.Tooltip" ) );
    props.setLook( wAddToResult );
    fdAddToResult = new FormData();
    fdAddToResult.left = new FormAttachment( middle, 0 );
    fdAddToResult.top = new FormAttachment( wDateTimeFormat, 2 * margin );
    fdAddToResult.right = new FormAttachment( 100, 0 );
    wAddToResult.setLayoutData( fdAddToResult );
    SelectionAdapter lsSelR = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wAddToResult.addSelectionListener( lsSelR );

    fdFileComp = new FormData();
    fdFileComp.left = new FormAttachment( 0, 0 );
    fdFileComp.top = new FormAttachment( 0, 0 );
    fdFileComp.right = new FormAttachment( 100, 0 );
    fdFileComp.bottom = new FormAttachment( 100, 0 );
    wFileComp.setLayoutData( fdFileComp );

    wFileComp.layout();
    wFileTab.setControl( wFileComp );

    // ///////////////////////////////////////////////////////////
    // / END OF FILE TAB
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////////////////////////////////
    // / START OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

    wContentTab = new CTabItem( wTabFolder, SWT.NONE );
    wContentTab.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.ContentTab.TabTitle" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = Const.FORM_MARGIN;
    contentLayout.marginHeight = Const.FORM_MARGIN;

    Composite wContentComp = new Composite( wTabFolder, SWT.NONE );
    wContentComp.setLayout( contentLayout );
    props.setLook( wContentComp );

        // Block size Line
    wlBlockSize = new Label( wContentComp, SWT.RIGHT );
    wlBlockSize.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.BlockSize.Label" ) );
    props.setLook( wlBlockSize );
    fdlBlockSize = new FormData();
    fdlBlockSize.left = new FormAttachment( 0, 0 );
    fdlBlockSize.right = new FormAttachment( middle, -margin );
    fdlBlockSize.top = new FormAttachment( 0, margin );
    wlBlockSize.setLayoutData( fdlBlockSize );

    wBlockSize = new TextVar( transMeta, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBlockSize );
    wBlockSize.addModifyListener( lsMod );
    fdBlockSize = new FormData();
    fdBlockSize.left = new FormAttachment( middle, 0 );
    fdBlockSize.top = new FormAttachment( 0, margin );
    fdBlockSize.right = new FormAttachment( 100, 0 );
    wBlockSize.setLayoutData( fdBlockSize );
    wBlockSize.addModifyListener( lsMod );
    /* End */

    // Page size Line
    wlPageSize = new Label( wContentComp, SWT.RIGHT );
    wlPageSize.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.PageSize.Label" ) );
    props.setLook( wlPageSize );
    fdlPageSize = new FormData();
    fdlPageSize.left = new FormAttachment( 0, 0 );
    fdlPageSize.right = new FormAttachment( middle, -margin );
    fdlPageSize.top = new FormAttachment( wBlockSize, margin );
    wlPageSize.setLayoutData( fdlPageSize );

    wPageSize = new TextVar( transMeta, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPageSize );
    wPageSize.addModifyListener( lsMod );
    fdPageSize = new FormData();
    fdPageSize.left = new FormAttachment( middle, 0 );
    fdPageSize.top = new FormAttachment( wBlockSize, margin );
    fdPageSize.right = new FormAttachment( 100, 0 );
    wPageSize.setLayoutData( fdPageSize );
    wPageSize.addModifyListener( lsMod );
    /* End */

    // Compression
    //
    // DateTimeFormat
    wlCompression = new Label( wContentComp, SWT.RIGHT );
    wlCompression.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Compression.Label" ) );
    props.setLook( wlCompression );
    fdlCompression = new FormData();
    fdlCompression.left = new FormAttachment( 0, 0 );
    fdlCompression.top = new FormAttachment( wPageSize, margin );
    fdlCompression.right = new FormAttachment( middle, -margin );
    wlCompression.setLayoutData( fdlCompression );

    wCompression = new ComboVar( transMeta, wContentComp, SWT.BORDER | SWT.READ_ONLY );
    wCompression.setEditable( true );
    props.setLook( wCompression );
    wCompression.addModifyListener( lsMod );
    fdCompression = new FormData();
    fdCompression.left = new FormAttachment( middle, 0 );
    fdCompression.top = new FormAttachment( wPageSize, margin );
    fdCompression.right = new FormAttachment( 75, 0 );
    wCompression.setLayoutData( fdCompression );
    String[] compressions = ParquetOutputMeta.compressionTypes;
    for ( String compression : compressions ) {
      wCompression.add( compression );
    }

    // Use configured block size line
    wlDictionaryCompression = new Label( wContentComp, SWT.RIGHT );
    wlDictionaryCompression.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.DictionaryCompression.Label" ) );
    props.setLook( wlDictionaryCompression );
    fdlDictionaryCompression = new FormData();
    fdlDictionaryCompression.left = new FormAttachment( 0, 0 );
    fdlDictionaryCompression.top = new FormAttachment( wCompression, margin );
    fdlDictionaryCompression.right = new FormAttachment( middle, -margin );
    wlDictionaryCompression.setLayoutData( fdlDictionaryCompression );

    wDictionaryCompression = new Button( wContentComp, SWT.CHECK );
    wDictionaryCompression.setToolTipText( BaseMessages.getString( PKG, "ParquetOutputDialog.DictionaryCompression.Tooltip" ) );
    props.setLook( wDictionaryCompression );
    fdDictionaryCompression = new FormData();
    fdDictionaryCompression.left = new FormAttachment( middle, 0 );
    fdDictionaryCompression.top = new FormAttachment( wCompression, margin );
    fdDictionaryCompression.right = new FormAttachment( 100, 0 );
    wDictionaryCompression.setLayoutData( fdDictionaryCompression );
    wDictionaryCompression.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    } );

    fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment( 0, 0 );
    fdContentComp.top = new FormAttachment( 0, 0 );
    fdContentComp.right = new FormAttachment( 100, 0 );
    fdContentComp.bottom = new FormAttachment( 100, 0 );
    wContentComp.setLayoutData( fdContentComp );

    wContentComp.layout();
    wContentTab.setControl( wContentComp );

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////



    // Fields tab...
    //
    wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.FieldsTab.TabTitle" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook( wFieldsComp );

    wGet = new Button( wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "System.Button.GetFields" ) );
    wGet.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.GetFields" ) );
    fdGet = new FormData();
    fdGet.right = new FormAttachment( 50, -4 * margin );
    fdGet.bottom = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    final int FieldsCols = 3;
    final int FieldsRows = input.getOutputFields().length;

    colinf = new ColumnInfo[FieldsCols];
    colinf[0] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ParquetOutputDialog.StreamColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    colinf[1] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ParquetOutputDialog.Path.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[2] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "ParquetOutputDialog.Nullable.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, YES_NO_COMBO, false );

     wFields =
      new TableView(
        transMeta, wFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, -margin );
    wFields.setLayoutData( fdFields );

    //
    // Search the fields in the background

    final Runnable runnable = new Runnable() {
      public void run() {
        StepMeta stepMeta = transMeta.findStep( stepname );
        if ( stepMeta != null ) {
          try {
            RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );

            // Remember these fields...
            for ( int i = 0; i < row.size(); i++ ) {
              inputFields.put( row.getValueMeta( i ).getName(), i );
            }
            setComboBoxes();
          } catch ( KettleException e ) {
            logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
          }
        }
      }
    };
    new Thread( runnable ).start();

    fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fdFieldsComp );

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wStepname, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fdTabFolder );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, wTabFolder );

    wDevelopedBy = new Link( shell, SWT.PUSH );
    wDevelopedBy.setText("Developed by Inquidia Consulting (<a href=\"http://www.inquidia.com\">www.inquidia.com</a>)");
    fdDevelopedBy = new FormData();
    fdDevelopedBy.right = new FormAttachment( 100, margin );
    fdDevelopedBy.bottom = new FormAttachment( 100, margin );
    wDevelopedBy.setLayoutData( fdDevelopedBy );
    wDevelopedBy.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        Program.launch("http://www.inquidia.com");
      }
    } );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        get();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wGet.addListener( SWT.Selection, lsGet );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wFilename.addSelectionListener( lsDef );
    wBlockSize.addSelectionListener( lsDef );
    wPageSize.addSelectionListener( lsDef );

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wFilename.setToolTipText( transMeta.environmentSubstitute( wFilename.getText() ) );
      }
    } );

    /* TODO: Add VFS file chooser. */
    wbFilename.addSelectionListener( new SelectionAdapter() {
          public void widgetSelected( SelectionEvent e ) {
            try {
              // Setup file type filtering
              String[] fileFilters = null;
              String[] fileFilterNames = null;

              fileFilters = new String[] { "*" };
              fileFilterNames =
                new String[] { BaseMessages.getString( PKG, "System.FileType.AllFiles" ) };


              // Get current file
              FileObject rootFile = null;
              FileObject initialFile = null;
              FileObject defaultInitialFile = null;

              if ( wFilename.getText() != null ) {
                String fileName = transMeta.environmentSubstitute( wFilename.getText() );

                if ( fileName != null && !fileName.equals( "" ) ) {
                  try {
                    initialFile = KettleVFS.getFileObject( fileName );
                    rootFile = initialFile.getFileSystem().getRoot();
                    defaultInitialFile = initialFile;
                  } catch ( Exception ex ) {
                    // Ignore, unable to obtain initial file, use default
                  }
                }
              }

              if ( rootFile == null ) {
                defaultInitialFile = KettleVFS.getFileObject( Spoon.getInstance().getLastFileOpened() );
                rootFile = defaultInitialFile.getFileSystem().getRoot();
                initialFile = defaultInitialFile;
              }

              VfsFileChooserDialog fileChooserDialog = Spoon.getInstance().getVfsFileChooserDialog( rootFile, initialFile );
              fileChooserDialog.defaultInitialFile = defaultInitialFile;

              FileObject selectedFile =
                fileChooserDialog.open( shell, null, HadoopSpoonPlugin.HDFS_SCHEME, true, null, fileFilters,
                  fileFilterNames, VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE_OR_DIRECTORY );
              if ( selectedFile != null ) {
                wFilename.setText( selectedFile.getURL().toString() );
              }
            } catch ( KettleFileException ex ) {
              log.logError( BaseMessages.getString( PKG, "HadoopFileInputDialog.FileBrowser.KettleFileException" ) );
            } catch ( FileSystemException ex ) {
              log.logError( BaseMessages.getString( PKG, "HadoopFileInputDialog.FileBrowser.FileSystemException" ) );
            }
          }
        }  );


    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    lsResize = new Listener() {
      public void handleEvent( Event event ) {
        Point size = shell.getSize();
        wFields.setSize( size.x - 10, size.y - 50 );
        wFields.table.setSize( size.x - 10, size.y - 50 );
        wFields.redraw();
      }
    };
    shell.addListener( SWT.Resize, lsResize );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );
    setFilenameInField();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  private void getFields() {
    if ( !gotPreviousFields ) {
      try {
        String inputFilenameField = wFilenameField.getText();

        RowMetaInterface r = transMeta.getPrevStepFields( stepname );
        if ( r != null ) {
          wFilenameField.setItems( r.getFieldNames() );
        }

        wFilenameField.setText( Const.NVL( inputFilenameField, "" ) );

      } catch ( KettleException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "TokenReplacementDialog.FailedToGetFields.DialogTitle" ),
          BaseMessages.getString( PKG, "TokenReplacementDialog.FailedToGetFields.DialogMessage" ), ke );
      }
      gotPreviousFields = true;
    }
  }


  protected void setFilenameInField() {
    boolean filenameInField = wFilenameInField.getSelection();

    wlFilenameField.setEnabled( filenameInField );
    wFilenameField.setEnabled( filenameInField );
    wlFilename.setEnabled( !filenameInField );
    wFilename.setEnabled( !filenameInField );
    wbFilename.setEnabled( !filenameInField );

    wlAddStepnr.setEnabled( !filenameInField );
    wAddStepnr.setEnabled( !filenameInField );
    wlAddPartnr.setEnabled( !filenameInField );
    wAddPartnr.setEnabled( !filenameInField );
    wlAddDate.setEnabled( !filenameInField  && !wSpecifyFormat.getSelection() );
    wAddDate.setEnabled( !filenameInField  && !wSpecifyFormat.getSelection() );
    wlAddTime.setEnabled( !filenameInField  && !wSpecifyFormat.getSelection() );
    wAddTime.setEnabled( !filenameInField  && !wSpecifyFormat.getSelection() );
    wlSpecifyFormat.setEnabled( !filenameInField );
    wSpecifyFormat.setEnabled( !filenameInField );
    wlDateTimeFormat.setEnabled( !filenameInField && wSpecifyFormat.getSelection() );
    wDateTimeFormat.setEnabled( !filenameInField && wSpecifyFormat.getSelection() );
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<String, Integer>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<String>( keySet );

    String[] fieldNames = entries.toArray( new String[entries.size()] );

    Const.sortStrings( fieldNames );
    colinf[0].setComboValues( fieldNames );
  }

  private void setDateTimeFormat() {
    if ( wSpecifyFormat.getSelection() ) {
      wAddDate.setSelection( false );
      wAddTime.setSelection( false );
    }

    wDateTimeFormat.setEnabled( wSpecifyFormat.getSelection() );
    wlDateTimeFormat.setEnabled( wSpecifyFormat.getSelection() );
    wAddDate.setEnabled( !( wSpecifyFormat.getSelection() ) );
    wlAddDate.setEnabled( !( wSpecifyFormat.getSelection() ) );
    wAddTime.setEnabled( !( wSpecifyFormat.getSelection() ) );
    wlAddTime.setEnabled( !( wSpecifyFormat.getSelection() ) );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wFilename.setText( Const.NVL( input.getFilename(), "" ) );
    wFilenameInField.setSelection( input.isAcceptFilenameFromField() );
    wFilenameField.setText( Const.NVL( input.getFilenameField(), "" ) );
    wCleanOutput.setSelection( input.isCleanOutput() );
    wCreateParentFolder.setSelection( input.isCreateParentFolder() );
    wAddStepnr.setSelection( input.isStepNrInFilename() );
    wAddPartnr.setSelection( input.isPartNrInFilename() );
    wAddDate.setSelection( input.isDateInFilename() );
    wAddTime.setSelection( input.isTimeInFilename() );
    wSpecifyFormat.setSelection( input.isSpecifyFormat() );
    wDateTimeFormat.setText( Const.NVL( input.getDateTimeFormat(), "" ) );
    wAddToResult.setSelection( input.isAddToResult() );

    wBlockSize.setText( Const.NVL( input.getBlockSize(), "" ) );
    wPageSize.setText( Const.NVL( input.getPageSize(), "" ) );
    wCompression.setText( Const.NVL( input.getCompressionCodec(), "none" ) );
    wDictionaryCompression.setSelection( input.isEnableDictionaryCompression() );

    logDebug( "getting fields info..." );

    for ( int i = 0; i < input.getOutputFields().length; i++ ) {
      ParquetOutputField field = input.getOutputFields()[i];

      TableItem item = wFields.table.getItem( i );
      if ( field.getName() != null ) {
        item.setText( 1, field.getName() );
      }
      if( field.getPath() != null ) {
    	item.setText( 2, field.getPath() );
      }
      if( field.getNullable() )
      {
        item.setText( 3, "Y" );
      } else {
        item.setText( 3, "N" );
      }
    }

    wFields.optWidth( true );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    avroSchema = null;

    input.setChanged( backupChanged );

    dispose();
  }

  private void getInfo( ParquetOutputMeta tfoi ) {
    tfoi.setFilename( wFilename.getText() );
    tfoi.setAcceptFilenameFromField( wFilenameInField.getSelection() );
    tfoi.setFilenameField( wFilenameField.getText() );
    tfoi.setCleanOutput( wCleanOutput.getSelection() );
    tfoi.setCreateParentFolder( wCreateParentFolder.getSelection() );
    tfoi.setStepNrInFilename( wAddStepnr.getSelection() );
    tfoi.setPartNrInFilename( wAddPartnr.getSelection() );
    tfoi.setDateInFilename( wAddDate.getSelection() );
    tfoi.setTimeInFilename( wAddTime.getSelection() );
    tfoi.setSpecifyFormat( wSpecifyFormat.getSelection() );
    tfoi.setDateTimeFormat( wDateTimeFormat.getText() );
    tfoi.setAddToResult( wAddToResult.getSelection() );

    tfoi.setBlockSize( wBlockSize.getText() );
    tfoi.setPageSize( wPageSize.getText() );
    tfoi.setCompressionCodec( wCompression.getText() );
    tfoi.setEnableDictionaryCompression( wDictionaryCompression.getSelection() );

    int i;
    // Table table = wFields.table;

    int nrfields = wFields.nrNonEmpty();

    tfoi.allocate( nrfields );

    for ( i = 0; i < nrfields; i++ ) {
      ParquetOutputField field = new ParquetOutputField();

      TableItem item = wFields.getNonEmpty( i );
      field.setName( item.getText( 1 ) );
      field.setPath( item.getText( 2 ) );
      boolean nullable = "Y".equalsIgnoreCase( item.getText( 3 ) );
      if( Const.isEmpty( item.getText(3) ) )
      {
        nullable = true;
      }
      field.setNullable( nullable );

      //CHECKSTYLE:Indentation:OFF
      tfoi.getOutputFields()[i] = field;
    }
  }

  private void ok() {
    avroSchema = null;
    if ( Const.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value

    getInfo( input );

    dispose();
  }

  private void get() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null ) {
        TableItemInsertListener listener = new TableItemInsertListener() {
          public boolean tableItemInserted( TableItem tableItem, ValueMetaInterface v ) {

            return true;
          }
        };
        getFieldsFromPrevious( r, wFields, 1, new int[] { 1 }, new int[] { 2 }, listener );
      }
    } catch ( KettleException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
        .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ke );
    }

  }

  public void getFieldsFromPrevious( RowMetaInterface row, TableView tableView, int keyColumn,
                                     int[] nameColumn, int[] pathColumn, TableItemInsertListener listener ) {
    if ( row == null || row.size() == 0 ) {
      return; // nothing to do
    }

    Table table = tableView.table;

    // get a list of all the non-empty keys (names)
    //
    List<String> keys = new ArrayList<String>();
    for ( int i = 0; i < table.getItemCount(); i++ ) {
      TableItem tableItem = table.getItem( i );
      String key = tableItem.getText( keyColumn );
      if ( !Const.isEmpty( key ) && keys.indexOf( key ) < 0 ) {
        keys.add( key );
      }
    }

    int choice = 0;

    if ( keys.size() > 0 ) {
      // Ask what we should do with the existing data in the step.
      //
      MessageDialog md =
        new MessageDialog( tableView.getShell(),
          BaseMessages.getString( PKG, "BaseStepDialog.GetFieldsChoice.Title" ), // "Warning!"
          null,
          BaseMessages.getString( PKG, "BaseStepDialog.GetFieldsChoice.Message", "" + keys.size(), "" + row.size() ),
          MessageDialog.WARNING, new String[] {
          BaseMessages.getString( PKG, "BaseStepDialog.AddNew" ),
          BaseMessages.getString( PKG, "BaseStepDialog.Add" ),
          BaseMessages.getString( PKG, "BaseStepDialog.ClearAndAdd" ),
          BaseMessages.getString( PKG, "BaseStepDialog.Cancel" ), }, 0 );
      MessageDialog.setDefaultImage( GUIResource.getInstance().getImageSpoon() );
      int idx = md.open();
      choice = idx & 0xFF;
    }

    if ( choice == 3 || choice == 255 ) {
      return; // Cancel clicked
    }

    if ( choice == 2 ) {
      tableView.clearAll( false );
    }

    for ( int i = 0; i < row.size(); i++ ) {
      ValueMetaInterface v = row.getValueMeta( i );

      boolean add = true;

      if ( choice == 0 ) { // hang on, see if it's not yet in the table view

        if ( keys.indexOf( v.getName() ) >= 0 ) {
          add = false;
        }
      }

      if ( add ) {
        TableItem tableItem = new TableItem( table, SWT.NONE );

        for ( int aNameColumn : nameColumn ) {
          tableItem.setText( aNameColumn, Const.NVL( v.getName(), "" ) );
        }

        for ( int aTokenNameColumn : pathColumn ) {
          tableItem.setText( aTokenNameColumn, Const.NVL( v.getName(), "" ) );
        }

        if ( listener != null ) {
          if ( !listener.tableItemInserted( tableItem, v ) ) {
            tableItem.dispose(); // remove it again
          }
        }
      }
    }
    tableView.removeEmptyRows();
    tableView.setRowNums();
    tableView.optWidth( true );
  }



}
