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

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.plugins.JobEntryPluginType;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInjectionInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;
import parquet.hadoop.ParquetWriter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/*
 * Created on 4-apr-2003
 * @author Inquidia Consulting
 */
@Step( id="ParquetOutput", image="parquetout.png", name="Step.Name", description="Step.Description",
  categoryDescription = "Category.Description", i18nPackageName = "org.openbi.kettle.plugins.parquetoutput",
  documentationUrl = "https://github.com/cdeptula/ParquetPlugin/wiki/Parquet-Output",
  casesUrl = "https://github.com/cdeptula/ParquetPlugin/issues",
  isSeparateClassLoaderNeeded = true )
public class ParquetOutputMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String FILENAME = "filename";
  public static final String FILENAME_IN_FIELD = "filename_in_field";
  public static final String FILENAME_FIELD = "filename_field";
  public static final String CREATE_PARENT_FOLDER = "create_parent_folder";
  public static final String INCLUDE_STEP_NR = "include_step_nr";
  public static final String INCLUDE_PART_NR = "include_part_nr";
  public static final String INCLUDE_DATE = "include_date";
  public static final String INCLUDE_TIME = "include_time";
  public static final String SPECIFY_FORMAT = "specify_format";
  public static final String DATE_FORMAT = "date_format";
  public static final String ADD_TO_RESULT = "add_to_result";
  public static final String USE_CONFIGURED_BLOCKSIZE = "use_configured_blocksize";
  public static final String BLOCKSIZE = "blocksize";
  public static final String PAGESIZE = "pagesize";
  public static final String COMPRESSION = "compression";
  public static final String DICTIONARY_COMPRESSION = "dictionary_compression";
  public static final String NAME = "name";
  public static final String PATH = "path";
  public static final String NULLABLE = "nullable";
  public static final String CLEAN_OUTPUT = "clean_output";
  private static Class<?> PKG = ParquetOutputMeta.class; // for i18n purposes, needed by Translator2!!

  public static final String[] compressionTypes = {"uncompressed","snappy","gzip","lzo"};

  /** The base name of the output file */
  private String filename;

  /** Flag: Accept filename from field */
  private boolean acceptFilenameFromField;

  /** Field to accept filename from */
  private String filenameField;

  /** Flag: Create the parent folder */
  private boolean createParentFolder = true;

  /** Flag: Clean output if file exists */
  private boolean cleanOutput = true;

  /** Flag: add the stepnr in the filename */
  private boolean stepNrInFilename;

  /** Flag: add the partition number in the filename */
  private boolean partNrInFilename;

  /** Flag: add the date in the filename */
  private boolean dateInFilename;

  /** Flag: add the time in the filename */
  private boolean timeInFilename;

  /** Flag: specify date time format in the filename */
  private boolean specifyFormat;

  /** date time format in the filename */
  private String dateTimeFormat;

  /** Flag: Add filename to result files */
  private boolean addToResult;

  /** Block size for file */
  private String blockSize;

  /** Page size for file */
  private String pageSize;

  /** Compression codec */
  private String compressionCodec;

  /** Flag: Enable dictionary compression */
  private boolean enableDictionaryCompression;

  /* THE FIELD SPECIFICATIONS ... */

  /** The output fields */
  private ParquetOutputField[] outputFields;


  public ParquetOutputMeta() {
    super(); // allocate BaseStepMeta
    allocate(0);
    setDefault();
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename( String filename ) {
    this.filename = filename;
  }

  public boolean isAcceptFilenameFromField() {
    return acceptFilenameFromField;
  }

  public void setAcceptFilenameFromField( boolean acceptFilenameFromField ) {
    this.acceptFilenameFromField = acceptFilenameFromField;
  }

  public String getFilenameField() {
    return filenameField;
  }

  public void setFilenameField( String filenameField ) {
    this.filenameField = filenameField;
  }

  public boolean isCreateParentFolder() {
    return createParentFolder;
  }

  public void setCreateParentFolder( boolean createParentFolder ) {
    this.createParentFolder = createParentFolder;
  }

  public boolean isCleanOutput() {
    return cleanOutput;
  }

  public void setCleanOutput( boolean cleanOutput ) {
    this.cleanOutput = cleanOutput;
  }

  public boolean isStepNrInFilename() {
    return stepNrInFilename;
  }

  public void setStepNrInFilename( boolean stepNrInFilename ) {
    this.stepNrInFilename = stepNrInFilename;
  }

  public boolean isPartNrInFilename() {
    return partNrInFilename;
  }

  public void setPartNrInFilename( boolean partNrInFilename ) {
    this.partNrInFilename = partNrInFilename;
  }

  public boolean isDateInFilename() {
    return dateInFilename;
  }

  public void setDateInFilename( boolean dateInFilename ) {
    this.dateInFilename = dateInFilename;
  }

  public boolean isTimeInFilename() {
    return timeInFilename;
  }

  public void setTimeInFilename( boolean timeInFilename ) {
    this.timeInFilename = timeInFilename;
  }

  public boolean isSpecifyFormat() {
    return specifyFormat;
  }

  public void setSpecifyFormat( boolean specifyFormat ) {
    this.specifyFormat = specifyFormat;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public void setDateTimeFormat( String dateTimeFormat ) {
    this.dateTimeFormat = dateTimeFormat;
  }

  public boolean isAddToResult() {
    return addToResult;
  }

  public void setAddToResult( boolean addToResult ) {
    this.addToResult = addToResult;
  }

  public String getBlockSize() {
    return blockSize;
  }

  public void setBlockSize( String blockSize ) {
    this.blockSize = blockSize;
  }

  public String getPageSize() {
    return pageSize;
  }

  public void setPageSize( String pageSize ) {
    this.pageSize = pageSize;
  }

  public String getCompressionCodec() {
    return compressionCodec;
  }

  public void setCompressionCodec( String compressionCodec ) {
    this.compressionCodec = compressionCodec;
  }

  public boolean isEnableDictionaryCompression() {
    return enableDictionaryCompression;
  }

  public void setEnableDictionaryCompression( boolean enableDictionaryCompression ) {
    this.enableDictionaryCompression = enableDictionaryCompression;
  }

  public ParquetOutputField[] getOutputFields() {
    return outputFields;
  }

  public void setOutputFields( ParquetOutputField[] outputFields ) {
    this.outputFields = outputFields;
  }

  public boolean isBigDataPluginInstalled() throws KettleException
  {
    if( PluginRegistry.getInstance().findPluginWithId( JobEntryPluginType.class, "HadoopTransJobExecutorPlugin" ) != null )
    {
      try {

        PluginRegistry.getInstance().loadClass(
          PluginRegistry.getInstance().findPluginWithId( JobEntryPluginType.class, "HadoopTransJobExecutorPlugin" ) );
      } catch (KettlePluginException ex)
      {
        throw new KettleException( ex.getMessage(), ex );
      }
      return true;
    }
    return false;
  }

  public String getDefaultPageSize()
  {
    int defaultPageSize = ParquetWriter.DEFAULT_PAGE_SIZE;
    int mbPageSize = defaultPageSize / 1024;
    return Integer.toString( mbPageSize );
  }



  public String getDefaultBlockSize()  {
    int defaultBlockSize = ParquetWriter.DEFAULT_BLOCK_SIZE;
    boolean bigDataPluginInstalled = false;
    try {
      bigDataPluginInstalled = isBigDataPluginInstalled();
    } catch ( KettleException ex )
    {
      logError( ex.getMessage() );
    }
    if( bigDataPluginInstalled )
    {
 /*     try {
        int hdfsBlockSize = BigDataHelper.getHdfsConfiguredBlockSize();
        if( hdfsBlockSize > 0 )
        {
          defaultBlockSize = hdfsBlockSize;
        }
      } catch ( KettleException ex )
      {
        logDebug( "Error fetching Hadoop configuration " + ex.getMessage() );
      }
*/
    }

    int mbBlockSize = defaultBlockSize / 1024 / 1024;
    return Integer.toString( mbBlockSize );
  }

  public boolean getDefaultIsDictionaryEnabled()
  {
    return ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED;
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore )
    throws KettleXMLException {
    readData( stepnode );
  }

  public void allocate( int nrfields ) {
    outputFields = new ParquetOutputField[nrfields];
  }

  public Object clone() {
    ParquetOutputMeta retval = (ParquetOutputMeta) super.clone();
    int nrfields = outputFields.length;

    retval.allocate( nrfields );

    for ( int i = 0; i < nrfields; i++ ) {
      retval.outputFields[i] = (ParquetOutputField) outputFields[i].clone();
    }

    return retval;
  }

  public void readData( Node stepnode ) throws KettleXMLException {
    try {
      filename = XMLHandler.getTagValue( stepnode, FILENAME );
      acceptFilenameFromField = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, FILENAME_IN_FIELD ) );
      filenameField = XMLHandler.getTagValue( stepnode, FILENAME_FIELD );
      createParentFolder = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, CREATE_PARENT_FOLDER ) );
      cleanOutput = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, CLEAN_OUTPUT ) );
      stepNrInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, INCLUDE_STEP_NR ) );
      partNrInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, INCLUDE_PART_NR ) );
      dateInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, INCLUDE_DATE ) );
      timeInFilename = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, INCLUDE_TIME ) );
      specifyFormat = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, SPECIFY_FORMAT ) );
      dateTimeFormat = XMLHandler.getTagValue( stepnode, DATE_FORMAT );
      addToResult = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, ADD_TO_RESULT ) );

      blockSize = XMLHandler.getTagValue( stepnode, BLOCKSIZE );
      pageSize = XMLHandler.getTagValue( stepnode, PAGESIZE );
      compressionCodec = XMLHandler.getTagValue( stepnode, COMPRESSION );
      enableDictionaryCompression = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, DICTIONARY_COMPRESSION ) );

      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        outputFields[i] = new ParquetOutputField();
        outputFields[i].setName( XMLHandler.getTagValue( fnode, NAME ) );
        outputFields[i].setPath( XMLHandler.getTagValue( fnode, PATH ) );
        outputFields[i].setNullable( "Y".equalsIgnoreCase( XMLHandler.getTagValue( fnode, NULLABLE ) ) );
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load step info from XML", e );
    }
  }

  public void setDefault()  {
    createParentFolder = true; // Default createparentfolder to true
    cleanOutput = true;
    stepNrInFilename = false;
    partNrInFilename = false;
    dateInFilename = false;
    timeInFilename = false;
    specifyFormat = false;
    addToResult = false;
    blockSize = getDefaultBlockSize();
    pageSize = getDefaultPageSize();
    compressionCodec = "none";
    enableDictionaryCompression = getDefaultIsDictionaryEnabled();
  }

  public String buildFilename( VariableSpace space, int stepnr, String partnr ) {
    return buildFilename( filename, space, stepnr, partnr, this );
  }

  public String buildFilename( String filename, VariableSpace space, int stepnr, String partnr,
      ParquetOutputMeta meta ) {
    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String realFileName = space.environmentSubstitute( filename );
    String extension = "";
    String retval = "";
    if( realFileName.contains(".") )
    {
    	retval = realFileName.substring( 0 , realFileName.lastIndexOf(".") );
    	extension = realFileName.substring( realFileName.lastIndexOf(".") +1 ); 
    } else {
    	retval = realFileName;
    }
    
    
    Date now = new Date();

    if ( meta.isSpecifyFormat() && !Const.isEmpty( space.environmentSubstitute( meta.getDateTimeFormat() ) ) ) {
      daf.applyPattern( space.environmentSubstitute( meta.getDateTimeFormat() ) );
      String dt = daf.format( now );
      retval += dt;
    } else {
      if ( meta.isDateInFilename() ) {
        daf.applyPattern( "yyyMMdd" );
        String d = daf.format( now );
        retval += "_" + d;
      }
      if ( meta.isTimeInFilename() ) {
        daf.applyPattern( "HHmmss" );
        String t = daf.format( now );
        retval += "_" + t;
      }
    }
    if ( meta.isStepNrInFilename() ) {
      retval += "_" + stepnr;
    }
    if ( meta.isPartNrInFilename() ) {
      retval += "_" + partnr;
    }

    if ( extension != null && extension.length() != 0 ) {
      retval += "." + extension;
    }
    return retval;
  }

  public String getXML() {
    StringBuffer retval = new StringBuffer( 800 );

    retval.append( "    " + XMLHandler.addTagValue( FILENAME, filename ) );
    retval.append( "    " + XMLHandler.addTagValue( CREATE_PARENT_FOLDER, createParentFolder ) );
    retval.append( "    " + XMLHandler.addTagValue( CLEAN_OUTPUT, cleanOutput ) );
    retval.append( "    " + XMLHandler.addTagValue( FILENAME_IN_FIELD, acceptFilenameFromField ) );
    retval.append( "    " + XMLHandler.addTagValue( FILENAME_FIELD, filenameField ) );
    retval.append( "    " + XMLHandler.addTagValue( INCLUDE_STEP_NR, stepNrInFilename ) );
    retval.append( "    " + XMLHandler.addTagValue( INCLUDE_PART_NR, partNrInFilename ) );
    retval.append( "    " + XMLHandler.addTagValue( INCLUDE_DATE, dateInFilename ) );
    retval.append( "    " + XMLHandler.addTagValue( INCLUDE_TIME, timeInFilename ) );
    retval.append( "    " + XMLHandler.addTagValue( SPECIFY_FORMAT, specifyFormat ) );
    retval.append( "    " + XMLHandler.addTagValue( DATE_FORMAT, dateTimeFormat ) );
    retval.append( "    " + XMLHandler.addTagValue( ADD_TO_RESULT, addToResult ) );

    retval.append( "    " + XMLHandler.addTagValue( BLOCKSIZE, blockSize ) );
    retval.append( "    " + XMLHandler.addTagValue( PAGESIZE, pageSize ) );
    retval.append( "    " + XMLHandler.addTagValue( COMPRESSION, compressionCodec ) ) ;
    retval.append( "    " + XMLHandler.addTagValue( DICTIONARY_COMPRESSION, enableDictionaryCompression ) );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < outputFields.length; i++ ) {
      ParquetOutputField field = outputFields[i];

      if ( field.getName() != null && field.getName().length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( NAME, field.getName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( PATH, field.getPath() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( NULLABLE, field.getNullable() ) );
        retval.append( "      </field>" ).append( Const.CR );
      }
    }
    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      filename = rep.getStepAttributeString( id_step, FILENAME );
      acceptFilenameFromField = rep.getStepAttributeBoolean( id_step, FILENAME_IN_FIELD );
      filenameField = rep.getStepAttributeString( id_step, FILENAME_FIELD );
      createParentFolder = rep.getStepAttributeBoolean( id_step, CREATE_PARENT_FOLDER );
      cleanOutput = rep.getStepAttributeBoolean( id_step, CLEAN_OUTPUT );
      stepNrInFilename = rep.getStepAttributeBoolean( id_step, INCLUDE_STEP_NR );
      partNrInFilename = rep.getStepAttributeBoolean( id_step, INCLUDE_PART_NR );
      dateInFilename = rep.getStepAttributeBoolean( id_step, INCLUDE_DATE );
      timeInFilename = rep.getStepAttributeBoolean( id_step, INCLUDE_TIME );
      specifyFormat = rep.getStepAttributeBoolean( id_step, SPECIFY_FORMAT );
      dateTimeFormat = rep.getStepAttributeString( id_step, DATE_FORMAT );
      addToResult = rep.getStepAttributeBoolean( id_step, ADD_TO_RESULT );

      blockSize = rep.getStepAttributeString( id_step, BLOCKSIZE );
      pageSize = rep.getStepAttributeString( id_step, PAGESIZE );
      compressionCodec = rep.getStepAttributeString( id_step, COMPRESSION );
      enableDictionaryCompression = rep.getStepAttributeBoolean( id_step, DICTIONARY_COMPRESSION );


      int nrfields = rep.countNrStepAttributes( id_step, "field" );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        outputFields[i] = new ParquetOutputField();

        outputFields[i].setName( rep.getStepAttributeString( id_step, i, NAME ) );
        outputFields[i].setPath( rep.getStepAttributeString( id_step, i, PATH ) );
        outputFields[i].setNullable( rep.getStepAttributeBoolean( id_step, i, NULLABLE ) );
      }

    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, FILENAME, filename );
      rep.saveStepAttribute( id_transformation, id_step, FILENAME_IN_FIELD, acceptFilenameFromField );
      rep.saveStepAttribute( id_transformation, id_step, FILENAME_FIELD, filenameField );
      rep.saveStepAttribute( id_transformation, id_step, CREATE_PARENT_FOLDER, createParentFolder );
      rep.saveStepAttribute( id_transformation, id_step, CLEAN_OUTPUT, cleanOutput );
      rep.saveStepAttribute( id_transformation, id_step, INCLUDE_STEP_NR, stepNrInFilename );
      rep.saveStepAttribute( id_transformation, id_step, INCLUDE_PART_NR, partNrInFilename );
      rep.saveStepAttribute( id_transformation, id_step, INCLUDE_DATE, dateInFilename );
      rep.saveStepAttribute( id_transformation, id_step, INCLUDE_TIME, timeInFilename );
      rep.saveStepAttribute( id_transformation, id_step, SPECIFY_FORMAT, specifyFormat );
      rep.saveStepAttribute( id_transformation, id_step, DATE_FORMAT, dateTimeFormat );
      rep.saveStepAttribute( id_transformation, id_step, ADD_TO_RESULT, addToResult );

      rep.saveStepAttribute( id_transformation, id_step, BLOCKSIZE, blockSize );
      rep.saveStepAttribute( id_transformation, id_step, PAGESIZE, pageSize );
      rep.saveStepAttribute( id_transformation, id_step, COMPRESSION, compressionCodec );
      rep.saveStepAttribute( id_transformation, id_step, DICTIONARY_COMPRESSION, enableDictionaryCompression );

      for ( int i = 0; i < outputFields.length; i++ ) {
        ParquetOutputField field = outputFields[i];

        rep.saveStepAttribute( id_transformation, id_step, i, NAME, field.getName() );
        rep.saveStepAttribute( id_transformation, id_step, i, PATH, field.getPath() );
        rep.saveStepAttribute( id_transformation, id_step, i, NULLABLE, field.getNullable() );
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    Repository repository, IMetaStore metaStore ) {
    CheckResult cr;

    // Check output fields
    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "ParquetOutputMeta.CheckResult.FieldsReceived", "" + prev.size() ), stepMeta );
      remarks.add( cr );

      String error_message = "";
      boolean error_found = false;

      // Starting from selected fields in ...
      for ( int i = 0; i < outputFields.length; i++ ) {
        int idx = prev.indexOfValue( outputFields[i].getName() );
        if ( idx < 0 ) {
          error_message += "\t\t" + outputFields[i].getName() + Const.CR;
          error_found = true;
        }
      }
      if ( error_found ) {
        error_message =
          BaseMessages.getString( PKG, "ParquetOutputMeta.CheckResult.FieldsNotFound", error_message );
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "ParquetOutputMeta.CheckResult.AllFieldsFound" ), stepMeta );
        remarks.add( cr );
      }
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "ParquetOutputMeta.CheckResult.ExpectedInputOk" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "ParquetOutputMeta.CheckResult.ExpectedInputError" ), stepMeta );
      remarks.add( cr );
    }

    cr =
      new CheckResult( CheckResultInterface.TYPE_RESULT_COMMENT, BaseMessages.getString(
        PKG, "ParquetOutputMeta.CheckResult.FilesNotChecked" ), stepMeta );
    remarks.add( cr );
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
    TransMeta transMeta, Trans trans ) {
    return new ParquetOutput( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new ParquetOutputData();
  }

  @Override
  public StepMetaInjectionInterface getStepMetaInjectionInterface() {
    return new ParquetOutputMetaInjection( this );
  }


}
