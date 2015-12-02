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

import org.apache.avro.generic.GenericData;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.StepInjectionMetaEntry;
import org.pentaho.di.trans.step.StepMetaInjectionEntryInterface;
import org.pentaho.di.trans.step.StepMetaInjectionInterface;
import static org.pentaho.di.trans.step.StepInjectionUtil.getEntry;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This takes care of the external metadata injection into the AvroOutputMeta class
 *
 * @author Inquidia Consulting
 */
public class ParquetOutputMetaInjection implements StepMetaInjectionInterface {

  public enum Entry implements StepMetaInjectionEntryInterface {

    FILENAME( ValueMetaInterface.TYPE_STRING, "The output filename." ),
    FILENAME_IN_FIELD( ValueMetaInterface.TYPE_STRING, "The filename is in a field? (Y/N)" ),
    FILENAME_FIELD( ValueMetaInterface.TYPE_STRING, "The filename field" ),
    CLEAN_OUTPUT( ValueMetaInterface.TYPE_STRING, "Clean the output file if it exists? (Y/N)" ),
    CREATE_PARENT_FOLDER( ValueMetaInterface.TYPE_STRING, "Create parent folder? (Y/N)" ),
    INCLUDE_STEPNR( ValueMetaInterface.TYPE_STRING, "Include the step nr in filename? (Y/N)" ),
    INCLUDE_PARTNR( ValueMetaInterface.TYPE_STRING, "Include partition nr in filename? (Y/N)" ),
    INCLUDE_DATE( ValueMetaInterface.TYPE_STRING, "Include date in filename? (Y/N)" ),
    INCLUDE_TIME( ValueMetaInterface.TYPE_STRING, "Include time in filename? (Y/N)" ),
    SPECIFY_FORMAT( ValueMetaInterface.TYPE_STRING, "Specify date format to include in filename? (Y/N)" ),
    DATE_FORMAT( ValueMetaInterface.TYPE_STRING, "Date format for filename" ),
    ADD_TO_RESULT( ValueMetaInterface.TYPE_STRING, "Add output filename to result? (Y/N)" ),

    USE_CONFIGURED_BLOCKSIZE( ValueMetaInterface.TYPE_STRING, "Use block size from HDFS config? (Y/N)" ),
    BLOCKSIZE( ValueMetaInterface.TYPE_STRING, "The block size in MB." ),
    PAGESIZE( ValueMetaInterface.TYPE_STRING, "The page size in KB." ),
    COMPRESSION( ValueMetaInterface.TYPE_STRING, "The compression codec. (none, snappy, gzip, lzo)" ),
    DICTIONARY_COMPRESSION( ValueMetaInterface.TYPE_STRING, "Use dictionary compression? (Y/N)" ),

    OUTPUT_FIELDS( ValueMetaInterface.TYPE_NONE, "The output fileds" ),
    OUTPUT_FIELD( ValueMetaInterface.TYPE_NONE, "One output field" ),
    STREAM_NAME( ValueMetaInterface.TYPE_STRING, "Field to output" ),
    PATH( ValueMetaInterface.TYPE_STRING, "Path to output to." ),
    NULLABLE( ValueMetaInterface.TYPE_STRING, "Is the field nullable? (Y/N)" );

    private int valueType;
    private String description;

    private Entry( int valueType, String description ) {
      this.valueType = valueType;
      this.description = description;
    }

    /**
     * @return the valueType
     */
    public int getValueType() {
      return valueType;
    }

    /**
     * @return the description
     */
    public String getDescription() {
      return description;
    }

    public static Entry findEntry( String key ) {
      return Entry.valueOf( key );
    }
  }

  private ParquetOutputMeta meta;

  public ParquetOutputMetaInjection( ParquetOutputMeta meta ) {
    this.meta = meta;
  }

  public List<StepInjectionMetaEntry> getStepInjectionMetadataEntries() throws KettleException {
    List<StepInjectionMetaEntry> all = new ArrayList<StepInjectionMetaEntry>();

    Entry[] topEntries =
      new Entry[] {
        Entry.FILENAME, Entry.FILENAME_IN_FIELD, Entry.FILENAME_FIELD, Entry.CREATE_PARENT_FOLDER,
        Entry.CLEAN_OUTPUT, Entry.INCLUDE_STEPNR, Entry.INCLUDE_PARTNR, Entry.INCLUDE_DATE, Entry.INCLUDE_TIME,
        Entry.SPECIFY_FORMAT, Entry.DATE_FORMAT, Entry.ADD_TO_RESULT, Entry.USE_CONFIGURED_BLOCKSIZE,
        Entry.BLOCKSIZE, Entry.PAGESIZE, Entry.COMPRESSION, Entry.DICTIONARY_COMPRESSION, };
    for ( Entry topEntry : topEntries ) {
      all.add( new StepInjectionMetaEntry( topEntry.name(), topEntry.getValueType(), topEntry.getDescription() ) );
    }

    // The fields
    //
    StepInjectionMetaEntry fieldsEntry =
      new StepInjectionMetaEntry(
        Entry.OUTPUT_FIELDS.name(), ValueMetaInterface.TYPE_NONE, Entry.OUTPUT_FIELDS.description );
    all.add( fieldsEntry );
    StepInjectionMetaEntry fieldEntry =
      new StepInjectionMetaEntry(
        Entry.OUTPUT_FIELD.name(), ValueMetaInterface.TYPE_NONE, Entry.OUTPUT_FIELD.description );
    fieldsEntry.getDetails().add( fieldEntry );

    Entry[] fieldsEntries = new Entry[] { Entry.STREAM_NAME, Entry.PATH, Entry.NULLABLE, };
    for ( Entry entry : fieldsEntries ) {
      StepInjectionMetaEntry metaEntry =
        new StepInjectionMetaEntry( entry.name(), entry.getValueType(), entry.getDescription() );
      fieldEntry.getDetails().add( metaEntry );
    }

    return all;
  }

  public void injectStepMetadataEntries( List<StepInjectionMetaEntry> all ) throws KettleException {

    List<String> outputFields = new ArrayList<String>();
    List<String> paths = new ArrayList<String>();
    List<Boolean> nullables = new ArrayList<Boolean>();

    // Parse the fields, inject into the meta class..
    //
    for ( StepInjectionMetaEntry lookFields : all ) {
      Entry fieldsEntry = Entry.findEntry( lookFields.getKey() );
      if ( fieldsEntry == null ) {
        continue;
      }

      String lookValue = (String) lookFields.getValue();
      switch ( fieldsEntry ) {
        case OUTPUT_FIELDS:
          for ( StepInjectionMetaEntry lookField : lookFields.getDetails() ) {
            Entry fieldEntry = Entry.findEntry( lookField.getKey() );
            if ( fieldEntry == Entry.OUTPUT_FIELD ) {

              String outputField = null;
              String path = null;
              boolean nullable = false;

              List<StepInjectionMetaEntry> entries = lookField.getDetails();
              for ( StepInjectionMetaEntry entry : entries ) {
                Entry metaEntry = Entry.findEntry( entry.getKey() );
                if ( metaEntry != null ) {
                  String value = (String) entry.getValue();
                  switch ( metaEntry ) {
                    case STREAM_NAME:
                      outputField = value;
                      break;
                    case PATH:
                      path = value;
                      break;
                    case NULLABLE:
                      nullable = "Y".equalsIgnoreCase( value );
                    default:
                      break;
                  }
                }
              }

              outputFields.add( outputField );
              paths.add( path );
              nullables.add( nullable );
            }
          }
          break;

        case FILENAME:
          meta.setFilename( lookValue );
          break;
        case FILENAME_IN_FIELD:
          meta.setAcceptFilenameFromField( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case FILENAME_FIELD:
          meta.setFilenameField( lookValue );
          break;
        case CLEAN_OUTPUT:
          meta.setCleanOutput( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case CREATE_PARENT_FOLDER:
          meta.setCreateParentFolder( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case INCLUDE_STEPNR:
          meta.setStepNrInFilename( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case INCLUDE_PARTNR:
          meta.setPartNrInFilename( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case INCLUDE_DATE:
          meta.setDateInFilename( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case INCLUDE_TIME:
          meta.setTimeInFilename( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case SPECIFY_FORMAT:
          meta.setSpecifyFormat( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case DATE_FORMAT:
          meta.setDateTimeFormat( lookValue );
          break;
        case ADD_TO_RESULT:
          meta.setAddToResult( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case BLOCKSIZE:
          meta.setBlockSize( lookValue );
          break;
        case PAGESIZE:
          meta.setPageSize( lookValue );
          break;
        case COMPRESSION:
          meta.setCompressionCodec( lookValue );
          break;
        case DICTIONARY_COMPRESSION:
          meta.setEnableDictionaryCompression( "Y".equalsIgnoreCase( lookValue ) );
          break;
        default:
          break;
      }
    }

    // Pass the grid to the step metadata
    //
    if ( outputFields.size() > 0 ) {
      ParquetOutputField[] aof = new ParquetOutputField[outputFields.size()];
      Iterator<String> iOutputFields = outputFields.iterator();
      Iterator<String> iPaths = paths.iterator();
      Iterator<Boolean> iNullables = nullables.iterator();

      int i = 0;
      while ( iOutputFields.hasNext() ) {
        ParquetOutputField field = new ParquetOutputField();
        field.setName( iOutputFields.next() );
        field.setPath( iPaths.next() );
        field.setNullable( iNullables.next() );
        aof[i] = field;
        i++;
      }
      meta.setOutputFields( aof );
    }
  }

  public List<StepInjectionMetaEntry> extractStepMetadataEntries() throws KettleException {
    List<StepInjectionMetaEntry> result = new ArrayList<StepInjectionMetaEntry>();
    result.add( getEntry( Entry.FILENAME, meta.getFilename() ) );
    result.add( getEntry( Entry.FILENAME_IN_FIELD, meta.isAcceptFilenameFromField() ) );
    result.add( getEntry( Entry.FILENAME_FIELD, meta.getFilenameField() ) );
    result.add( getEntry( Entry.CLEAN_OUTPUT, meta.isCleanOutput() ) );
    result.add( getEntry( Entry.CREATE_PARENT_FOLDER, meta.isCreateParentFolder() ) );
    result.add( getEntry( Entry.INCLUDE_STEPNR, meta.isStepNrInFilename() ) );
    result.add( getEntry( Entry.INCLUDE_PARTNR, meta.isPartNrInFilename() ) );
    result.add( getEntry( Entry.INCLUDE_DATE, meta.isDateInFilename() ) );
    result.add( getEntry( Entry.INCLUDE_TIME, meta.isTimeInFilename() ) );
    result.add( getEntry( Entry.SPECIFY_FORMAT, meta.isSpecifyFormat() ) );
    result.add( getEntry( Entry.DATE_FORMAT, meta.getDateTimeFormat() ) );
    result.add( getEntry( Entry.ADD_TO_RESULT, meta.isAddToResult() ) );
    result.add( getEntry( Entry.BLOCKSIZE, meta.getBlockSize() ) );
    result.add( getEntry( Entry.PAGESIZE, meta.getPageSize() ) );
    result.add( getEntry( Entry.COMPRESSION, meta.getCompressionCodec() ) );
    result.add( getEntry( Entry.DICTIONARY_COMPRESSION, meta.isEnableDictionaryCompression() ) );

    StepInjectionMetaEntry fieldsEntry = getEntry( Entry.OUTPUT_FIELDS );
    if( !Const.isEmpty( meta.getOutputFields() ) ) {
      for( ParquetOutputField outputField : meta.getOutputFields() ) {
        StepInjectionMetaEntry fieldEntry = getEntry( Entry.OUTPUT_FIELD );
        fieldEntry.getDetails().add( fieldEntry );
        List<StepInjectionMetaEntry> fieldDetails = fieldEntry.getDetails();

        fieldDetails.add( getEntry( Entry.STREAM_NAME, outputField.getName() ) );
        fieldDetails.add( getEntry( Entry.PATH, outputField.getPath() ) );
        fieldDetails.add( getEntry( Entry.NULLABLE, outputField.getNullable() ) );
      }
    }
    result.add( fieldsEntry );

    return result;
  }


  public ParquetOutputMeta getMeta() {
    return meta;
  }
}