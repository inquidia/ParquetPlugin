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
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.util.Arrays;

/**
 * Describes a single field in a text file
 *
 * @author Inquidia Consulting
 *
 */
public class ParquetOutputField implements Cloneable, Comparable<ParquetOutputField> {
	

  private String name;
  
  private String path;

  private boolean nullable;

  public final static int AVRO_TYPE_NONE = 0;
  public final static int AVRO_TYPE_BOOLEAN = 1;
  public final static int AVRO_TYPE_DOUBLE = 2;
  public final static int AVRO_TYPE_FLOAT = 3;
  public final static int AVRO_TYPE_INT = 4;
  public final static int AVRO_TYPE_LONG = 5;
  public final static int AVRO_TYPE_STRING = 6;

  private static String[] avroDescriptions = {"","Boolean","Double","Float","Int","Long","String"};

  public ParquetOutputField( String name, String path, boolean nullable ) {
    this.name = name;
    this.path = path;
    this.nullable = nullable;
  }

  public ParquetOutputField() {
  }
  
  public int compare( Object obj ) {
    ParquetOutputField field = (ParquetOutputField) obj;

    return name.compareTo( field.getName() );
  }

  public boolean equal( Object obj ) {
    ParquetOutputField field = (ParquetOutputField) obj;

    return name.equals( field.getName() );
  }

  public Object clone() {
    try {
      return super.clone();
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  /**
   * Get the stream field name.
   * @return name
   */
  public String getName() {
    return name;
  }

  /**
   * Set the stream field name.
   * @param fieldname
   */
  public void setName( String fieldname ) {
    this.name = fieldname;
  }

  public String getPath() {
    return path;
  }

  public void setPath( String path ) {
    this.path = path;
  }

  /**
   * Return if the field is nullable in the Avro schema.
   * @return nullable
   */
  public boolean getNullable()
  {
    return nullable;
  }

  /**
   * Set if the field is nullable or not.
   * @param nullable
   */
  public void setNullable( boolean nullable )
  {
    this.nullable = nullable;
  }

  /**
   * Returns the array of all Avro datatype descriptions
   * sorted alphabetically.
   * @return
   */
  public static String[] getAvroTypeArraySorted()
  {
    String[] sorted = avroDescriptions;
    Arrays.sort( sorted, 1, avroDescriptions.length-1 );
    return sorted;
  }

  public static Schema.Type getDefaultAvroType( int pentahoType )
  {
    switch( pentahoType ) {
      case ValueMetaInterface.TYPE_NUMBER :
      case ValueMetaInterface.TYPE_BIGNUMBER :
        return Schema.Type.DOUBLE;
      case ValueMetaInterface.TYPE_INTEGER :
        return Schema.Type.LONG;
      case ValueMetaInterface.TYPE_BOOLEAN :
        return Schema.Type.BOOLEAN;
      default:
        return Schema.Type.STRING;
    }
  }

  public boolean validate() throws KettleException
  {
    if( name == null )
    {
      throw new KettleException( "Validation error: Stream field name is required." );
    }

    return true;
  }

  public String toString() {
    return name;
  }

  public int compareTo( ParquetOutputField compareField ) {

    return this.getPath()!=null ? (compareField.getPath()!=null ? this.getName().compareTo( compareField.getName() ) : -1 ) : -1;
  }

}
