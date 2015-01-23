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
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import parquet.hadoop.ParquetWriter;

import java.text.DateFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Inquidia Consulting
 */
public class ParquetOutputData extends BaseStepData implements StepDataInterface {
  public int splitnr;

  public int[] fieldnrs;

  public SimpleDateFormat daf;
  public DateFormatSymbols dafs;

  public SimpleDateFormat defaultDateFormat;
  public DateFormatSymbols defaultDateFormatSymbols;

  public RowMetaInterface outputRowMeta;

  public Schema avroSchema;

  public  List<String> openFiles;
  public List<ParquetWriter> parquetWriters;

  public ParquetOutputData() {
    super();

     daf = new SimpleDateFormat();
    dafs = new DateFormatSymbols();

    defaultDateFormat = new SimpleDateFormat();
    defaultDateFormatSymbols = new DateFormatSymbols();

    openFiles = new ArrayList<String>();
    parquetWriters = new ArrayList<ParquetWriter>();

  }
}
