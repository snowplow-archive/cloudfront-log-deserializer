/* 
 * Copyright (c) 2012 Orderly Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.serde

// Java
import java.nio.charset.CharacterCodingException
import java.util.{List, Properties}
import java.util.regex.{Matcher, Pattern}

// Commons Logging
import org.apache.commons.logging.{Log, LogFactory}

// Hadoop
import org.apache.hadoop.conf.Configuration

// Hive
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.SerDeException
import org.apache.hadoop.hive.serde2.SerDeStats
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorFactory => OIF}
import org.apache.hadoop.hive.serde2.objectinspector.ReflectionStructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructField
import org.apache.hadoop.io.{BytesWritable, Test, Writable}

/**
 * S3LogDeserializer reads AWS S3 and CloudFront access log data into Hive.
 * 
 * For documentation please see the introductory README.md in the project root.
 */
class S3LogDeserializer implements Deserializer @throws(classOf[SerDeException]) {

  // -------------------------------------------------------------------------------------------------------------------
  // Default constructor
  // -------------------------------------------------------------------------------------------------------------------

  // Setup logging
  private val log: Log = LogFactory.getLog(classOf[S3LogDeserializer].getName())

  // Our mutable object inspector
  private var inspector: ObjectInspector = _

  // For performance reasons we reuse the same object to deserialize all of our rows
  private val struct: S3LogObject = new S3LogObject()

  // -------------------------------------------------------------------------------------------------------------------
  // Initializer
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Initialize the S3LogDeserializer.
   *
   * @param conf System properties
   * @param tbl Table properties
   * @throws SerDeException For any exception during initialization
   */
  @throws classOf[SerDeException]
  override def initialize(conf: Configuration, tbl: Properties) {

    inspector = OIF.getReflectionObjectInspector(classOf[S3LogTable], OIF.ObjectInspectorOptions.JAVA)
    log.debug("%s initialized".format(getClass().getName()))
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Deserializer
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Deserialize an object out of a Writable blob. In most cases, the return
   * value of this function will be constant since the function will reuse the
   * returned object. If the client wants to keep a copy of the object, the
   * client needs to clone the returned value by calling
   * ObjectInspectorUtils.getStandardObject().
   * 
   * @param blob The Writable object containing a serialized object
   * @return A Java object representing the contents in the blob.
   * @throws SerDeException For any exception during initialization
   */
  @throws classOf[SerDeException]
  def deserialize(blob: Writable): Object = {
  
    // Extract the String value from the blob
    val row: String = blob match {
      case b:BytesWritable =>
        try {
          Text.decode(b.getBytes(), 0, b.getLength())
        } catch cce:CharacterCodingException => throw new SerDeException(cce)
      case t:Text => t.toString()
      case _ => throw new SerDeException("%s expects blob to be Text or BytesWritable".format(this.getClass.getName), e)
    }

    // Construct and return the S3LogObject from the row data
    struct.parse(row)
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Getters
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Retrieve statistics for this SerDe. Returns null
   * because we don't support statistics (yet).
   *
   * @return The SerDe's statistics (null in this case)
   */
  def getSerDeStats(): SerDeStats = null

  /**
   * Get the object inspector that can be used to navigate through the internal
   * structure of the Object returned from deserialize(...).
   *
   * @return The ObjectInspector for this Deserializer 
   * @throws SerDeException For any exception during initialization
   */
  @throws SerDeException
  def getObjectInspector(): ObjectInspector = inspector
}