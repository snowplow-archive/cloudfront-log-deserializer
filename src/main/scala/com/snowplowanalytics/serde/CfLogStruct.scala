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
import java.text.SimpleDateFormat
import java.lang.{Integer => JInteger}

// Scala
import scala.util.matching.Regex

// Hive
import org.apache.hadoop.hive.serde2.SerDeException

/**
 * CfLogStruct represents the Hive struct for a row in a CloudFront access log.
 *
 * Contains a parse() method to perform an update-in-place for this instance
 * based on the current row's contents.
 *
 * Constructor is empty because we do updates-in-place for performance reasons.
 * An immutable Scala case class would be nice but fear it would be s-l-o-w
 */
class CfLogStruct() {

  // -------------------------------------------------------------------------------------------------------------------
  // Mutable properties for this Hive struct
  // -------------------------------------------------------------------------------------------------------------------

  var dt: String = _
  var edgelocation: String = _
  var bytessent: JInteger = _ 
  var ipaddress: String = _
  var operation: String = _
  var domain: String = _
  var objct: String = _
  var httpstatus: JInteger = _
  var referrer: String = _
  var useragent: String = _
  var querystring: String = _
  // var querymap: Map[String, String] TODO add this

  // -------------------------------------------------------------------------------------------------------------------
  // Static configuration
  // -------------------------------------------------------------------------------------------------------------------

  // Define the regular expression for extracting the fields
  // Adapted from Amazon's own cloudfront-loganalyzer.tgz
  private val w = "[\\s]+" // Whitespace regex
  private val CfRegex = new Regex("([\\S]+"   // Date          / date
                            + w + "([\\S]+)"  // Time          / time
                            + w + "([\\S]+)"  // EdgeLocation  / x-edge-location
                            + w + "([\\S]+)"  // BytesSent     / sc-bytes
                            + w + "([\\S]+)"  // IPAddress     / c-ip
                            + w + "([\\S]+)"  // Operation     / cs-method
                            + w + "([\\S]+)"  // Domain        / cs(Host)
                            + w + "([\\S]+)"  // Object        / cs-uri-stem
                            + w + "([\\S]+)"  // HttpStatus    / sc-status
                            + w + "([\\S]+)"  // Referrer      / cs(Referer)
                            + w + "([\\S]+)"  // UserAgent     / cs(User Agent)
                            + w + "(.+)")     // Querystring   / cs-uri-query

  // To handle the CloudFront DateTime format
  private val cfDateFormat = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss ZZZZZ")
  private val hiveDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

  // -------------------------------------------------------------------------------------------------------------------
  // Deserialization logic
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Parses the input row String into a Java object.
   * For performance reasons this works in-place updating the fields
   * within this CfLogStruct, rather than creating a new one.
   * 
   * @param row The raw String containing the row contents
   * @return This struct with all values updated
   * @throws SerDeException For any exception during parsing
   */
  @throws(classOf[SerDeException])
  def parse(row: String): Object = {
    
    // Check our row is kosher
    row match {
      case CfRegex(date, time, edgelocation, bytessent, ipaddress, operation, domain, objct, httpstatus, referrer, useragent, querystring) =>
        this.dt = toHiveDate(date + " " + time)
        this.edgelocation = edgelocation
        this.bytessent = bytessent
        this.ipaddress = ipaddress
        this.operation = operation
        this.domain = domain
        this.objct = objct
        this.httpstatus = httpstatus
        this.referrer = referrer
        this.useragent = useragent
        this.querystring = querystring
        // TODO: build the querymap too
      case _ => throw new SerDeException("CloudFront regexp did not match: %s".format(row))
    }

    this // Return the CfLogStruct
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Datatype conversions
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Implicit conversion from String to Integer. To deal with
   * the fact that AWS uses a single "-"" for null.
   *
   * @param s The String to check
   * @return The Integer, or null if the String was "-" 
   */
  private implicit def string2JInteger(s: String): JInteger =
    if (s matches "-") null else Integer.valueOf(s)

  /**
   * Explicit conversion to turn a "-" String into null.
   * Useful for "-" URIs (URI is set to "-" if e.g. S3 is accessed
   * from a file:// protocol).
   *
   * @param s The String to check
   * @return The original String, or null if the String was "-" 
   */
  private def nullifyHyphen(s: String): String =
    if (s matches "-") null else s

  /**
   * Convert a date from CloudFront format to Hive format
   *
   * @param dt The datetime in CloudFront String format
   * @return The datetime in Hive-friendly String format
   */
  private def toHiveDate(dt: String): String =
    hiveDateFormat.format(cfDateFormat.parse(dt).getTime())
}