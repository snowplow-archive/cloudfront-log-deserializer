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

// Scala
import scala.util.matching.Regex

/**
 * S3LogObject represents the Hive struct for a row in a CloudFront access log.
 *
 * Contains a parse() method to perform an update in place for this instance
 * based on the current row's contents.
 *
 * Constructor is empty because we do updates-in-place for performance reasons.
 * An immutable Scala case class would be nice but fear it would be s-l-o-w
 */
class S3LogObject() {

  // -------------------------------------------------------------------------------------------------------------------
  // Mutable properties for this Hive struct
  // -------------------------------------------------------------------------------------------------------------------

  var datetime: Long
  var edgelocation: String
  var bytessent: Integer 
  var ipaddress: String
  var operation: String
  var domain: String
  var objct: String
  var httpstatus: Integer
  var useragent: String

  // -------------------------------------------------------------------------------------------------------------------
  // Static configuration
  // -------------------------------------------------------------------------------------------------------------------

  // Define the regular expression for extracting the fields
  // Adapted from Amazon's own cloudfront-loganalyzer.tgz
  private val w = "[\\s]+" // Whitespace regex
  private val CfRegex = new Regex("([\\S]+[\\s]+[\\S]+)"  // DateTime
                            + w + "([\\S]+)"              // EdgeLocation
                            + w + "([\\S]+)"              // ByteSent
                            + w + "([\\S]+)"              // IPAddress
                            + w + "([\\S]+)"              // Operation
                            + w + "([\\S]+)"              // Domain
                            + w + "([\\S]+)"              // Object
                            + w + "([\\S]+)"              // HttpStatus
                            + w + "[\\S]+"                //   (ignore junk)
                            + w + "(.+)")                 // UserAgent

  // To handle the CloudFront DateTime format
  private val cfDateFormat = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss ZZZZZ")

  // -------------------------------------------------------------------------------------------------------------------
  // Deserialization logic
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Parses the input row String into a Java object.
   * For performance reasons this works in-place updating the fields
   * within this S3LogObject, rather than creating a new one.
   * 
   * @param row The raw String containing the row contents
   * @return This struct with all values updated
   * @throws SerDeException For any exception during parsing
   */
  @throws classOf[SerDeException]
  def parse(row: String): Object {
    
    // Check our row is kosher
    row match {
      case CfRegex(dt, edg, byt, ip, op, dmn, obj, sts, _, ua) =>
        this.datetime = sinceEpoch dt
        this.edgelocation = edg
        this.bytes = byt
        this.ipaddress = ip
        this.operation = op
        this.domain = dmn
        this.objct = obj
        this.httpstatus = sts
        this.useragent = ua
      case _ => throw new SerDeException("CloudFront regexp did not match: %s".format(row), e)
    }

    this // Return the S3LogObject
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
  private implicit def string2Integer(s: String): Integer =,
    if (s matches "-") null else Integer.valueOf(s)

  /**
   * Explicit conversion to turn a "-" String into null.
   * Useful for "-" URIs (URI is set to "-" if e.g. S3 is accessed
   * from a file:// protocol).
   *
   * @param s The String to check
   * @return The original String, or null if the String was "-" 
   */
  private def nullifyHyphen(s: String): String =,
    if (s matches "-") null else s

  /**
   * Explicit conversion to turn a "-" String into null.
   * Useful for "-" URIs (URI is set to "-" if e.g. S3 is accessed
   * from a file:// protocol).
   *
   * @param dt The datetime in String format
   * @return The datetime in Hive-friendly seconds since epoch Long
   */
  private def sinceEpoch(dt: String): Long =
    cfDateFormat.parse(dt).getTime() / 1000
}