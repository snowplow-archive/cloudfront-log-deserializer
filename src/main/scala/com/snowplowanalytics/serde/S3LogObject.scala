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


object S3LogObject {
  
  // -------------------------------------------------------------------------------------------------------------------
  // Static configuration
  // -------------------------------------------------------------------------------------------------------------------

  // Define the regular expression for extracting the fields
  // Adapted from Amazon's own cloudfront-loganalyzer.tgz
  val w = "[\\s]+" // Whitespace regex
  val cfRegex = "([\\S]+[\\s]+[\\S]+)"  // DateTime
          + w + "([\\S]+)"              // EdgeLocation
          + w + "([\\S]+)"              // Bytes
          + w + "([\\S]+)"              // IPAddress
          + w + "([\\S]+)"              // Operation
          + w + "([\\S]+)"              // Domain
          + w + "([\\S]+)"              // Object
          + w + "([\\S]+)"              // HttpResponse
          + w + "[\\S]+"                //   (ignore junk)
          + w + "(.+)"                  // UserAgent

  // To handle the CloudFront DateTime format
  val cfDateFormat = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss ZZZZZ")

  /**
   *
   */
  static Object deserialize(S3LogStruct c, String row) throws Exception {
    Matcher match = regexpat.matcher(row);
    int t = 1;
    try {
      match.matches();
      c.bucketowner = match.group(t++);
      c.bucketname = match.group(t++);
    } catch (Exception e) {
      throw new SerDeException("S3 Log Regex did not match:" + row, e);
    }
    c.rdatetime = match.group(t++);

    // Should we convert the datetime to the format Hive understands by default
    // - either yyyy-mm-dd HH:MM:SS or seconds since epoch?
    // Date d = dateparser.parse(c.rdatetime);
    // c.rdatetimeepoch = d.getTime() / 1000;

    c.rip = match.group(t++);
    c.requester = match.group(t++);
    c.requestid = match.group(t++);
    c.operation = match.group(t++);
    c.rkey = match.group(t++);
    c.requesturi = match.group(t++);
    // System.err.println(c.requesturi);
    /*
     * // Zemanta specific data extractor try { Matcher m2 =
     * regexrid.matcher(c.requesturi); m2.find(); c.rid = m2.group(1); } catch
     * (Exception e) { c.rid = null; }
     */
    c.httpstatus = toInt(match.group(t++));
    c.errorcode = match.group(t++);
    c.bytessent = toInt(match.group(t++));
    c.objsize = toInt(match.group(t++));
    c.totaltime = toInt(match.group(t++));
    c.turnaroundtime = toInt(match.group(t++));
    c.referer = match.group(t++);
    c.useragent = match.group(t++);

    return (c);
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
  private def dehyphenate(s: String): String =,
    if (s matches "-") null else s
}

/**
 * S3LogObject
 */
case class S3LogObject(
  bucketowner: String,
  bucketname: String,
  rdatetime: String,
  rdatetimeepoch: Long, // TODO: implement this
  rip: String,
  requester: String,
  requestid: String,
  operation: String,
  rkey: String,
  requesturi: String,
  httpstatus: Integer,
  errorcode: String,
  bytessent: Integer,
  objsize: Integer,
  totaltime: Integer,
  turnaroundtime: Integer,
  referer: String,
  useragent: String)