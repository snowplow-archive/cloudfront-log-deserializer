# Hive Deserializer for Amazon S3 and CloudFront Access Logs

## Introduction

amazon-s3log-serde is a Deserializer to import Amazon Web Services' S3 and CloudFront access logs into [Apache Hive] [hive] ready for analysis.

This Deserializer serves as the basis for the SnowPlow-specific Deserializers found [in the SnowPlow project] [snowplowserdes] but should be generally useful for anyone wanting to analyse AWS access log files in Hive, whether SnowPlow users or not.

amazon-s3log-serde is written in Scala and is [available] [downloads] from GitHub as a minified jarfile. It has slightly different functionality from the [S3LogDeserializer] [s3logdeserializer] which comes bundled with Hive (the differences are detailed below).

## The AWS access log format

Amazon Web Services' S3 and CloudFront access logs both use the [W3C extended format] [w3cformat], consisting of the following fields:

* TODO
* TODO
* TODO
* TODO
* TODO

Fields are tab-separated, with multi-part fields (e.g. user agent) enclosed in double quotes.

Here are a couple of example log entries for CloudFront access to SnowPlow:

    TODO
    TODO 

## The Hive table format

amazon-s3log-serde maps the AWS log format very directly onto an equivalent Hive table structure. The only transformation is that the querystring on the accessed URI is converted into a Hive `MAP<STRING, STRING>`.

Here is the Hive table definition in full:

    TODO

## Differences from S3LogDeserializer

This Deserializer differs in a few subtle but important ways from the [S3LogDeserializer] [s3logdeserializer] which comes as part of the Apache Hive project:

* TODO
* This Deserializer has up-to-date documentation available (this file!)

## Usage

You can download a minified jarfile for amazon-s3log-serde from GitHub from the [Downloads] [downloads] menu.

Once you have the jarfile on your classpath, using this Deserializer with Hive is easy:

    TODO 

## Copyright and license

amazon-s3log-serde is copyright 2012 Orderly Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[hive]: http://hive.apache.org/ 
[snowplowserdes]: https://github.com/snowplow/snowplow/tree/master/serdes 
[license]: http://www.apache.org/licenses/LICENSE-2.0
[w3cformat]: http://www.w3.org/TR/WD-logfile.html 
[downloads]: https://github.com/snowplow/amazon-s3log-serde/downloads
[s3logdeserializer]: http://javasourcecode.org/html/open-source/hive/hive-0.7.1/org/apache/hadoop/hive/contrib/serde2/s3/S3LogDeserializer.html