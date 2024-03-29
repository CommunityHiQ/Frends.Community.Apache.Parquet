This repository is archived and the task has been moved to https://github.com/CommunityHiQ/Frends.Community.Apache

# Frends.Community.Apache.Parquet

FRENDS Community Task for Apache Parquet related operations.

Task operations that use parquet-dotnet library for writing Parquet files
https://github.com/elastacloud/parquet-dotnet

- [Installing](#installing)
- [Tasks](#tasks)
     - [ConvertCsvToParquet](#ConvertCsvToParquet)
     - [ConvertJsonToParquet](#ConvertJsonToParquet)
- [Building](#building)
- [Contributing](#contributing)
- [Change Log](#change-log)

# Installing

You can install the task via FRENDS UI Task View or you can find the nuget package from the following nuget feed
'Insert nuget feed here'

# Tasks

## ConvertCsvToParquet
Converts CSV file to Parquet format using given metadata.

### Properties

#### Input
| Property | Type | Description | Example |
| -------- | -------- | -------- | -------- |
| Schema | json | Schema for CSV data. Types: boolean, datetime, datetimeoffset, decimal, double, float, int16, int, int32, int64, string and unspecified where ? means nullable column. Special characters are not allowed in column names. (Eg. characters '. ,;{}()=' have proven to be problematic.). Array syntax is array\<type\>. | [  {"name": "Id", "type": "int"}, {"name": "Name", "type": "string"}, {"name": "Desc", "type": "string?"}, {"name": "number", "type": "decimal?", "culture": "en-US"}, {"name": "IntArray", "type": "array\<Int32\>"} ] |
| CSV Filename | string |  Full path to the file to be read. | c:\temp\test.csv |
| Output Filename | string |Full path to the file to be write. | c:\temp\test.parquet |
| Throw exception on error response | bool | Do not handle exceptions if set true. Otherwise catch exception and return error message. | true |

Note: Decimals, floats and doubles have "fi-FI" default culture. Decimal separator is ",". Array's items are combined with "|", eg. "1|2|3".

#### CSV Options
| Property | Type | Description | Example |
| -------- | -------- | -------- | -------- |
| CSV Delimeter | string | The separator used in the csv string. | ; |
| ContainsHeaderRow    | bool                 | This flag tells the reader if there is a header row in the CSV string. Default is true. |  true |
| TrimOutput           | bool                 | This flag tells the reader to trim whitespace from the beginning and ending of the field value when reading.              |  false |
| Ignore quotes | bool | This flag indicates if quotes should be ignored when parsing and be treated like any other character. | true |
| CultureInfo          | string               | The culture info to parse the file with, e.g. for decimal separators. InvariantCulture will be used by default. See list of cultures [here](https://msdn.microsoft.com/en-us/library/ee825488(v=cs.20).aspx); use the Language Culture Name. <br> NOTE: Due to an issue with the CsvHelpers library, all CSV tasks will use the culture info setting of the first CSV task in the process; you cannot use different cultures for reading and parsing CSV files in the same process.|   |
| FileEncoding                                | Enum           | Encoding for the read content. By selecting 'Other' you can use any encoding. | |
| EncodingInString                            | string         | The name of encoding to use. Required if the FileEncoding choice is 'Other'. A partial list of supported encoding names: https://msdn.microsoft.com/en-us/library/system.text.encoding.getencodings(v=vs.110).aspx | `iso-8859-1` |
| EnableBom | bool |A parameter specifies whether to provide a Unicode byte order mark. | false |

#### Parquet Options
| Property | Type | Description | Example |
| -------- | -------- | -------- | -------- |
| Parquet row group size | number | Parquet files row group size. Batch size should be large enough because of perfomance later. | 5000 |
| Parquet compression method | Enum | Parquet's compression level. GZip (smallest filesize) / Snappy / None | Gzip |
| Count rows before processing | bool | Count CSV file rows before processing. If row count if smaller than Parquet row group size, decrease group size. Because this operation reads CSV file before processing, CSV file is processed two times. | false |

### Returns

| Property | Type | Description | Example |
| -------- | -------- | -------- | -------- |
| Success | bool | Tells if the operation was successful (true/false)  | true|
| StatusMessage | string | "ok" when Success==true. Otherwise returns error message |  "ok" |
| ParquetFileName | string | Full path to the written file. (Same as Output filename parameter) | c:\temp\test.parquet |
| Rows | number | Number of handled rows. | 124 |


## ConvertJsonToParquet
Converts JSON file to Parquet format using given metadata. Json structure must be array.

### Properties

#### Input
| Property | Type | Description | Example |
| -------- | -------- | -------- | -------- |
| Schema | json | Schema for CSV data. Types: boolean, datetime, datetimeoffset, decimal, double, float, int16, int, int32, int64, string and unspecified where ? means nullable column. Special characters are not allowed in column names. (Eg. characters '. ,;{}()=' have proven to be problematic.). | [  {"name": "Id", "type": "int"}, {"name": "Name", "type": "string"}, {"name": "Desc", "type": "string?"}, {"name": "number", "type": "decimal?"}, {"name": "IntArray", "type": "array\<Int32\>"} ] |
| JSON Filename | string |  Full path to the file to be read. | c:\temp\test.json |
| Output Filename | string |Full path to the file to be write. | c:\temp\test.parquet |
| Throw exception on error response | bool | Do not handle exceptions if set true. Otherwise catch exception and return error message. | true |

Note: Decimals, floats and doubles have InvarianCultere as default culture (Decimal separator is "."). Do NOT set culture without testing.

Arrays are in json format: "IntArray": "[1,2,3]"

#### JSON Options
| Property | Type | Description | Example |
| -------- | -------- | -------- | -------- |
| FileEncoding                                | Enum           | Encoding for the read content. By selecting 'Other' you can use any encoding. | |
| EncodingInString                            | string         | The name of encoding to use. Required if the FileEncoding choice is 'Other'. A partial list of supported encoding names: https://msdn.microsoft.com/en-us/library/system.text.encoding.getencodings(v=vs.110).aspx | `iso-8859-1` |
| EnableBom | bool |A parameter specifies whether to provide a Unicode byte order mark. | false |

#### Parquet Options
| Property | Type | Description | Example |
| -------- | -------- | -------- | -------- |
| Parquet row group size | number | Parquet files row group size. Batch size should be large enough because of perfomance later. | 5000 |
| Parquet compression method | Enum | Parquet's compression level. GZip (smallest filesize) / Snappy / None | Gzip |


### Returns

| Property | Type | Description | Example |
| -------- | -------- | -------- | -------- |
| Success | bool | Tells if the operation was successful (true/false)  | true|
| StatusMessage | string | "ok" when Success==true. Otherwise returns error message |  "ok" |
| ParquetFileName | string | Full path to the written file. (Same as Output filename parameter) | c:\temp\test.parquet |
| Rows | number | Number of handled rows. | 124 |

License
=======
This project is licensed under the MIT License - see the LICENSE file for details

Building
========

Clone a copy of the repo

`git clone https://github.com/CommunityHiQ/Frends.Community.Apache.Parquet`

Restore dependencies

`dotnet restore`

Rebuild the project

`dotnet build` 

Run tests

`dotnet test`

Create a nuget package

`dotnet pack -c Release`

Contributing
============
When contributing to this repository, please first discuss the change you wish to make via issue, email, or any other method with the owners of this repository before making a change.

1. Fork the repo on GitHub
2. Clone the project to your own machine
3. Commit changes to your own branch
4. Push your work back up to your fork
5. Submit a Pull request so that we can review your changes

NOTE: Be sure to merge the latest from "upstream" before making a pull request!

# Change Log

| Version | Changes |
| ----- | ----- |
| 1.0.4 |Initial version - converts CSV file to Parquet file |
| 1.0.5 |Decimal separator format added decimals, doubles and floats |
| 1.0.6 |Set default culture to "fi-FI" for decimal separator |
| 1.0.7 |Memory allocation optimized. A new option to analyze maximum Parquet row group size added. |
| 1.0.8 |Standard datatypes (non-nullable) are now supported. |
| 1.1.0 |Arrays (repeatable fields) are now supported (beta, write-only) |
| 1.2.1 |JSON - converts json array to Parquet file 
