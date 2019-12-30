using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Threading;
using CsvHelper;
using CsvHelper.Configuration;
using Newtonsoft.Json.Linq;
using Parquet;
using Parquet.Data;

#pragma warning disable 1591 // ReSharper disable InconsistentNaming
#pragma warning disable 1573 // disable warning for missing documentation for internal cancellation tokens

namespace Frends.Community.Apache.Parquet
{
    public class ParquetTasks
    {
        /// <summary>
        /// Converts csv file to Parquet file
        /// </summary>
        /// <param name="input">Input parameters</param>
        /// <param name="csvOptions">CSV file options</param>
        /// <param name="parquetOptions">Parquet file options</param>
        /// <returns>Object with following properties: bool Success, string StatusMsg, string ParquetFileName</returns>
        public static WriteResult ConvertCsvToParquet([PropertyTab] WriteInput input, [PropertyTab] WriteCSVOptions csvOptions, [PropertyTab] WriteParquetOptions parquetOptions,CancellationToken cancellationToken)
        {
            int csvRowCount = 0;

            try
            {
                var encoding = Definitions.GetEncoding(csvOptions.FileEncoding, csvOptions.EnableBom, csvOptions.EncodingInString);
                var jsonConf = JToken.Parse(input.Schema);

                // Create CSV configuration
                var csvConfiguration = new Configuration
                {
                    CultureInfo = new CultureInfo(csvOptions.CultureInfo),
                    Delimiter = csvOptions.CsvDelimiter,
                    HasHeaderRecord = csvOptions.ContainsHeaderRow,
                    IgnoreQuotes = csvOptions.IgnoreQuotes,
                    TrimOptions = csvOptions.TrimOutput ? TrimOptions.Trim : TrimOptions.None
                };

                var config = new Config(jsonConf);

                // Create schema for Parquet
                var dataFields = CreateDataFields(jsonConf);
                var schema = new Schema(dataFields);
                int colCount = schema.Fields.Count;

                // Decrease ParquetRowGroupSize if possible
                var fileInfo = new FileInfo(input.CsvFileName);
                long csvFileSive = fileInfo.Length;

                long parquetRowGroupSize = parquetOptions.ParquetRowGroupSize;

                if( parquetRowGroupSize < 1 )
                {
                    throw new ArgumentException("ParguetRowGroupSize must be greater than 0.");
                }

                // Maximum rows of csv file: size of the file
                if (parquetRowGroupSize  > fileInfo.Length)
                {
                    parquetRowGroupSize = fileInfo.Length;
                }



                // Open file and read csv
                // CSV is row-oriented, Parquet is column-oriented.

                using (var reader = new StreamReader(input.CsvFileName, encoding))
                {
                    using (var csv = new CsvReader(reader, csvConfiguration))
                    {
                        // Read header row
                        if (csvOptions.ContainsHeaderRow)
                        {
                            csv.Read();
                            csv.ReadHeader();
                        }

                        using (Stream fileStream = System.IO.File.Open(input.OuputFileName, FileMode.CreateNew))
                        {
                            using (var parquetWriter = new ParquetWriter(schema, fileStream))
                            {
                                parquetWriter.CompressionMethod = Definitions.GetCompressionMethod(parquetOptions.ParquetCompressionMethod);

                                // Jagged array because column orientation
                                string[][] csvArr = new string[colCount][];
                                for (int i = 0; i < colCount; i++)
                                {
                                    csvArr[i] = new string[parquetRowGroupSize];
                                }

                                long dataIndex = 0;

                                // Read csv rows
                                while (csv.Read())
                                {
                                    // Insert data to structure
                                    for (int i = 0; i < colCount; i++)
                                    {
                                        csvArr[i][dataIndex] = csv.GetField(i);
                                    }

                                    dataIndex++;

                                    // Write data if data structure is full
                                    if (dataIndex >= parquetRowGroupSize)
                                    {
                                        Writer.WriteGroup(csvArr, dataIndex, parquetWriter, dataFields, config);
                                        dataIndex = 0;
                                    }

                                    csvRowCount++;

                                    // Check if process is terminated
                                    cancellationToken.ThrowIfCancellationRequested();
                                }

                                // Write non-empty data structure
                                if (dataIndex > 0)
                                {
                                    Writer.WriteGroup(csvArr, dataIndex, parquetWriter, dataFields, config);
                                }
                            }
                        }
                    }
                }

                // Return success
                return new WriteResult()
                {
                    Success = true,
                    ParquetFileName = input.OuputFileName,
                    StatusMessage = "ok",
                    Rows = csvRowCount
                };
            }
            catch (Exception e)
            {
                // Throw exception or return error

                if(input.ThrowExceptionOnErrorResponse)
                {
                    throw;
                }
                return new WriteResult()
                {
                    Success = false,
                    ParquetFileName = "",
                    StatusMessage = e.ToString(),
                    Rows = csvRowCount
                };
            }
        }

        /// <summary>
        /// Get Parquet Datafields from task's JSON configuration
        /// </summary>
        /// <param name="json">JSON configuration</param>
        /// <returns>List of datafields</returns>
        private static List<DataField> CreateDataFields(JToken json)
        {
            var fields = new List<DataField>();

            foreach(var element in json)
            {
                string type = element.Value<string>("type");

                bool nullable = false;
                if (type.EndsWith("?"))
                {
                    nullable = true;
                }

                var field = new DataField(element.Value<string>("name"), DataTypes.GetDataType(type), nullable);
                fields.Add(field);
            }

            return fields;
        }
    }
}
