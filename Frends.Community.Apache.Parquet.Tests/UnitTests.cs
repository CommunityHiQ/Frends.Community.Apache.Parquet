using NUnit.Framework;
using Parquet;
using System;
using System.IO;
using System.Linq;
using System.Text;
using Par = Parquet;

namespace Frends.Community.Apache.Parquet.Tests
{
    public class Tests
    {
        // File paths
        private static readonly string _basePath = Path.GetTempPath();
        private readonly string _inputCsvFileName = Path.Combine(_basePath, "testi-csv-" + Path.GetRandomFileName());
        private readonly string _inputCsvFileNameQuotes = Path.Combine(_basePath, "testi-quot-csv-" + Path.GetRandomFileName());
        private readonly string _inputCsvFileNameDecDot = Path.Combine(_basePath, "testi-dec-csv-" + Path.GetRandomFileName());
        private readonly string _inputCsvFileNameDecComma = Path.Combine(_basePath, "testi-dec2-csv-" + Path.GetRandomFileName());
        private readonly string _inputCsvFileNameNoNulls = Path.Combine(_basePath, "testi-nn1-csv-" + Path.GetRandomFileName());
        private readonly string _inputCsvFileNameLarge = Path.Combine(_basePath, "testi-large-csv-" + Path.GetRandomFileName());
        private readonly string _inputCsvFileNameArray = Path.Combine(_basePath, "testi-array1-csv-" + Path.GetRandomFileName());
        private readonly string _inputCsvFileNameArray2 = Path.Combine(_basePath, "testi-array2-csv-" + Path.GetRandomFileName());
        private readonly string _inputCsvFileNameArrayLarge = Path.Combine(_basePath, "testi-array3-csv-" + Path.GetRandomFileName());

        private readonly string _outputFileName = Path.Combine(_basePath, "testi-parquet-" + Path.GetRandomFileName());

        private const string _commonSchema = @"[
    { ""name"": ""Id"", ""type"": ""int?""},
    {""name"": ""Time"", ""type"": ""datetime?"", ""format"": ""dd.MM.yyyy""},
    {""name"": ""Decimal"", ""type"": ""decimal?"", ""culture"": ""en-US""},
    { ""name"": ""Description"", ""type"": ""string?""},
]";

        private const string _commonArraySchema = @"[
                                { ""name"": ""Id"", ""type"": ""int?""},
                                {""name"": ""IntArray"", ""type"": ""array<int>""},
                                {""name"": ""StringArray"", ""type"": ""array<string>""},
                                { ""name"": ""Description"", ""type"": ""string?""},
                            ]";

        [SetUp]
        public void Setup()
        {
            // Write csv test file
            File.WriteAllText(_inputCsvFileName, @"Id;Date;Decimal;Text
1;01.10.2019;5.0;Testirivi 1
1;15.04.2018;3.5;Testirivi 2 - pidempi teksti ja ääkkösiä
;;;Tyhjä rivi 1
4;1.1.2020;9.999;
3;11.11.2011;1.2345;Viimeinen rivi
");
            File.WriteAllText(_inputCsvFileNameQuotes, @"Id;Date;Decimal;Text
1;01.10.2019;5.0;Testirivi 1
1;15.04.2018;3.5;Testirivi 2 - ""pidempi teksti"" ja ääkkösiä
;;;Tyhjä rivi 1
4;1.1.2020;9.999;
3;11.11.2011;1.2345;Viimeinen rivi
");

            File.WriteAllText(_inputCsvFileNameDecComma, @"Id;Decimal
1;12345,6789;12345,6789;12345,6789
2;2,3;2,3;2,3
3;4,4;4,4;4,4
");
            File.WriteAllText(_inputCsvFileNameNoNulls, @"Id;DateTime;Decimal;Text
1;01.10.2019T00:00:00+02:00;5.0;Testirivi 1
2;15.04.2018T00:00:00+02:00;3.5;Testirivi 2 - pidempi teksti ja ääkkösiä
3;31.12.2019T00:00:00+02:00;1.12020;Testirivi 3
4;01.01.2020T00:00:00+02:00;9.999; Testirivi 4
3;11.11.2011T00:00:00+02:00;1.2345;Viimeinen rivi
");

            File.WriteAllText(_inputCsvFileNameDecDot, @"Id;Decimal
1;12345.6789;12345.6789;12345.6789
2;2.3;2.3;2.3
3;4.4;4.4;4.4
");

            File.WriteAllText(_inputCsvFileNameArray, @"Id;Decimal
1;;;Tyhjät alkiot
2;21;Yksi2;Yksi alkio
3;31|32;Yksi3|Kaksi3;Kaksi alkiota
4;41|43|;Yksi4||Kolme4;Tyhjä alkio 
5;|52|53;|Kaksi5|Kolme5;Tyhjä alkio alussa
6;61;Yksi6|Kaksi6|Kolme6;Neljä;Eri kokoiset arrayt
7;71|72|73|74|75||77|78|79|;Yksi7||Kolme7;Sekalaiset testit
8;81;Yksi8|Tyhjä description;
");

            File.WriteAllText(_inputCsvFileNameArray2, @"11.10.2021;2;Tyhjät alkiot;;;;;;;;;;
11.10.2021;2;Yksi alkio;21;Yksi2;false;11.10.2021;2021-10-11T11:00:00+02:00;2.2345;2.2345;2.23455;21;212
13.10.2021;3;Kaksi alkiota;31|32;Yksi3|Kaksi3;false|true;11.10.2021|10.10.2021;2021-10-11T11:00:00+02:00|2021-10-10T10:00:00+02:00;|3.1345|3.312;3.2345|3.0;3.13|3.23;31|32;312|322
14.10.2021;4;Tyhjä alkio keskellä;41||43;Yksi4||Kolme4;false||false;11.10.2021||09.10.2021;2021-10-11T11:00:00+02:00||2021-09-10T09:00:00+02:00;4.2345123||4.3;4.1245||4.3;4.13455||4.32;41||43;412||413
15.10.2021;5;Tyhjä alkio alussa;|52|53;|Kaksi5|Kolme5;|true|false;|10.10.2021|09.10.2021;|2021-10-10T10:00:00+02:00|2021-09-10T09:00:00+02:00;|5.1345123|5.2|5.31232424;|5.4345|5.51;|5.23455|5.312345;|52|531;|522|532
16.10.2021;6;Tyhjä alkio lopussa;61|62|;Yksi6|Kaksi6|Kolme6|;false|true|false|true|;11.10.2021|10.10.2021|9.10.2021|;2021-10.11T11:00:00+02:00|2021-10-10T10:00:00+03:00|2021-09-10T09:00:00+04:00|;6.1345|6.2|6.3124324|;6.13455|6.212345|6.3|;6.1999|6.29999|6.39|;61|62|63|;612|622|632|
17.10.2021;7;Paljon alkioita;71|72|73|74|74|;Yksi7|Kaksi7|Kolme7|Neljä7;false|true|false|true|false|true|true|false|false;11.10.2021|10.10.2021|9.10.2021|8.10.2021|7.10.2021;2021-10.11T11:00:00+02:00|2021-10-10T10:00:00+03:00|2021-09-10T09:00:00+04:00|2021-10-08T08:00:00+02:00;7.1345123|7.2|7.31232424|7.4444444441|7.51234567891234567890;7.1345|7.2|7.3124324|7.4|7.5123123;7.13455|7.212345|7.3|7.4123123;71|72|73|74123123|751234567890;712|722|732|742|16000
"); 
        }

        [TearDown]
        public void TearDown()
        {
            // Remove all test files
            foreach (var name in new string[] { _inputCsvFileNameNoNulls, _inputCsvFileName, _inputCsvFileNameLarge, _inputCsvFileNameQuotes, _inputCsvFileNameDecComma, _inputCsvFileNameDecDot, _outputFileName, _inputCsvFileNameArray })
            {
                if (File.Exists(name))
                {
                    File.Delete(name);
                }
            }
        }

        /// <summary>
        /// Simple csv -> parquet test case.
        /// </summary>
        [Test]
        public void WriteParquetFile()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip
            };

            var input = new WriteInput()
            {
                FileName = _inputCsvFileName,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = _commonSchema
            };

            ParquetTasks.ConvertCsvToParquet(input, options, poptions, new System.Threading.CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);

            Assert.IsTrue(hash == "2b2ea410911658f3fbfa36ec7938d27e", "File checksum didn't match.");
        }

        /// <summary>
        /// Simple csv, no null values
        /// </summary>
        [Test]
        public void WriteParquetFileNoNulls()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip
            };

            var input = new WriteInput()
            {
                FileName = _inputCsvFileNameNoNulls,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = @"[
    { ""name"": ""Id"", ""type"": ""int?""},
    {""name"": ""Time"", ""type"": ""datetime?"", ""format"": ""dd.MM.yyyyTHH:mm:sszzz""},
    {""name"": ""Decimal"", ""type"": ""decimal?"", ""culture"": ""en-US""},
    { ""name"": ""Description"", ""type"": ""string?""},
]".Replace("?\"", "\"")
            };

            ParquetTasks.ConvertCsvToParquet(input, options, poptions, new System.Threading.CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.IsTrue(hash == "026109e0c5583dfce6e10e5757cfc38c", "File checksum didn't match.");
        }

        /// <summary>
        /// Quotes test
        /// </summary>
        [Test]
        public void WriteParquetFileQuotes()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = "",
                IgnoreQuotes = true
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip
            };

            var input = new WriteInput()
            {
                FileName = _inputCsvFileNameQuotes,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = _commonSchema
            };

            ParquetTasks.ConvertCsvToParquet(input, options, poptions, new System.Threading.CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.IsTrue(hash == "c2d14b21c69fec284e20801cf43041f6", "File checksum didn't match.");

        }

        [Test]
        public void ArrayTest1()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Snappy
            };

            var input = new WriteInput()
            {
                FileName = _inputCsvFileNameArray,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = _commonArraySchema
            };

            ParquetTasks.ConvertCsvToParquet(input, options, poptions, new System.Threading.CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.IsTrue(hash == "f3e7683b40a4e2a9dfa09668b378dd4e", "File checksum didn't match.");
        }
       
        [Test]
        public void ArrayTest2()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = "",
                ContainsHeaderRow = false
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Snappy
            };

            var input = new WriteInput()
            {
                FileName = _inputCsvFileNameArray2,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = @"[
                                {""name"": ""paivays"", ""type"": ""datetime?"", ""format"": ""dd.MM.yyyy""},
                                { ""name"": ""Id"", ""type"": ""int?""},
                                { ""name"": ""Description"", ""type"": ""string?""},
                                {""name"": ""AInt"", ""type"": ""array<int>""},
                                {""name"": ""AString"", ""type"": ""array<string>""},
                                {""name"": ""ABool"", ""type"": ""array<boolean>""},
                                {""name"": ""ADatetime"", ""type"": ""array<datetime>"", ""format"": ""dd.MM.yyyy""},
                                {""name"": ""AoffsetDatetime"", ""type"": ""array<datetimeoffset>"",""format"": ""yyyy-MM-ddTHH:mm:sszzz""},
                                {""name"": ""ADecimal"", ""type"": ""array<double>"", ""culture"": ""en-US""},
                                {""name"": ""ADouble"", ""type"": ""array<double>"", ""culture"": ""en-US""},
                                {""name"": ""AFloat"", ""type"": ""array<float>"", ""culture"": ""en-US""},
                                {""name"": ""AInt64"", ""type"": ""array<int64>""},
                                {""name"": ""AInt16"", ""type"": ""array<int32>""}
                            
                            ]"
            };
   
            ParquetTasks.ConvertCsvToParquet(input, options, poptions, new System.Threading.CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.AreEqual("a6cc0c1b414467a3c10708be30361ed6", hash, "File checksum didn't match.");
        }

        /// <summary>
        /// Simple csv -> parquet test case with large group size
        /// </summary>
        [Test]
        public void ArrayTestLarge()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            string schema = CreateLargeArrayCSVFile(_inputCsvFileNameArrayLarge, 1000);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip
            };

            var input = new WriteInput()
            {
                FileName = _inputCsvFileNameArrayLarge,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = schema
            };

            ParquetTasks.ConvertCsvToParquet(input, options, poptions, new System.Threading.CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.IsTrue(hash == "25ed5c84846597294990b32b8fbc4f53", "File checksum didn't match.");
        }

        [Test]
        public void ArrayGetValues()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Snappy
            };

            var input = new WriteInput()
            {
                FileName = _inputCsvFileNameArray,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = _commonArraySchema
            };

            ParquetTasks.ConvertCsvToParquet(input, options, poptions, new System.Threading.CancellationToken());

            Assert.AreEqual(null, ReturnParquetValue(_outputFileName, 1, 0));
            Assert.AreEqual("21", ReturnParquetValue(_outputFileName, 1, 1));
            Assert.AreEqual("31|32", ReturnParquetValue(_outputFileName, 1, 2));
            Assert.AreEqual("41|43", ReturnParquetValue(_outputFileName, 1, 3));
            Assert.AreEqual("71|72|73|74|75||77|78|79", ReturnParquetValue(_outputFileName, 1, 6));

            Assert.AreEqual("", ReturnParquetValue(_outputFileName, 2, 0));
            Assert.AreEqual("Yksi2", ReturnParquetValue(_outputFileName, 2, 1));
            Assert.AreEqual("Yksi3|Kaksi3", ReturnParquetValue(_outputFileName, 2,2));
            Assert.AreEqual("Yksi7||Kolme7", ReturnParquetValue(_outputFileName, 2, 6));

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.IsTrue(hash == "f3e7683b40a4e2a9dfa09668b378dd4e", "File checksum didn't match.");
        }

        /// <summary>
        /// Test case when decimal separator is dot. "."
        /// </summary>
        [Test]
        public void DecimalTestDot1()
        {
            TestTools.RemoveOutputFile(_outputFileName);
            RunDecimalTestNullable("en-US", _inputCsvFileNameDecDot);
            //var hash = TestTools.MD5Hash(_outputFileName);

            Assert.AreEqual(12345.6789m, ReturnParquetValue(_outputFileName, 1, 0));
            Assert.AreEqual(12345.6789f, ReturnParquetValue(_outputFileName, 2, 0));
            Assert.AreEqual(12345.6789d, ReturnParquetValue(_outputFileName, 3, 0));
        }

        /// <summary>
        /// Now cultere is empty -> default should be CultureInfo.InvariantCulture
        /// </summary>
        [Test]
        public void DecimalTestDefault()
        {
            TestTools.RemoveOutputFile(_outputFileName);
            RunDecimalTestNormal("", _inputCsvFileNameDecComma);
            //var hash = TestTools.MD5Hash(_outputFileName);

            Assert.AreEqual(2.3m, ReturnParquetValue(_outputFileName, 1, 1));
            Assert.AreEqual(2.3f, ReturnParquetValue(_outputFileName, 2, 1));
            Assert.AreEqual(2.3d, ReturnParquetValue(_outputFileName, 3, 1));
        }

        /// <summary>
        /// Test case when decimal separator is comma. ","
        /// </summary>
        [Test]
        public void DecimalTestComma()
        {
            TestTools.RemoveOutputFile(_outputFileName);
            RunDecimalTestNullable("fi-FI", _inputCsvFileNameDecComma);
            //var hash = TestTools.MD5Hash(_outputFileName);

            Assert.AreEqual(12345.6789m, ReturnParquetValue(_outputFileName, 1, 0));
            Assert.AreEqual(12345.6789f, ReturnParquetValue(_outputFileName, 2, 0));
            Assert.AreEqual(12345.6789d, ReturnParquetValue(_outputFileName, 3, 0));
        }

        /// <summary>
        /// Reads first value from first group and given column
        /// </summary>
        /// <param name="parquetFilePath">Full filepath</param>
        /// <param name="columnIndex">Column index 0...n</param>
        /// <returns></returns>

        public static object ReturnParquetValue(string parquetFilePath, int columnIndex, int rowIndex)
        {
            var encoding = Definitions.GetEncoding(FileEncoding.UTF8, false, "");

            using var filereader = File.Open(parquetFilePath, FileMode.Open, FileAccess.Read);

            var options = new ParquetOptions { TreatByteArrayAsString = true };
            var parquetReader = new ParquetReader(filereader, options);

            Par.Data.DataField[] dataFields = parquetReader.Schema.GetDataFields();

            using ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(0);

            Par.Data.DataColumn[] columns = dataFields.Select(groupReader.ReadColumn).ToArray();
            Par.Data.DataColumn dataColumn = columns[columnIndex];

            if (dataFields[columnIndex].IsArray)
            {
                int count = 0;
                int index = 0;
                var sb = new StringBuilder("");

                switch (dataFields[columnIndex].DataType)
                {
                    case Par.Data.DataType.String:
                        string[] str = (string[])dataColumn.Data;

                        foreach(var status in dataColumn.RepetitionLevels)
                        {
                            // 0 = start
                            if( index>0 && status == 0)
                            {
                                count++;
                            }
                            if(count == rowIndex)
                            {
                                if( str[index]==null && status==0)
                                {
                                    if( dataColumn.RepetitionLevels.Length==1 || 
                                        dataColumn.RepetitionLevels.Length<=index+1 ||
                                        dataColumn.RepetitionLevels[index+1]==0)
                                    {
                                        return null;
                                    }
                                }
                                sb.Append(str[index]);
                                sb.Append("|");
                            }
                            if(count > rowIndex)
                            {
                                return sb.ToString().TrimEnd('|');
                            }
                            index++;
                        }
                        return sb.ToString().TrimEnd('|');
                        
                    case Par.Data.DataType.Int32:
                        Int32?[] i32 = (Int32?[])dataColumn.Data;

                        foreach (var status in dataColumn.RepetitionLevels)
                        {
                            // 0 = start
                            if (index > 0 && status == 0)
                            {
                                count++;
                            }
                            if (count == rowIndex)
                            {
                                if (i32[index] == null && status == 0)
                                {
                                    // one element
                                    // last element
                                    // no more subelements
                                    if (dataColumn.RepetitionLevels.Length == 1 ||
                                        dataColumn.RepetitionLevels.Length <= index + 1 ||
                                        dataColumn.RepetitionLevels[index + 1] == 0)
                                    {
                                        return null;
                                    }
                                }
                                sb.Append(i32[index].ToString());
                                sb.Append("|");
                            }
                            if (count > rowIndex)
                            {
                                return sb.ToString().TrimEnd('|');
                            }
                            index++;
                        }
                        return sb.ToString().TrimEnd('|');
                    case Par.Data.DataType.Double:
                        double?[] dbl = (double?[])dataColumn.Data;

                        foreach (var status in dataColumn.RepetitionLevels)
                        {
                            // 0 = start
                            if (index > 0 && status == 0)
                            {
                                count++;
                            }
                            if (count == rowIndex)
                            {
                                if (dbl[index] == null && status == 0)
                                {
                                    // one element
                                    // last element
                                    // no more subelements
                                    if (dataColumn.RepetitionLevels.Length == 1 ||
                                        dataColumn.RepetitionLevels.Length <= index + 1 ||
                                        dataColumn.RepetitionLevels[index + 1] == 0)
                                    {
                                        return null;
                                    }
                                }
                                if(dbl[index]!=null)
                                {
                                    double dstr = (double)dbl[index];
                                    sb.Append(dstr.ToString(System.Globalization.CultureInfo.InvariantCulture));
                                }
                                sb.Append("|");
                            }
                            if (count > rowIndex)
                            {
                                return sb.ToString().TrimEnd('|');
                            }
                            index++;
                        }
                        return sb.ToString().TrimEnd('|');
                    default:
                        throw new System.Exception("Unknown array datatype:" + dataFields[columnIndex].DataType);
                }
            }
            else if (dataFields[columnIndex].HasNulls)
            {
                switch (dataFields[columnIndex].DataType)
                {
                    case Par.Data.DataType.Decimal:
                        decimal?[] dec = (decimal?[])dataColumn.Data;
                        return dec[rowIndex];
                    case Par.Data.DataType.Float:
                        float?[] flo = (float?[])dataColumn.Data;
                        return flo[rowIndex];
                    case Par.Data.DataType.Double:
                        double?[] dou = (double?[])dataColumn.Data;
                        return dou[rowIndex];
                    case Par.Data.DataType.String:
                        string[] str = (string[])dataColumn.Data;
                        return str[rowIndex];
                    case Par.Data.DataType.Int32:
                        Int32?[] i32 = (Int32?[])dataColumn.Data;
                        return i32[rowIndex];
                    default:
                        throw new System.Exception("Unknown nullable datatype:" + dataFields[columnIndex].DataType);
                }
            }
            else
            {
                switch (dataFields[columnIndex].DataType)
                {
                    case Par.Data.DataType.Decimal:
                        decimal[] dec = (decimal[])dataColumn.Data;
                        return dec[rowIndex];
                    case Par.Data.DataType.Float:
                        float[] flo = (float[])dataColumn.Data;
                        return flo[rowIndex];
                    case Par.Data.DataType.Double:
                        double[] dou = (double[])dataColumn.Data;
                        return dou[rowIndex];
                    case Par.Data.DataType.String:
                        string[] str = (string[])dataColumn.Data;
                        return str[rowIndex];
                    case Par.Data.DataType.Int32:
                        Int32[] i32 = (Int32[])dataColumn.Data;
                        return i32[rowIndex];
                    default:
                        throw new System.Exception("Unknown datatype:" + dataFields[columnIndex].DataType);
                }
            }
        }

        /// <summary>
        /// Runs decimal tests using static schema and given input
        /// </summary>
        /// <param name="decimalType"></param>
        /// <param name="inputFileName"></param>
        private void RunDecimalTestNullable(string cultureStr, string inputFileName)
        {
            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5,
                ParquetCompressionMethod = CompressionType.Snappy
            };

            var input = new WriteInput()
            {
                FileName = inputFileName,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = @"[
    {""name"": ""Id"", ""type"": ""int?""},
    {""name"": ""Decimal"", ""type"": ""decimal?""" + (System.String.IsNullOrEmpty(cultureStr) ? "}" : @",""culture"": """ + cultureStr + @"""}") + @",
    {""name"": ""Float"", ""type"": ""float?""" + (System.String.IsNullOrEmpty(cultureStr) ? "}" : @",""culture"": """ + cultureStr + @"""}") + @",
    {""name"": ""Double"", ""type"": ""double?""" + (System.String.IsNullOrEmpty(cultureStr) ? "}" : @",""culture"": """ + cultureStr + @"""}") + @",
]"
            };

            ParquetTasks.ConvertCsvToParquet(input, options, poptions, new System.Threading.CancellationToken());
        }

        private void RunDecimalTestNormal(string cultureStr, string inputFileName)
        {
            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5,
                ParquetCompressionMethod = CompressionType.Snappy
            };

            var input = new WriteInput()
            {
                FileName = inputFileName,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = @"[
    {""name"": ""Id"", ""type"": ""int""},
    {""name"": ""Decimal"", ""type"": ""decimal""" + (System.String.IsNullOrEmpty(cultureStr) ? "}" : @",""culture"": """ + cultureStr + @"""}") + @",
    {""name"": ""Float"", ""type"": ""float""" + (System.String.IsNullOrEmpty(cultureStr) ? "}" : @",""culture"": """ + cultureStr + @"""}") + @",
    {""name"": ""Double"", ""type"": ""double""" + (System.String.IsNullOrEmpty(cultureStr) ? "}" : @",""culture"": """ + cultureStr + @"""}") + @",
]"
            };

            ParquetTasks.ConvertCsvToParquet(input, options, poptions, new System.Threading.CancellationToken());
        }

        /// <summary>
        /// Test case for counting rows before processing
        /// </summary>
        [Test]
        public void WriteParquetFileCountRows()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip,
                CountRowsBeforeProcessing = true
            };

            var input = new WriteInput()
            {
                FileName = _inputCsvFileName,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = _commonSchema
            };

            ParquetTasks.ConvertCsvToParquet(input, options, poptions, new System.Threading.CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.IsTrue(hash == "2b2ea410911658f3fbfa36ec7938d27e", "File checksum didn't match.");
        }


        /// <summary>
        /// Simple csv -> parquet test case with large group size
        /// </summary>
        [Test]
        public void WriteParquetFileMaxMemory()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            string schema = CreateLargeCSVFile(_inputCsvFileNameLarge, 1000000);

            var options = new WriteCSVOptions()
            {
                CsvDelimiter = ";",
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptions()
            {
                ParquetRowGroupSize = 100000000,
                ParquetCompressionMethod = CompressionType.Gzip
            };

            var input = new WriteInput()
            {
                FileName = _inputCsvFileNameLarge,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = schema
            };

            ParquetTasks.ConvertCsvToParquet(input, options, poptions, new System.Threading.CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.IsTrue(hash == "20ef70f3939604391e48276a636c4716", "File checksum didn't match.");
        }


        /// <summary>
        /// Creates CSV file and returns JSON schema for Parquet
        /// </summary>
        /// <param name="fileName">CSV filename, full path</param>
        /// <param name="rows">Number of rows</param>
        /// <returns>JSON schema of the csv file</returns>
        private string CreateLargeCSVFile(string fileName, int rows)
        {
            using (StreamWriter outputFile = new StreamWriter(fileName))
            {
                var startDate = DateTime.Parse("1995-01-01");

                outputFile.WriteLine("Id;Date;Decimal;Text");
                for (int i = 0; i < rows; i++)
                {
                    double dec1 = (i + 1.0) + (i % 100) / 100.0;
                    double days = i % 10000;
                    outputFile.WriteLine((i + 1) + ";" + startDate.AddDays(days).ToString("dd.MM.yyyy") + ";" +
                        dec1.ToString(System.Globalization.CultureInfo.InvariantCulture) + ";" +
                        "Testirivi " + (i + 1) + " ja jotain tekstiä.");
                }
            }

            return _commonSchema.Replace("int?", "int");
        }


        /// <summary>
        /// Creates CSV file with arrays and returns JSON schema for Parquet
        /// </summary>
        /// <param name="fileName">CSV filename, full path</param>
        /// <param name="rows">Number of rows</param>
        /// <returns>JSON schema of the csv file</returns>
        private string CreateLargeArrayCSVFile(string fileName, int rows)
        {
            using (StreamWriter outputFile = new StreamWriter(fileName))
            {
                outputFile.WriteLine("Id;IntArray;StringArray;Description");

                for (int i = 0; i < rows; i++)
                {
                    int lask = i % 3;
                    int rivi = i + 1;

                    if( lask == 0)
                    {
                        outputFile.WriteLine($"{rivi};{rivi - 1};Eka;Rivi {rivi}");
                    } else if(lask==1)
                    {
                        outputFile.WriteLine($"{rivi};{rivi - 1}|{rivi};Eka|Toka;Rivi {rivi}");
                    }
                    else
                    {
                        outputFile.WriteLine($"{rivi};{rivi - 1}|{rivi}|{rivi + 1};Eka|Toka|Kolmas;Rivi {rivi}");
                    }
                }
            }
            string schema = @"[
                                { ""name"": ""Id"", ""type"": ""int?""},
                                {""name"": ""IntArray"", ""type"": ""array<int>""},
                                {""name"": ""StringArray"", ""type"": ""array<string>""},
                                { ""name"": ""Description"", ""type"": ""string?""},
                            ]";
            return schema;
        }
    }
}