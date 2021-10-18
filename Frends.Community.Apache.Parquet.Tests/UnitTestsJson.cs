using NUnit.Framework;
using Parquet;
using System;
using System.IO;
using System.Linq;
using Par = Parquet;


namespace Frends.Community.Apache.Parquet.Tests
{
    public class TestsJson
    {
        // File paths
        private static readonly string _basePath = Path.GetTempPath();
        private readonly string _inputJsonFileName = Path.Combine(_basePath, "testi-json-" + Path.GetRandomFileName());
        private readonly string _inputJsonFileNameArray = Path.Combine(_basePath, "testi-array-json-" + Path.GetRandomFileName());
        private readonly string _inputJsonFileNameDecDot = Path.Combine(_basePath, "testi-dec2-json-" + Path.GetRandomFileName());
        private readonly string _inputJsonFileNameNoNulls = Path.Combine(_basePath, "testi-nonull-json-" + Path.GetRandomFileName());
        private readonly string _inputJsonFileNameLarge = Path.Combine(_basePath, "testi-large-json-" + Path.GetRandomFileName());
        private readonly string _inputJsonFileNameArray2 = Path.Combine(_basePath, "testi-array2-csv-" + Path.GetRandomFileName());
        private readonly string _inputJsonFileNameArrayLarge = Path.Combine(_basePath, "testi-array3-csv-" + Path.GetRandomFileName());
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
            File.WriteAllText(_inputJsonFileName, @"[
    { ""Id"": 1, ""Time"": ""01.10.2019"", ""Decimal"": 5.0, ""Description"": ""Testirivi 1"" },
    { ""Id"": 1, ""Time"": ""15.04.2018"", ""Decimal"": 3.5, ""Description"": ""Testirivi 2 - pidempi teksti ja ääkkösiä"" },
    { ""Id"": null , ""Time"": null, ""Decimal"": null, ""Description"": ""Tyhjä rivi 1"" },
    { ""Description"": ""Tyhjä rivi 2"" },
    { ""Id"": 4, ""Time"": ""1.1.2020"", ""Decimal"": 9.999  },
    { ""Id"": 3, ""Time"": ""3.11.2011"", ""Decimal"": 1.12345, 
      ""Description"": ""Viimeinen rivi"" }
]");
            File.WriteAllText(_inputJsonFileNameDecDot, @"[
    { ""Id"": 1, ""Decimal"": 12345.6789, ""Float"": 12345.6789, ""Double"": 12345.6789 },
    { ""Id"": 2, ""Decimal"": 2.3, ""Float"": 2.3, ""Double"": 2.3 },
    { ""Id"": 3, ""Decimal"": 4.4, ""Float"": 4.4, ""Double"": 4.4 }
]");

            File.WriteAllText(_inputJsonFileNameNoNulls, @"[
    { ""Id"": 1, ""Time"": ""01.10.2019T00:00:00+02:00"", ""Decimal"": 5.1, ""Description"": ""Testirivi 1"" },
    { ""Id"": 2, ""Time"": ""15.04.2018T00:00:00+02:00"", ""Decimal"": 3.5, ""Description"": ""Testirivi 2 - pidempi teksti ja ääkkösiä"" },
    { ""Id"": 3, ""Time"": ""31.12.2019T00:00:00+02:00"", ""Decimal"": 1.12020, ""Description"": ""Testirivi 3"" },
    { ""Id"": 4, ""Time"": ""01.01.2020T00:00:00+02:00"", ""Decimal"": 9.999, ""Description"": ""Testirivi 4"" },
    { ""Id"": 5, ""Time"": ""11.11.2011T00:00:00+02:30"", ""Decimal"": 1.2345, ""Description"": ""Viimeinen rivi"" }
]
");

            File.WriteAllText(_inputJsonFileNameArray, @"[ 
{ ""Id"": 1, ""IntArray"": [ ], ""StringArray"": [ ], ""Description"": ""Tyhjät alkiot""},
{ ""Id"": 2, ""IntArray"": [ 21 ], ""StringArray"": [ ""Yksi2""], ""Description"": ""Yksi alkio""},
{ ""Id"": 3, ""IntArray"": [ 31,32 ], ""StringArray"": [ ""Yksi3"",""Kaksi3""], ""Description"": ""Kaksi alkiota""},
{ ""Id"": 4, ""IntArray"": [ 41, null, 43 ], ""StringArray"": [ ""Yksi4"",null,""Kolme4""], ""Description"": ""Tyhjä alkio""},
{ ""Id"": 5, ""IntArray"": [ null, 52,53 ], ""StringArray"": [ null,""Kaksi5"",""Kolme5""], ""Description"": ""Tyhjä alkio alussa""},
{ ""Id"": 6, ""IntArray"": [ 61 ], ""StringArray"": [""Yksi6"",""Kaksi6"",""Kolme6""], ""Description"": ""Eri kokoiset arrayt""},
{ ""Id"": 7, ""IntArray"": [ 71,72,73,74,75,null,77,78,79 ], ""StringArray"": [""Yksi7"",null,null,""Kolme7""], ""Description"": ""Sekalaiset testit""},
{ ""Id"": 8, ""IntArray"": [ 81 ], ""StringArray"": [""Yksi8"", ""Tyhjä description""], ""Description"": """"},
{ ""Id"": 9, ""IntArray"": [ 91,92, null ], ""StringArray"": [""Yksi9"",""Kaksi9"", null], ""Description"": ""Tyhjä alkio lopussa""},
{ ""Id"": 10, ""IntArray"": [ 81 ], ""StringArray"": [""Yksi8"", ""Tyhjä description2""], ""Description"": null},
{ ""Id"": 11, ""IntArray"": [ 81 ], ""StringArray"": [""Yksi8"", ""Tyhjä description3""]},
{ ""Id"": 12, ""Description"": ""Tyhjät alkiot 2""}
]");

            File.WriteAllText(_inputJsonFileNameArray2, @"[
    { ""paivays"": ""11.10.2021"", ""Id"":  1, ""Description"": ""Tyhjät alkiot"", ""AInt"": [], ""AString"": [], ""ABool"": [], ""ADateTime"": [], ""AoffsetDatetime"": [],  ""ADecimal"": [], ""ADouble"": [], ""AFloat"": [], ""AInt64"": [], ""AInt16"": [] },
    { ""paivays"": ""11.10.2021"", ""Id"":  2, ""Description"": ""Yksi alkio"", ""AInt"": [ 21 ]  , ""AString"": [""Yksi2""], ""ABool"": [false], ""ADateTime"": [""11.10.2021""], ""AoffsetDatetime"": [""2021-10-11T11:00:00+02:00""],  ""ADecimal"": [2.2345], ""ADouble"": [2.2345], ""AFloat"": [2.23455], ""AInt64"": [21], ""AInt16"": [212] },
    { ""paivays"": ""17.10.2021"", ""Id"":  7, ""Description"": ""Paljon alkioita"", ""AInt"": [71,72,null,74,75,76,null]  , ""AString"": [""Yksi7"",""Kaksi7"",""Kolme7"",null,""Viis7""], ""ABool"": [true,false,true,false], ""ADateTime"": [""11.10.2021"",""10.10.2021"",null,""8.10.2021""], ""AoffsetDatetime"": [""2021;2021-10-11T11:00:00+02:00"",null, ""2021-10-09T11:00:00+02:00""],  ""ADecimal"": [7.1345123,null,7.3345123], ""ADouble"": [7.1445123,null,7.3445123], ""AFloat"": [7.4145123,null,7.4345123], ""AInt64"": [711,null,713,712312321313], ""AInt16"": [711,null,712,16000] }
]
");

        }

        [TearDown]
        public void TearDown()
        {
            // Remove all test files
            foreach (var name in new string[] { _inputJsonFileName, _inputJsonFileNameArray, _inputJsonFileNameDecDot })
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

            var options = new WriteJSONOptions()
            {
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptionsJSON()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip
            };

            var input = new WriteInput()
            {
                FileName = _inputJsonFileName,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = _commonSchema
            };

            ParquetTasks.ConvertJsonToParquet(input, options, poptions, new System.Threading.CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);

            Assert.IsTrue(hash == "e569d2dac1618d8f44f544a2e8fff956", "File checksum didn't match." + hash);
        }

        [Test]
        public void ArrayTest1()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteJSONOptions()
            {
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptionsJSON()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Snappy
            };

            var input = new WriteInput()
            {
                FileName = _inputJsonFileNameArray,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = _commonArraySchema
            };

            

            ParquetTasks.ConvertJsonToParquet(input, options, poptions, new System.Threading.CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.IsTrue(hash == "92bcd1ec5c22d943b30a383010020da5", "File checksum didn't match.");
        }

        [Test]
        public void ArrayTest2()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteJSONOptions()
            {
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptionsJSON()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Snappy
            };

            var input = new WriteInput()
            {
                FileName = _inputJsonFileNameArray2,
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
                                {""name"": ""ADecimal"", ""type"": ""array<double>""},
                                {""name"": ""ADouble"", ""type"": ""array<double>""},
                                {""name"": ""AFloat"", ""type"": ""array<float>""},
                                {""name"": ""AInt64"", ""type"": ""array<int64>""},
                                {""name"": ""AInt16"", ""type"": ""array<int32>""}
                            
                            ]"
            };

            ParquetTasks.ConvertJsonToParquet(input, options, poptions, new System.Threading.CancellationToken());


            Assert.AreEqual("21", Tests.ReturnParquetValue(_outputFileName, 3, 1));
            Assert.AreEqual("71|72||74|75|76", Tests.ReturnParquetValue(_outputFileName, 3, 2));

            Assert.AreEqual("Yksi2", Tests.ReturnParquetValue(_outputFileName, 4, 1));
            Assert.AreEqual("Yksi7|Kaksi7|Kolme7||Viis7", Tests.ReturnParquetValue(_outputFileName, 4, 2));

            Assert.AreEqual("2.2345", Tests.ReturnParquetValue(_outputFileName, 8, 1));
            Assert.AreEqual("7.1345123||7.3345123", Tests.ReturnParquetValue(_outputFileName, 8, 2));

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.AreEqual("08f4d0dd5ffd8c214d20906dea423a98", hash, "File checksum didn't match.");
        }

        /// <summary>
        /// Now cultere is empty -> default should be CultureInfo.InvariantCulture
        /// </summary>
        [Test]
        public void DecimalTestDefault()
        {
            TestTools.RemoveOutputFile(_outputFileName);
            RunDecimalTestNormal("", _inputJsonFileNameDecDot);
            //var hash = TestTools.MD5Hash(_outputFileName);

            Assert.AreEqual(12345.6789m, Tests.ReturnParquetValue(_outputFileName, 1, 0));
            Assert.AreEqual(12345.6789f, Tests.ReturnParquetValue(_outputFileName, 2, 0));
            Assert.AreEqual(12345.6789d, Tests.ReturnParquetValue(_outputFileName, 3, 0));
        }


        /// <summary>
        /// Simple csv -> parquet test case with large group size
        /// </summary>
        [Test]
        public void ArrayTestLarge()
        {
            TestTools.RemoveOutputFile(_outputFileName);
            int rows = 1000;
            string schema = CreateLargeArrayJsonFile(_inputJsonFileNameArrayLarge, rows);

            var options = new WriteJSONOptions()
            {
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptionsJSON()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip
            };

            var input = new WriteInput()
            {
                FileName = _inputJsonFileNameArrayLarge,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = schema
            };

            ParquetTasks.ConvertJsonToParquet(input, options, poptions, new System.Threading.CancellationToken());

            Assert.AreEqual("998|999|1000", Tests.ReturnParquetValue(_outputFileName, 1, 998));
            Assert.AreEqual("Eka|Toka|Kolmas", Tests.ReturnParquetValue(_outputFileName, 2, 998));

            Assert.AreEqual("0", Tests.ReturnParquetValue(_outputFileName, 1, rows));
            Assert.AreEqual("Vika", Tests.ReturnParquetValue(_outputFileName, 2, rows));


            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.AreEqual("3bbb66b604eed92e84acc24e58ed7c57", hash, "File checksum didn't match.");
        }

        /// <summary>
        /// Test case for counting rows before processing
        /// </summary>
        [Test]
        public void WriteParquetFileCountRows()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteJSONOptions()
            {
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptionsJSON()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip,
            };

            var input = new WriteInput()
            {
                FileName = _inputJsonFileName,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = _commonSchema
            };

            ParquetTasks.ConvertJsonToParquet(input, options, poptions, new System.Threading.CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.AreEqual("e569d2dac1618d8f44f544a2e8fff956", hash, "File checksum didn't match.");
        }
        /// <summary>
        /// Simple json, no null values
        /// </summary>
        [Test]
        public void WriteParquetFileNoNulls()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteJSONOptions()
            {
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptionsJSON()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip
            };

            var input = new WriteInput()
            {
                FileName = _inputJsonFileNameNoNulls,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = @"[
    { ""name"": ""Id"", ""type"": ""int?""},
    {""name"": ""Time"", ""type"": ""datetime?"", ""format"": ""dd.MM.yyyyTHH:mm:sszzz""},
    {""name"": ""Decimal"", ""type"": ""double?"", ""culture"": ""fi-FI""},
    { ""name"": ""Description"", ""type"": ""string?""},
]".Replace("?\"", "\"")
            };

            ParquetTasks.ConvertJsonToParquet(input, options, poptions, new System.Threading.CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.AreEqual("e5584fe7a1a3ebb7e3a8de165d431e4c", hash, "File checksum didn't match." + hash);
        }

       

        [Test]
        public void ArrayGetValues()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            var options = new WriteJSONOptions()
            {
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptionsJSON()
            {
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Snappy
            };

            var input = new WriteInput()
            {
                FileName = _inputJsonFileNameArray,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = _commonArraySchema
            };

            ParquetTasks.ConvertJsonToParquet(input, options, poptions, new System.Threading.CancellationToken());

            Assert.AreEqual(null, Tests.ReturnParquetValue(_outputFileName, 1, 0));
            Assert.AreEqual("21", Tests.ReturnParquetValue(_outputFileName, 1, 1));
            Assert.AreEqual("31|32", Tests.ReturnParquetValue(_outputFileName, 1, 2));
            Assert.AreEqual("41||43", Tests.ReturnParquetValue(_outputFileName, 1, 3));
            Assert.AreEqual("71|72|73|74|75||77|78|79", Tests.ReturnParquetValue(_outputFileName, 1, 6));

            Assert.AreEqual("", Tests.ReturnParquetValue(_outputFileName, 2, 0));
            Assert.AreEqual("Yksi2", Tests.ReturnParquetValue(_outputFileName, 2, 1));
            Assert.AreEqual("Yksi3|Kaksi3", Tests.ReturnParquetValue(_outputFileName, 2, 2));
            Assert.AreEqual("Yksi7|||Kolme7", Tests.ReturnParquetValue(_outputFileName, 2, 6));

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.AreEqual("92bcd1ec5c22d943b30a383010020da5", hash, "File checksum didn't match.");
        }

        /// <summary>
        /// Simple json -> parquet test case with large group size
        /// </summary>
        [Test]
        public void WriteParquetFileMaxMemory()
        {
            TestTools.RemoveOutputFile(_outputFileName);

            string schema = CreateLargeJSONFile(_inputJsonFileNameLarge, 1000000);

            var options = new WriteJSONOptions()
            {
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptionsJSON()
            {
                ParquetRowGroupSize = 100000000,
                ParquetCompressionMethod = CompressionType.Gzip
            };

            var input = new WriteInput()
            {
                FileName = _inputJsonFileNameLarge,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = schema
            };

            ParquetTasks.ConvertJsonToParquet(input, options, poptions, new System.Threading.CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.AreEqual("b78399cf76acc6ffe8ac4937e209cffa", hash, "File checksum didn't match.");
        }

        // ----------------------------------------------------------

        private void RunDecimalTestNormal(string cultureStr, string inputFileName)
        {
            var options = new WriteJSONOptions()
            {
                FileEncoding = FileEncoding.UTF8,
                EnableBom = false,
                EncodingInString = ""
            };

            var poptions = new WriteParquetOptionsJSON()
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

            ParquetTasks.ConvertJsonToParquet(input, options, poptions, new System.Threading.CancellationToken());
        }

        /// <summary>
        /// Creates JSON file and returns JSON schema for Parquet
        /// </summary>
        /// <param name="fileName">JSON filename, full path</param>
        /// <param name="rows">Number of rows</param>
        /// <returns>JSON schema of the csv file</returns>
        private string CreateLargeJSONFile(string fileName, int rows)
        {
            using (StreamWriter outputFile = new StreamWriter(fileName))
            {
                var startDate = DateTime.Parse("1995-01-01");

                outputFile.WriteLine("[");
                for (int i = 0; i < rows; i++)
                {
                    double dec1 = (i + 1.0) + (i % 100) / 100.0;
                    double days = i % 10000;

                    outputFile.WriteLine(@"{ ""Id"": " + (i + 1) + @", ""Time"": """ + startDate.AddDays(days).ToString("dd.MM.yyyy") + @""", ""Decimal"": " + dec1.ToString(System.Globalization.CultureInfo.InvariantCulture) + @", ""Description"": ""Testirivi " + (i + 1) + @" ja jotain tekstiä."" }, ");

                }
                outputFile.WriteLine(@" { ""Id"": 0, ""Time"": ""10.10.2021"", ""Decimal"": 1.0, ""Description"": ""Last row"" }");
                outputFile.WriteLine("]");
            }

            return _commonSchema.Replace("int?", "int");
        }

        /// <summary>
        /// Creates JSON file with arrays and returns JSON schema for Parquet
        /// </summary>
        /// <param name="fileName">JSON filename, full path</param>
        /// <param name="rows">Number of rows</param>
        /// <returns>JSON schema of the json file</returns>
        private string CreateLargeArrayJsonFile(string fileName, int rows)
        {
            using (StreamWriter outputFile = new StreamWriter(fileName))
            {
                outputFile.WriteLine("[");

                for (int i = 0; i < rows; i++)
                {
                    int lask = i % 3;
                    int rivi = i + 1;

                    if (lask == 0)
                    {
                        outputFile.WriteLine(@"{ ""Id"": " + rivi + @", ""IntArray"": [ " + (rivi-1) + @"], ""StringArray"": [ ""Eka"" ], ""Description"": ""Rivi " + rivi + @""" },");                       
                    }
                    else if (lask == 1)
                    {
                        outputFile.WriteLine(@"{ ""Id"": " + rivi + @", ""IntArray"": [ " + (rivi - 1) + "," + (rivi) + @"], ""StringArray"": [ ""Eka"",""Toka"" ], ""Description"": ""Rivi " + rivi + @""" },");
                    }
                    else
                    {
                        outputFile.WriteLine(@"{ ""Id"": " + rivi + @", ""IntArray"": [ " + (rivi - 1) + "," + (rivi) +"," + (rivi+1) + @"], ""StringArray"": [ ""Eka"",""Toka"", ""Kolmas"" ], ""Description"": ""Rivi " + rivi + @""" },");

                    }

                    
                }
                outputFile.WriteLine(@"{ ""Id"": 0 ,""IntArray"": [ 0 ], ""StringArray"": [ ""Vika"" ], ""Description"": ""Viimeinen rivi"" }");
                outputFile.WriteLine("]");
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