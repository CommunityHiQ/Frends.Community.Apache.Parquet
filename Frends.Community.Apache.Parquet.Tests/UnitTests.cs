using NUnit.Framework;
using System.IO;

using Par = Parquet;

namespace Frends.Community.Apache.Parquet.Tests
{
    public class Tests
    {
        // File paths
        private static readonly string _basePath = Path.GetTempPath();
        private readonly string _inputCsvFileName = Path.Combine(_basePath, "testi-csv-" + Path.GetRandomFileName());
        private readonly string _inputCsvFileName2 = Path.Combine(_basePath, "testi-csv-" + Path.GetRandomFileName());
        private readonly string _outputFileName = Path.Combine(_basePath, "testi-parquet-" + Path.GetRandomFileName());

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
            File.WriteAllText(_inputCsvFileName2, @"Id;Date;Decimal;Text
1;01.10.2019;5.0;Testirivi 1
1;15.04.2018;3.5;Testirivi 2 - ""pidempi teksti"" ja ääkkösiä
;;;Tyhjä rivi 1
4;1.1.2020;9.999;
3;11.11.2011;1.2345;Viimeinen rivi
");

        }

        [TearDown]
        public void TearDown()
        {
            // remove test files
            if (File.Exists(_inputCsvFileName))
            {
                File.Delete(_inputCsvFileName);
            }

            if (File.Exists(_inputCsvFileName2))
            {
                File.Delete(_inputCsvFileName2);
            }

            if (File.Exists(_outputFileName))
            {
                File.Delete(_outputFileName);
            }
        }

        /// <summary>
        /// Simple csv -> parquet test case.
        /// </summary>
        [Test]
        public void WriteParquetFile()
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
                ParquetRowGroupSize = 5000,
                ParquetCompressionMethod = CompressionType.Gzip
            };

            var input = new WriteInput() 
            {
                CsvFileName = _inputCsvFileName, OuputFileName = _outputFileName, 
                ThrowExceptionOnErrorResponse = true, 
                Schema = @"[
    { ""name"": ""Id"", ""type"": ""int?""},
    {""name"": ""Time"", ""type"": ""datetime?"", ""format"": ""dd.MM.yyyy""},
    {""name"": ""Decimal"", ""type"": ""decimal?""},
    { ""name"": ""Description"", ""type"": ""string?""},
]" 
            };

            ParquetTasks.ConvertCsvToParquet(input, options, poptions, new System.Threading.CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.IsTrue(hash == "2b2ea410911658f3fbfa36ec7938d27e", "File checksum didn't match.");
        }

        /// <summary>
        /// Quotes test
        /// </summary>
        [Test]
        public void WriteParquetFileQuotes()
        {
            if (File.Exists(_outputFileName))
            {
                File.Delete(_outputFileName);
            }

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
                CsvFileName = _inputCsvFileName2,
                OuputFileName = _outputFileName,
                ThrowExceptionOnErrorResponse = true,
                Schema = @"[
    { ""name"": ""Id"", ""type"": ""int?""},
    {""name"": ""Time"", ""type"": ""datetime?"", ""format"": ""dd.MM.yyyy""},
    {""name"": ""Decimal"", ""type"": ""decimal?""},
    { ""name"": ""Description"", ""type"": ""string?""},
]"
            };

            ParquetTasks.ConvertCsvToParquet(input, options, poptions, new System.Threading.CancellationToken());

            var hash = TestTools.MD5Hash(_outputFileName);
            Assert.IsTrue(hash == "c2d14b21c69fec284e20801cf43041f6", "File checksum didn't match.");
            
        }
    }
}