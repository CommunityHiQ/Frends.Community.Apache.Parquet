using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

#pragma warning disable 1591

namespace Frends.Community.Apache.Parquet
{
    public static class Writer
    {
        /// <summary>
        /// Writes dataLen rows and typed columns to the file.
        /// </summary>
        /// <param name="csvArr">CSV datas as jagged string array</param>
        /// <param name="dataLen">Row count</param>
        /// <param name="writer">ParquetWriter</param>
        /// <param name="fields">Field structure</param>
        /// <param name="config">Config structure</param>
        public static void WriteGroup(string[][] csvArr, long dataLen, ParquetWriter writer, List<DataField> fields, Config config)
        {
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
                for (int i = 0; i < fields.Count; i++)
                {
                    switch (fields[i].DataType)
                    {
                        case DataType.Boolean:
                            rg.WriteColumn(new DataColumn(fields[i], GetBooleanArray(csvArr[i].Take((int)dataLen))));
                            break;
                        case DataType.DateTimeOffset:
                            rg.WriteColumn(new DataColumn(fields[i], GetDateTimeArray(csvArr[i].Take((int)dataLen).ToArray(), config.GetConfigValue(fields[i].Name))));
                            break;
                        case DataType.Decimal:
                            rg.WriteColumn(new DataColumn(fields[i], GetDecimalArray(csvArr[i].Take((int)dataLen), config.GetConfigValue(fields[i].Name))));
                            break;
                        case DataType.Double:
                            rg.WriteColumn(new DataColumn(fields[i], GetDoubleArray(csvArr[i].Take((int)dataLen), config.GetConfigValue(fields[i].Name))));
                            break;
                        case DataType.Float:
                            rg.WriteColumn(new DataColumn(fields[i], GetFloatArray(csvArr[i].Take((int)dataLen), config.GetConfigValue(fields[i].Name))));
                            break;
                        case DataType.Int16:
                            rg.WriteColumn(new DataColumn(fields[i], GetInt16Array(csvArr[i].Take((int)dataLen))));
                            break;
                        case DataType.Int32:
                            rg.WriteColumn(new DataColumn(fields[i], GetInt32Array(csvArr[i].Take((int)dataLen))));
                            break;
                        case DataType.Int64:
                            rg.WriteColumn(new DataColumn(fields[i], GetInt64Array(csvArr[i].Take((int)dataLen))));
                            break;
                        case DataType.String:
                            rg.WriteColumn(new DataColumn(fields[i], csvArr[i].Take((int)dataLen).ToArray()));
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(fields[i].DataType.ToString());
                    }
                }
            }
        }

        // All types have own methods. It would be possible to use generics but...

        /// <summary>
        /// Gets boolean array
        /// </summary>
        /// <param name="strList">Array of boolean strings</param>
        /// <returns>bool array</returns>
        private static bool?[] GetBooleanArray(IEnumerable<string> strList)
        {
            // Create specific type
            bool?[] boolArr = new bool?[strList.Count()];
            
            int index = 0;

            // Loop all values
            foreach (var boolStr in strList)
            {
                // Convert value from string to specific type. Allow nulls
                bool? bl = null;
                
                if (!String.IsNullOrWhiteSpace(boolStr))
                {
                    if (boolStr.ToLower() == "true" || boolStr == "1")
                    {
                        bl = true;
                    }
                    else
                    {
                        bl = false;
                    }
                }
                // Add specific type to array and handle next
                boolArr[index] = bl;
                index++;
            }
            return boolArr;
        }

        private static float?[] GetFloatArray(IEnumerable<string> strList, string culture)
        {
            float?[] ftlArr = new float?[strList.Count()];

            int index = 0;
            foreach (var ftlStr in strList)
            {
                float? ftl = null;

                if (!String.IsNullOrWhiteSpace(ftlStr))
                {
                    // Decimal dot is .
                    ftl = float.Parse(ftlStr, GetCultureInfo(culture));
                }
                ftlArr[index] = ftl;
                index++;
            }
            return ftlArr;
        }

        private static DateTimeOffset?[] GetDateTimeArray(IEnumerable<string> dtList, string timeFormat)
        {
            DateTimeOffset?[] dtArr = new DateTimeOffset?[dtList.Count()];

            int index = 0;
            foreach (var dtStr in dtList)
            {
                DateTimeOffset? dt = null;
                if (!String.IsNullOrWhiteSpace(dtStr))
                {
                    if (!String.IsNullOrWhiteSpace(timeFormat))
                    {
                        if (DateTimeOffset.TryParseExact(dtStr, timeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTimeOffset dtt))
                        {
                            dt = dtt;
                        }
                    }
                    else
                    {
                        dt = DateTimeOffset.Parse(dtStr);
                    }
                }
                dtArr[index] = dt;
                index++;
            }
            return dtArr;
        }

        private static decimal?[] GetDecimalArray(IEnumerable<string> strList, string culture)
        {
            decimal?[] decArr = new decimal?[strList.Count()];

            int index = 0;
            foreach (var decStr in strList)
            {
                decimal? dec = null;
                // Decimal dot is .
                if (!String.IsNullOrWhiteSpace(decStr))
                {
                    dec = decimal.Parse(decStr, GetCultureInfo(culture));
                }
                decArr[index] = dec;
                index++;
            }
            return decArr;
        }

        private static double?[] GetDoubleArray(IEnumerable<string> strList, string culture)
        {
            double?[] dblArr = new double?[strList.Count()];

            

            int index = 0;
            foreach (var dblStr in strList)
            {
                double? dbl = null;
                // Decimal dot is .
                if (!String.IsNullOrWhiteSpace(dblStr))
                {
                    dbl = double.Parse(dblStr, GetCultureInfo(culture));
                }
                dblArr[index] = dbl;
                index++;
            }
            return dblArr;
        }

        private static Int16?[] GetInt16Array(IEnumerable<string> strList)
        {
            Int16?[] intArr = new Int16?[strList.Count()];

            int index = 0;
            foreach (var intStr in strList)
            {
                Int16? val = null;
                if (!String.IsNullOrWhiteSpace(intStr))
                {
                    val = Int16.Parse(intStr);
                }
                intArr[index] = val;
                index++;
            }
            return intArr;
        }

        private static Int32?[] GetInt32Array(IEnumerable<string> strList)
        {
            // Generics are a bit complex
            Int32?[] intArr = new Int32?[strList.Count()];

            int index = 0;
            foreach (var intStr in strList)
            {
                Int32? val = null;
                if (!String.IsNullOrWhiteSpace(intStr))
                {
                    val = Int32.Parse(intStr);
                }
                intArr[index] = val;
                index++;
            }
            return intArr;
        }

        private static Int64?[] GetInt64Array(IEnumerable<string> strList)
        {
            // Generics are a bit complex
            Int64?[] intArr = new Int64?[strList.Count()];

            int index = 0;
            foreach (var intStr in strList)
            {
                Int64? val = null;
                if (!String.IsNullOrWhiteSpace(intStr))
                {
                    val = Int64.Parse(intStr);
                }
                intArr[index] = val;
                index++;
            }
            return intArr;
        }


        private static CultureInfo GetCultureInfo(string culture)
        {
            if( System.String.IsNullOrEmpty(culture))
            {
                //return CultureInfo.InvariantCulture;
                return new CultureInfo("fi-FI");
            } 
            else
            {
                return new CultureInfo(culture);
            }
        }
    }
}
