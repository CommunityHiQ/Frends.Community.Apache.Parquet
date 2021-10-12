using Parquet;
using Parquet.Data;
using System;
using System.Collections.Generic;
using System.Globalization;

#pragma warning disable 1591

namespace Frends.Community.Apache.Parquet
{
    public static class Writer
    {



        /// <summary>
        /// Writes dataLen rows and typed columns to the file.
        /// </summary>
        /// <param name="csvColumns">Processed CSV data</param>
        /// <param name="dataLen">Row count</param>
        /// <param name="writer">ParquetWriter</param>
        /// <param name="fields">Field structure</param>
        /// <param name="config">Config structure</param>
        public static void WriteGroup(List<Object> csvColumns, long dataLen, ParquetWriter writer, List<DataField> fields, Config config)
        {
            string csvSep =  "|";
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
                for (int i = 0; i < fields.Count; i++)
                {
                    if( fields[i].IsArray)
                    {
                        var reps = new List<int>();
                        switch (fields[i].DataType)
                        {
                            case DataType.Boolean:
                                var boolData = new List<bool?>();
                                foreach (string col in (string[])csvColumns[i])
                                {
                                    int count = 0;
                                    foreach (string item in col?.Split(csvSep[0]))
                                    {
                                        boolData.Add(GetBooleanValueNullable(item));
                                        reps.Add(count == 0 ? 0 : 1);
                                        count++;
                                    }

                                    if (count == 0)
                                    {
                                        boolData.Add(null);
                                        reps.Add(0);
                                    }
                                }
                                rg.WriteColumn(new DataColumn(fields[i], boolData.ToArray(), reps.ToArray()));
                                break;
                            case DataType.DateTimeOffset:
                                var dtData = new List<DateTimeOffset?>();
                                foreach (string col in (string[])csvColumns[i])
                                {
                                    int count = 0;
                                    foreach (string item in col?.Split(csvSep[0]))
                                    {
                                        dtData.Add(GetDateTimeOffsetValueNullable(item, config.GetConfigValue(fields[i].Name)));
                                        reps.Add(count == 0 ? 0 : 1);
                                        count++;
                                    }

                                    if (count == 0)
                                    {
                                        dtData.Add(null);
                                        reps.Add(0);
                                    }
                                }
                                rg.WriteColumn(new DataColumn(fields[i], dtData.ToArray(), reps.ToArray()));
                                break;
                            case DataType.Decimal:
                                var decData = new List<Decimal?>();
                                foreach (string col in (string[])csvColumns[i])
                                {
                                    int count = 0;
                                    foreach (string item in col?.Split(csvSep[0]))
                                    {
                                        decData.Add(GetDecimalValueNullable(item, config.GetConfigValue(fields[i].Name)));
                                        reps.Add(count == 0 ? 0 : 1);
                                        count++;
                                    }

                                    if (count == 0)
                                    {
                                        decData.Add(null);
                                        reps.Add(0);
                                    }
                                }
                                rg.WriteColumn(new DataColumn(fields[i], decData.ToArray(), reps.ToArray()));
                                break;
                            case DataType.Double:
                                var doubData = new List<double?>();
                                foreach (string col in (string[])csvColumns[i])
                                {
                                    int count = 0;
                                    foreach (string item in col?.Split(csvSep[0]))
                                    {
                                        doubData.Add(GetDoubleValueNullable(item, config.GetConfigValue(fields[i].Name)));
                                        reps.Add(count == 0 ? 0 : 1);
                                        count++;
                                    }

                                    if (count == 0)
                                    {
                                        doubData.Add(null);
                                        reps.Add(0);
                                    }
                                }
                                rg.WriteColumn(new DataColumn(fields[i], doubData.ToArray(), reps.ToArray()));
                                break;
                            case DataType.Float:
                                var flData = new List<float?>();
                                foreach (string col in (string[])csvColumns[i])
                                {
                                    int count = 0;
                                    foreach (string item in col?.Split(csvSep[0]))
                                    {
                                        flData.Add(GetFloatValue(item, config.GetConfigValue(fields[i].Name)));
                                        reps.Add(count == 0 ? 0 : 1);
                                        count++;
                                    }

                                    if (count == 0)
                                    {
                                        flData.Add(null);
                                        reps.Add(0);
                                    }
                                }
                                rg.WriteColumn(new DataColumn(fields[i], flData.ToArray(), reps.ToArray()));
                                break;
                            case DataType.Int16:
                                var i16Data = new List<Int32?>();
                                foreach (string col in (string[])csvColumns[i])
                                {
                                    int count = 0;
                                    foreach (string item in col?.Split(csvSep[0]))
                                    {
                                        i16Data.Add(GetInt16ValueNullable(item));
                                        reps.Add(count == 0 ? 0 : 1);
                                        count++;
                                    }

                                    if (count == 0)
                                    {
                                        i16Data.Add(null);
                                        reps.Add(0);
                                    }
                                }
                                // reps: 0 means that this is a start of an array and 1 - it's a value continuation.
                                rg.WriteColumn(new DataColumn(fields[i], i16Data.ToArray(), reps.ToArray()));
                                break;
                            case DataType.Int32:
                                var i32Data = new List<Int32?>();
                                foreach (string col in (string[])csvColumns[i])
                                {
                                    int count = 0;
                                    foreach (string item in col?.Split(csvSep[0]))
                                    {
                                        i32Data.Add(GetInt32ValueNullable(item));
                                        reps.Add(count == 0 ? 0 : 1);
                                        count++;
                                    }

                                    if (count == 0)
                                    {
                                        i32Data.Add(null);
                                        reps.Add(0);
                                    }
                                }
                                // reps: 0 means that this is a start of an array and 1 - it's a value continuation.
                                rg.WriteColumn(new DataColumn(fields[i], i32Data.ToArray(), reps.ToArray()));
                                break;
                            case DataType.Int64:
                                var i64Data = new List<Int64?>();
                                foreach (string col in (string[])csvColumns[i])
                                {
                                    int count = 0;
                                    foreach (string item in col?.Split(csvSep[0]))
                                    {
                                        i64Data.Add(GetInt64ValueNullable(item));
                                        reps.Add(count == 0 ? 0 : 1);
                                        count++;
                                    }

                                    if (count == 0)
                                    {
                                        i64Data.Add(null);
                                        reps.Add(0);
                                    }
                                }
                                rg.WriteColumn(new DataColumn(fields[i], i64Data.ToArray(), reps.ToArray()));
                                break;
                            case DataType.String:
                                var strData = new List<string>();
                                foreach (string col in (string[])csvColumns[i])
                                {
                                    int count = 0;

                                    foreach (string item in col?.Split(csvSep[0]))
                                    {
                                        // TODO: implement null if needed
                                        strData.Add(item);
                                        reps.Add(count == 0 ? 0 : 1);
                                        count++;
                                    }
                                    
                                    if (count == 0)
                                    {
                                        strData.Add(null);
                                        reps.Add(0);
                                    }
                                }
                                rg.WriteColumn(new DataColumn(fields[i], strData.ToArray(), reps.ToArray()));
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(fields[i].DataType.ToString());
                        }
                    } else if (fields[i].HasNulls)
                    {
                        switch (fields[i].DataType)
                        {
                            case DataType.Boolean:
                                rg.WriteColumn(new DataColumn(fields[i], ((bool?[])csvColumns[i])));
                                break;
                            case DataType.DateTimeOffset:
                                rg.WriteColumn(new DataColumn(fields[i], ((DateTimeOffset?[])csvColumns[i])));
                                break;
                            case DataType.Decimal:
                                rg.WriteColumn(new DataColumn(fields[i], ((decimal?[])csvColumns[i])));
                                break;
                            case DataType.Double:
                                rg.WriteColumn(new DataColumn(fields[i], ((double?[])csvColumns[i])));
                                break;
                            case DataType.Float:
                                rg.WriteColumn(new DataColumn(fields[i], ((float?[])csvColumns[i])));
                                break;
                            case DataType.Int16:
                                rg.WriteColumn(new DataColumn(fields[i], ((Int16?[])csvColumns[i])));
                                break;
                            case DataType.Int32:
                                rg.WriteColumn(new DataColumn(fields[i], ((Int32?[])csvColumns[i])));
                                break;
                            case DataType.Int64:
                                rg.WriteColumn(new DataColumn(fields[i], ((Int64?[])csvColumns[i])));
                                break;
                            case DataType.String:
                                rg.WriteColumn(new DataColumn(fields[i], ((string[])csvColumns[i])));
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(fields[i].DataType.ToString());
                        }
                    }
                    else
                    {
                        switch (fields[i].DataType)
                        {
                            case DataType.Boolean:
                                rg.WriteColumn(new DataColumn(fields[i], ((bool[])csvColumns[i])));
                                break;
                            case DataType.DateTimeOffset:
                                rg.WriteColumn(new DataColumn(fields[i], ((DateTimeOffset[])csvColumns[i])));
                                break;
                            case DataType.Decimal:
                                rg.WriteColumn(new DataColumn(fields[i], ((decimal[])csvColumns[i])));
                                break;
                            case DataType.Double:
                                rg.WriteColumn(new DataColumn(fields[i], ((double[])csvColumns[i])));
                                break;
                            case DataType.Float:
                                rg.WriteColumn(new DataColumn(fields[i], ((float[])csvColumns[i])));
                                break;
                            case DataType.Int16:
                                rg.WriteColumn(new DataColumn(fields[i], ((Int16[])csvColumns[i])));
                                break;
                            case DataType.Int32:
                                rg.WriteColumn(new DataColumn(fields[i], ((Int32[])csvColumns[i])));
                                break;
                            case DataType.Int64:
                                rg.WriteColumn(new DataColumn(fields[i], ((Int64[])csvColumns[i])));
                                break;
                            case DataType.String:
                                rg.WriteColumn(new DataColumn(fields[i], ((string[])csvColumns[i])));
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(fields[i].DataType.ToString());
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Return CultureInfo for given cultero or fi-FI
        /// </summary>
        /// <param name="culture">Culture name</param>
        /// <returns>CultureInfo</returns>
        public static CultureInfo GetCultureInfo(string culture)
        {
            if (System.String.IsNullOrEmpty(culture))
            {
                //return CultureInfo.InvariantCulture;
                return new CultureInfo("fi-FI");
            }
            else
            {
                return new CultureInfo(culture);
            }
        }

        /// <summary>
        /// Parse Int16 or return null if value is empty
        /// </summary>
        /// <param name="value">Int16 as string</param>
        /// <returns>Int16</returns>
        public static Int16? GetInt16ValueNullable(string value)
        {
            if (!String.IsNullOrWhiteSpace(value))
            {
                return Int16.Parse(value);
            }
            return null;
        }

        /// <summary>
        /// Parse Int32 array or return null if it is empty
        /// </summary>
        /// <param name="value">Int32 array as string</param>
        /// <param name="csvsep">A character that delimist the int strings</param>
        /// <returns>Int32?[] array</returns>
        public static Int32?[] GetInt32ValueArrayNullable(string value, char csvsep = '|')
        {
            var results = new List<Int32?>();

            foreach (var elem in value.Split(csvsep))
            {
                if (!String.IsNullOrWhiteSpace(elem))
                {
                    Int32? val = Int32.Parse(elem);
                    results.Add(val);
                }
                else
                {
                    results.Add(null);
                }
            }
            return results.ToArray();
        }

        /// <summary>
        /// Parse Int32 or return null if value is empty
        /// </summary>
        /// <param name="value">Int32 as string</param>
        /// <returns>Int32</returns>
        public static Int32? GetInt32ValueNullable(string value)
        {
            if (!String.IsNullOrWhiteSpace(value))
            {
                return Int32.Parse(value);
            }
            return null;
        }

        /// <summary>
        /// Parse Int64 or return null if value is empty
        /// </summary>
        /// <param name="value">Int64 as string</param>
        /// <returns>Int64</returns>
        public static Int64? GetInt64ValueNullable(string value)
        {
            if (!String.IsNullOrWhiteSpace(value))
            {
                return Int64.Parse(value);
            }
            return null;
        }

        /// <summary>
        /// Parse float or return null if value is empty
        /// </summary>
        /// <param name="value">float as string</param>
        /// <param name="culture">Optional CultureInfo object</param>
        /// <returns>float</returns>
        public static float? GetFloatValue(string value, string culture)
        {
            if (!String.IsNullOrWhiteSpace(value))
            {
                return float.Parse(value, Writer.GetCultureInfo(culture));
            }
            return null;
        }

        /// <summary>
        /// Parse decimal or return null if value is empty
        /// </summary>
        /// <param name="value">decimal as string</param>
        /// <param name="culture">Optional CultureInfo object</param>
        /// <returns>decimal</returns>
        public static decimal? GetDecimalValueNullable(string value, string culture)
        {
            if (!String.IsNullOrWhiteSpace(value))
            {
                return decimal.Parse(value, Writer.GetCultureInfo(culture));
            }
            return null;
        }

        /// <summary>
        /// Parse double or return null if value is empty
        /// </summary>
        /// <param name="value">double as string</param>
        /// <param name="culture">Optional CultureInfo object</param>
        /// <returns>double</returns>
        public static double? GetDoubleValueNullable(string value, string culture)
        {
            if (!String.IsNullOrWhiteSpace(value))
            {
                return double.Parse(value, Writer.GetCultureInfo(culture));
            }
            return null;
        }

        /// <summary>
        /// Parse boolean value. "true" or "1" is true, otherwise value is false. Empty values are null.
        /// </summary>
        /// <param name="value">boolean as string</param>
        /// <returns>boolean</returns>
        public static bool? GetBooleanValueNullable(string value)
        {
            if (!String.IsNullOrWhiteSpace(value))
            {
                if (value.ToLower() == "true" || value == "1")
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            return null;
        }

        /// <summary>
        /// Returns true when string is true or 1.
        /// Otherwise returns false.
        /// </summary>
        /// <param name="value">boolean as string</param>
        /// <returns>bool</returns>
        public static bool GetBooleanValue(string value)
        {
            if (value.ToLower() == "true" || value == "1")
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Parses datetime
        /// If format is given, tries parse that exact format.
        /// If date is empty, returns null.
        /// </summary>
        /// <param name="dtStr">Date as string</param>
        /// <param name="timeFormat">Optional exact format</param>
        /// <returns>datetimeoffset object</returns>
        public static DateTimeOffset? GetDateTimeOffsetValueNullable(string dtStr, string timeFormat)
        {
            if (!String.IsNullOrWhiteSpace(dtStr))
            {
                if (!String.IsNullOrWhiteSpace(timeFormat))
                {
                    if (DateTimeOffset.TryParseExact(dtStr, timeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTimeOffset dt))
                    {
                        return dt;
                    }
                }
                else
                {
                    return DateTimeOffset.Parse(dtStr);
                }

            }
            return null;
        }

        /// <summary>
        /// Parses datetime
        /// If format is given, tries parse that exact format.
        /// If exact format parsing fails, throws exception.
        /// </summary>
        /// <param name="dtStr">Date as string</param>
        /// <param name="timeFormat">Optional exact format</param>
        /// <returns>datetimeoffset object</returns>
        public static DateTimeOffset GetDateTimeOffsetValue(string dtStr, string timeFormat)
        {
            if (!String.IsNullOrWhiteSpace(timeFormat))
            {
                if (DateTimeOffset.TryParseExact(dtStr, timeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTimeOffset dt))
                {
                    return dt;
                }
                else
                {
                    throw new ArgumentException($"DateTimeOffset.TryParseExact failed. Value: {dtStr}, format: {timeFormat}");
                }
            }
            else
            {
                return DateTimeOffset.Parse(dtStr);
            }

        }
    }
}
