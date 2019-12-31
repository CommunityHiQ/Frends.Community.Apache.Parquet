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
            using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
            {
                for (int i = 0; i < fields.Count; i++)
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
            }
        }

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
    }
}
