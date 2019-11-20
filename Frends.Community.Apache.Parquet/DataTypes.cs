using Parquet.Data;
using System;

#pragma warning disable 1591

namespace Frends.Community.Apache.Parquet
{
    public static class DataTypes
    {
        /// <summary>
        /// Return Parquet datatypes
        /// </summary>
        /// <param name="typeStr">Name of the data type</param>
        /// <returns>Parquet.Data.DataType</returns>
        public static DataType GetDataType(string typeStr)
        {
            string str = typeStr.TrimEnd(new char[] { '?' });

            switch (str.ToLower())
            {
                case "boolean":
                    return DataType.Boolean;
                case "datetime":
                case "datetimeoffset":
                    return DataType.DateTimeOffset;
                case "decimal":
                    return DataType.Decimal;
                case "double":
                    return DataType.Double;
                case "float":
                    return DataType.Float;
                case "int16":
                    return DataType.Int16;
                case "int":
                case "int32":
                    return DataType.Int32;
                case "int64":
                    return DataType.Int64;
                //case "int96":
                // this is for datetimes. Use datetimeoffset
                case "string":
                    return DataType.String;
                case "unspecified":
                    return DataType.Unspecified;
                default:
                    throw new ArgumentOutOfRangeException(str);
            }
        }
    }
}
