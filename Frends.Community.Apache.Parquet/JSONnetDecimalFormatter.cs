using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace Frends.Community.Apache.Parquet
{

    /// <summary>
    /// Converts decimals using given culture
    /// </summary>
    public class FormattedDecimalConverter : JsonConverter
    {
        private readonly CultureInfo _culture;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="culture"></param>
        public FormattedDecimalConverter(CultureInfo culture)
        {
            this._culture = culture;
        }

        /// <summary>
        /// Returns true when type is decimal, double or float
        /// </summary>
        /// <param name="objectType">type</param>
        /// <returns>true/false</returns>
        public override bool CanConvert(Type objectType)
        {
            return (  objectType == typeof(decimal) ||
                      objectType == typeof(double)  ||
                      objectType == typeof(float)
                    );
        }

        /// <summary>
        /// Writes decimal using cultureinfo
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="value"></param>
        /// <param name="serializer"></param>
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteValue(Convert.ToString(value, _culture));
        }

        /// <summary>
        /// Not implemented
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="objectType"></param>
        /// <param name="existingValue"></param>
        /// <param name="serializer"></param>
        /// <returns></returns>
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }
    }
}
