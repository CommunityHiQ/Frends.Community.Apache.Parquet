﻿using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;

#pragma warning disable 1591

namespace Frends.Community.Apache.Parquet
{
    public class Config
    {
        private readonly Dictionary<string, string> _config;

        /// <summary>
        /// Creates config data structure for later use
        /// </summary>
        /// <param name="json">JSON configuration</param>
        /// <param name="globalCulture">Default culture if set (for json)</param>
        /// <returns>Config dictionary</returns>
        public Config(JToken json, string globalCulture)
        {
            _config = new Dictionary<string, string>();

            // A simple key-value store
            foreach (var element in json)
            {
                string name = element.Value<string>("name");
                string format = element.Value<string>("format");
                string culture = element.Value<string>("culture");

                if (String.IsNullOrWhiteSpace(format))
                {
                    format = "";
                }

                // Culture overwrites format. (decimals, floats and doubles)
                if (!String.IsNullOrWhiteSpace(culture))
                {
                    // the local parameter overwrites the global parameter
                    if (!String.IsNullOrWhiteSpace(culture))
                    {
                        format = culture;
                    }
                    else
                    { 
                        format = globalCulture;
                    }
                }

                _config[name] = format;
            }
        }

        /// <summary>
        /// Gets value from config using key - for future use
        /// </summary>
        /// <param name="key">Key</param>
        /// <returns>Value</returns>
        public string GetConfigValue(string key)
        {
            return _config[key];
        }

        /// <summary>
        /// Returns value from config using key. If key is empty, returns default value
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="defaultValue">default value</param>
        /// <returns>value</returns>
        public string GetConfigValue(string key, string defaultValue)
        {
            if (String.IsNullOrEmpty(_config[key]))
            {
                return defaultValue;
            } 
            else
            {
                return _config[key];
            }
        }
    }
}
