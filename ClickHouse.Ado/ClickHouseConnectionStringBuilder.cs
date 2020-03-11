using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;

namespace ClickHouse.Ado
{
    public class ClickHouseConnectionStringBuilder : DbConnectionStringBuilder
    {
        internal static readonly string[] EmptyStringArray;

        private static readonly Dictionary<string, PropertyInfo> PropertiesByKeyword;

        /// <summary>
        /// Maps CLR property names (e.g. BufferSize) to their canonical keyword name, which is the
        /// property's [DisplayName] (e.g. Buffer Size)
        /// </summary>
        private static readonly Dictionary<string, string> PropertyNameToCanonicalKeyword;

        /// <summary>
        /// Maps each property to its [DefaultValue]
        /// </summary>
        private static readonly Dictionary<PropertyInfo, object> PropertyDefaults;

        static ClickHouseConnectionStringBuilder()
        {
            EmptyStringArray = new string[0];
#if NETSTANDARD15
            var source = (from p in typeof(ClickHouseConnectionStringBuilder).GetTypeInfo().GetProperties()
                          where p.GetCustomAttribute<ClickHouseConnectionStringPropertyAttribute>() != null
                          select p).ToArray();
            PropertyDefaults = source.Where((PropertyInfo p) => p.GetCustomAttribute<ObsoleteAttribute>() == null)
                .ToDictionary(
                    (PropertyInfo p) => p,
                    (PropertyInfo p) => p.GetCustomAttribute<DefaultValueAttribute>() == null
                                            ? !p.PropertyType.GetTypeInfo().IsValueType ? null :
                                              Activator.CreateInstance(p.PropertyType)
                                            : p.GetCustomAttribute<DefaultValueAttribute>().Value);

#else
            var source = (from p in typeof(ClickHouseConnectionStringBuilder).GetProperties()
                          where p.GetCustomAttribute<ClickHouseConnectionStringPropertyAttribute>() != null
                          select p).ToArray();
            PropertyDefaults = source.Where((PropertyInfo p) => p.GetCustomAttribute<ObsoleteAttribute>() == null)
                .ToDictionary(
                    (PropertyInfo p) => p,
                    (PropertyInfo p) => p.GetCustomAttribute<DefaultValueAttribute>() == null
                                            ? !p.PropertyType.IsValueType ? null :
                                              Activator.CreateInstance(p.PropertyType)
                                            : p.GetCustomAttribute<DefaultValueAttribute>().Value);

#endif
            PropertiesByKeyword = (from p in source
                                   let displayName =
                                       p.GetCustomAttribute<DisplayNameAttribute>().DisplayName.ToUpperInvariant()
                                   let propertyName = p.Name.ToUpperInvariant()
                                   from k in from k in new string[1] { displayName }.Concat(
                                                 (IEnumerable<string>)(!(propertyName != displayName)
                                                                           ? (object)EmptyStringArray
                                                                           : (object)new string[1] { propertyName }))
                                             select new { Property = p, Keyword = k }
                                   select k).ToDictionary(t => t.Keyword, t => t.Property);
            PropertyNameToCanonicalKeyword = source.ToDictionary(
                p => p.Name,
                (PropertyInfo p) => p.GetCustomAttribute<DisplayNameAttribute>().DisplayName);
        }
        private void SetValue(string name, string value)
        {
#if FRAMEWORK20 || FRAMEWORK40
            PropertiesByKeyword[name].GetSetMethod()
#else
            PropertiesByKeyword[name].SetMethod
#endif
            .Invoke(this, new[] { Convert.ChangeType(value, PropertiesByKeyword[name].PropertyType) });
        }

        public ClickHouseConnectionStringBuilder()
        { }

        public ClickHouseConnectionStringBuilder(string connectionString)
        {
            StringBuilder varName = new StringBuilder();
            StringBuilder varValue = new StringBuilder();

            char? valueEscape = null;
            bool inEscape = false;
            bool inValue = false;
            foreach (char c in connectionString)
            {
                if (inEscape)
                {
                    if (inValue) varValue.Append(c);
                    else varName.Append(c);
                    inEscape = false;
                }
                else if (valueEscape.HasValue)
                {
                    if (valueEscape.Value == c)
                        valueEscape = null;
                    else
                    {
                        if (inValue) varValue.Append(c);
                        else varName.Append(c);
                    }
                }
                else if (c == '\\')
                    inEscape = true;
                else if (c == '"' || c == '\'')
                    valueEscape = c;
                else if (char.IsWhiteSpace(c))
                    continue;
                else if (c == '=')
                {
                    if (inValue) throw new FormatException($"Value for parameter {varName} in the connection string contains unescaped '='.");
                    inValue = true;
                }
                else if (c == ';')
                {
                    if (!inValue) throw new FormatException($"No value for parameter {varName} in the connection string.");
                    SetValue(varName.ToString(), varValue.ToString());
                    inValue = false;
                    varName.Clear();
                    varValue.Clear();
                }
                else
                {
                    if (inValue) varValue.Append(c);
                    else varName.Append(c);
                }
            }
            if (inValue) SetValue(varName.ToString(), varValue.ToString());
        }

        [ClickHouseConnectionStringProperty]
        public bool Async { get; set; }
        [ClickHouseConnectionStringProperty] 
        public int BufferSize { get; set; } = 4096;
        [ClickHouseConnectionStringProperty] 
        public int ApacheBufferSize { get; set; }
        [ClickHouseConnectionStringProperty] 
        public int SocketTimeout { get; set; } = 1000;
        [ClickHouseConnectionStringProperty] 
        public int ConnectionTimeout { get; set; } = 1000;
        [ClickHouseConnectionStringProperty] 
        public int DataTransferTimeout { get; set; } = 1000;
        [ClickHouseConnectionStringProperty] 
        public int KeepAliveTimeout { get; set; } = 1000;
        [ClickHouseConnectionStringProperty] 
        public int TimeToLiveMillis { get; set; }
        [ClickHouseConnectionStringProperty]
        public int DefaultMaxPerRoute { get; set; }
        [ClickHouseConnectionStringProperty] 
        public int MaxTotal { get; set; }
        [ClickHouseConnectionStringProperty] 
        public string Host { get; set; }
        [ClickHouseConnectionStringProperty] 
        public int Port { get; set; }

        //additional
        [ClickHouseConnectionStringProperty] 
        public int MaxCompressBufferSize { get; set; }


        // queries settings
        [ClickHouseConnectionStringProperty] 
        public int MaxParallelReplicas { get; set; }
        [ClickHouseConnectionStringProperty] 
        public string TotalsMode { get; set; }
        [ClickHouseConnectionStringProperty] 
        public string QuotaKey { get; set; }
        [ClickHouseConnectionStringProperty] 
        public int Priority { get; set; }
        [ClickHouseConnectionStringProperty] 
        public string Database { get; set; }
        [ClickHouseConnectionStringProperty] 
        public bool Compress { get; set; }
        [ClickHouseConnectionStringProperty] 
        public string Compressor { get; set; }
        [ClickHouseConnectionStringProperty] 
        public bool CheckCompressedHash { get; set; } = true;
        [ClickHouseConnectionStringProperty] 
        public bool Decompress { get; set; }
        [ClickHouseConnectionStringProperty] 
        public bool Extremes { get; set; }
        [ClickHouseConnectionStringProperty] 
        public int MaxThreads { get; set; }
        [ClickHouseConnectionStringProperty] 
        public int MaxExecutionTime { get; set; }
        [ClickHouseConnectionStringProperty] 
        public int MaxBlockSize { get; set; }
        [ClickHouseConnectionStringProperty] 
        public int MaxRowsToGroupBy { get; set; }
        [ClickHouseConnectionStringProperty] 
        public string Profile { get; set; }
        [ClickHouseConnectionStringProperty] 
        public string User { get; set; }
        [ClickHouseConnectionStringProperty] 
        public string Password { get; set; }
        [ClickHouseConnectionStringProperty] 
        public bool DistributedAggregationMemoryEfficient { get; set; }
        [ClickHouseConnectionStringProperty] 
        public int MaxBytesBeforeExternalGroupBy { get; set; }
        [ClickHouseConnectionStringProperty] 
        public int MaxBytesBeforeExternalSort { get; set; }
        public override string ToString()
        {
            var builder = new StringBuilder();
            foreach (var prop in PropertiesByKeyword)
            {
                var value = prop.Value.GetValue(this, null);
                if (value == null)
                {
                    continue;
                }

                builder.Append(prop.Key);
                builder.Append("=\"");
                builder.Append(value.ToString().Replace("\\", "\\\\").Replace("\"", "\\\""));
                builder.Append("\";");
            }
            return builder.ToString();
        }
    }
}