using System;
using System.Collections;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
#if !NETCOREAPP11
using System.Data;
using System.Data.Common;
#endif
using ClickHouse.Ado.Impl;

namespace ClickHouse.Ado
{
    public class ClickHouseParameter
#if !NETCOREAPP11
        : DbParameter
#endif
    {

        ClickHouseParameterCollection _collection;
        string _name = String.Empty;
        public override DbType DbType { get; set; }

        public override ParameterDirection Direction { get; set; }
#if !NETCOREAPP11

        public override bool IsNullable { get; set; }

        /// <summary>
        /// Gets or sets The name of the <see cref="NpgsqlParameter">NpgsqlParameter</see>.
        /// </summary>
        /// <value>The name of the <see cref="NpgsqlParameter">NpgsqlParameter</see>.
        /// The default is an empty string.</value>
        [DefaultValue("")]
        public override string ParameterName
        {
            get { return _name; }
            set
            {
                _name = value;
                if (value == null)
                {
                    _name = String.Empty;
                }
                // no longer prefix with : so that The name returned is The name set

                _name = _name.Trim();

                if (_collection != null)
                {
                    _collection.InvalidateHashLookups();
                }
            }
        }


        public override string SourceColumn { get; set; }

        public override bool SourceColumnNullMapping { get; set; }

        public override void ResetDbType()
        {
            throw new NotImplementedException();
        }

#endif
        public override int Size { get; set; }

        public override object Value { get; set; }

        /// <summary>
        /// The name scrubbed of any optional marker
        /// </summary>
        internal string CleanName
        {
            get
            {
                string name = ParameterName;
                if (name[0] == ':' || name[0] == '@')
                {
                    return name.Length > 1 ? name.Substring(1) : string.Empty;
                }
                return name;

            }
        }
        /// <summary>
        /// The collection to which this parameter belongs, if any.
        /// </summary>
        public ClickHouseParameterCollection Collection
        {
            get { return _collection; }

            internal set
            {
                _collection = value;
            }
        }


        private string AsSubstitute(object val)
        {
            if (DbType == DbType.String
#if !NETCOREAPP11
                || DbType == DbType.AnsiString || DbType == DbType.StringFixedLength || DbType == DbType.AnsiStringFixedLength
#endif
                ||(DbType==0 && val is string)
            )
                if (!(val is string) && val is IEnumerable)
                    return string.Join(",", ((IEnumerable) val).Cast<object>().Select(AsSubstitute));
                else
                    return ProtocolFormatter.EscapeStringValue(val.ToString());
            if (DbType == DbType.DateTime
#if !NETCOREAPP11
                || DbType == DbType.DateTime2 || DbType == DbType.DateTime2
#endif
                || (DbType==0 && val is DateTime)
            )
                return $"'{(DateTime)val:yyyy-MM-dd HH:mm:ss}'";
            if (DbType == DbType.Date)
                return $"'{(DateTime)val:yyyy-MM-dd}'";
            if ((DbType != 0
#if !NETCOREAPP11
                 && DbType != DbType.Object
#endif
                ) && !(val is string) && val is IEnumerable)
            {
                return string.Join(",", ((IEnumerable)val).Cast<object>().Select(AsSubstitute));
            }
            if ((DbType==0
#if !NETCOREAPP11
                || DbType==DbType.Object
#endif
                ) && !(val is string) && val is IEnumerable )
            {
                return "[" + string.Join(",", ((IEnumerable) val).Cast<object>().Select(AsSubstitute)) + "]";
            }

            if (val is IFormattable formattable)
                return formattable.ToString(null, CultureInfo.InvariantCulture);
            return val.ToString();
        }
        public string AsSubstitute()
        {
            return AsSubstitute(Value);
        }

        public override string ToString()
        {
            return $"{ParameterName}({DbType}): {Value}";
        }
        public ClickHouseParameter(string parameterName,object value)
        {
            ParameterName = parameterName;
            Value = value;
        }
        public ClickHouseParameter()
        {
            SourceColumn = String.Empty;
            Direction = ParameterDirection.Input;
        }
    }
}