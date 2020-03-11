using System;
using System.Data.Common;
#if !NETCOREAPP11
using System.Data;
#endif
using ClickHouse.Ado.Impl;
using ClickHouse.Ado.Impl.ColumnTypes;
using ClickHouse.Ado.Impl.Data;
using System.Collections;

namespace ClickHouse.Ado
{
    public class ClickHouseDataReader :
#if NETCOREAPP11
        IDisposable
#else
        DbDataReader
#endif
    {

#if !NETCOREAPP11
        private readonly CommandBehavior _behavior;
#endif
        private ClickHouseConnection _clickHouseConnection;

        private Block _currentBlock;
        private int _currentRow;

        internal ClickHouseDataReader(ClickHouseConnection clickHouseConnection
#if !NETCOREAPP11
            , CommandBehavior behavior
#endif
            )
        {
            _clickHouseConnection = clickHouseConnection;
#if !NETCOREAPP11
            _behavior = behavior;
#endif
            NextResult();
        }

        public override string GetName(int i)
        {
            return _currentBlock.Columns[i].Name;
        }

        public override string GetDataTypeName(int i)
        {
            if (_currentBlock == null)
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            return _currentBlock.Columns[i].Type.AsClickHouseType();
        }

        public override Type GetFieldType(int i)
        {
            if (_currentBlock == null)
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            return _currentBlock.Columns[i].Type.CLRType;
        }

        public override object GetValue(int i)
        {
            if (_currentBlock == null || _currentBlock.Rows <= _currentRow || i < 0 || i >= FieldCount)
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            return _currentBlock.Columns[i].Type.Value(_currentRow);
        }

        public override int GetValues(object[] values)
        {
            if (_currentBlock == null || _currentBlock.Rows <= _currentRow)
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            var n = Math.Max(values.Length, _currentBlock.Columns.Count);
            for (var i = 0; i < n; i++)
                values[i] = _currentBlock.Columns[i].Type.Value(_currentRow);
            return n;
        }

        public override int GetOrdinal(string name)
        {
            if (_currentBlock == null || _currentBlock.Rows <= _currentRow)
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            return _currentBlock.Columns.FindIndex(x => x.Name == name);
        }

        public override bool GetBoolean(int i)
        {
            return GetInt64(i) != 0;
        }

        public override byte GetByte(int i)
        {
            return (byte) GetInt64(i);
        }

        public override long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException();
        }

        public override char GetChar(int i)
        {
            return (char)GetInt64(i);
        }

        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException();
        }

        public override Guid GetGuid(int i)
        {
            throw new NotSupportedException();
        }

        public override short GetInt16(int i)
        {
            return (short)GetInt64(i);
        }

        public override int GetInt32(int i)
        {
            return (int)GetInt64(i);
        }

        public override long GetInt64(int i)
        {
            if (_currentBlock == null || _currentBlock.Rows <= _currentRow || i < 0 || i >= FieldCount)
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            return _currentBlock.Columns[i].Type.IntValue(_currentRow);
        }

        public override float GetFloat(int i)
        {
            return Convert.ToSingle(GetValue(i));
        }

        public override double GetDouble(int i)
        {
            return Convert.ToDouble(GetValue(i));
        }

        public override string GetString(int i)
        {
            return GetValue(i).ToString();
        }

        public override decimal GetDecimal(int i)
        {
            return Convert.ToDecimal(GetValue(i));
        }

        public override DateTime GetDateTime(int i)
        {
            return Convert.ToDateTime(GetValue(i));
        }
#if !NETCOREAPP11

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => GetValue(GetOrdinal(name));
#endif

        public override bool IsDBNull(int i)
        {
            if (_currentBlock == null)
                throw new InvalidOperationException("Trying to read beyond end of stream.");

            var type = _currentBlock.Columns[i].Type as NullableColumnType;
            if (type != null)
                return type.IsNull(_currentRow);
            return false;
        }

        public override int FieldCount => _currentBlock.Columns.Count;


        public void Close()
        {
            if (_currentBlock != null)
                _clickHouseConnection.Formatter.ReadResponse();
#if !NETCOREAPP11
            if((_behavior&CommandBehavior.CloseConnection)!=0)
                _clickHouseConnection.Close();
#endif
            _clickHouseConnection = null;
        }
#if !NETCOREAPP11
        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }
#endif

        public override bool NextResult()
        {
            _currentRow = -1;
            return (_currentBlock = _clickHouseConnection.Formatter.ReadBlock()) != null;
        }

        public override bool Read()
        {
            if(_currentBlock==null)
                throw new InvalidOperationException("Trying to read beyond end of stream.");
            _currentRow++;
            if (_currentBlock.Rows <= _currentRow)
                return false;
            return true;
        }

        public override IEnumerator GetEnumerator()
        {
            return new DbEnumerator(this,false);
        }

        public override int Depth { get; }=1;
        public override bool IsClosed => _clickHouseConnection == null;
        public override int RecordsAffected => _currentBlock.Rows;

        public override bool HasRows => _currentBlock!=null;
    }
}