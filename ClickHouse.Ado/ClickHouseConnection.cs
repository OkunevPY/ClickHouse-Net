using System;
#if !NETCOREAPP11
using System.Data;
using System.Data.Common;
#endif
using System.IO;
using System.Net.Sockets;
using ClickHouse.Ado.Impl;
using ClickHouse.Ado.Impl.Data;

namespace ClickHouse.Ado
{
    public class ClickHouseConnection
#if !NETCOREAPP11
        : DbConnection
#endif
    {
        private bool _disposed;
        public ClickHouseConnectionStringBuilder ConnectionSettings { get; private set; }

        public ClickHouseConnection()
        {
        }

        public ClickHouseConnection(ClickHouseConnectionStringBuilder settings)
        {
            ConnectionSettings = settings;
        }
        public ClickHouseConnection(string connectionString)
        {
            ConnectionSettings = new ClickHouseConnectionStringBuilder(connectionString);
        }

        private TcpClient _tcpClient;
        private Stream _stream;
        /*private BinaryReader _reader;
        private BinaryWriter _writer;*/
        internal ProtocolFormatter Formatter { get; set; }
        private NetworkStream _netStream;
        public override void Close()
        {
            /*if (_reader != null)
            {
                _reader.Close();
                _reader.Dispose();
                _reader = null;
            }
            if (_writer != null)
            {
                _writer.Close();
                _writer.Dispose();
                _writer = null;
            }*/
            if (_stream != null)
            {
#if !NETSTANDARD15 && !NETCOREAPP11
				_stream.Close();
#endif
				_stream.Dispose();
                _stream = null;
            }
            if (_netStream != null)
            {
#if !NETSTANDARD15 &&!NETCOREAPP11
				_netStream.Close();
#endif
				_netStream.Dispose();
                _netStream = null;
            }
            if (_tcpClient != null)
            {
#if !NETSTANDARD15 && !NETCOREAPP11
				_tcpClient.Close();
#else
				_tcpClient.Dispose();
#endif
				_tcpClient = null;
            }
            if (Formatter != null)
            {
                Formatter.Close();
                Formatter = null;
            }
            _disposed = true;
        }


        public override void Open()
        {
            if(_tcpClient!=null) throw new InvalidOperationException("Connection already open.");
            _tcpClient=new TcpClient();
            _tcpClient.ReceiveTimeout = ConnectionSettings.SocketTimeout;
            _tcpClient.SendTimeout = ConnectionSettings.SocketTimeout;
            //_tcpClient.NoDelay = true;
            _tcpClient.ReceiveBufferSize = ConnectionSettings.BufferSize;
            _tcpClient.SendBufferSize = ConnectionSettings.BufferSize;
#if NETCOREAPP11
            _tcpClient.ConnectAsync(ConnectionSettings.Host, ConnectionSettings.Port).Wait();
#elif NETSTANDARD15
            _tcpClient.ConnectAsync(ConnectionSettings.Host, ConnectionSettings.Port).ConfigureAwait(false).GetAwaiter().GetResult();
#else
			_tcpClient.Connect(ConnectionSettings.Host, ConnectionSettings.Port);
#endif
            _netStream = new NetworkStream(_tcpClient.Client);
            _stream =new UnclosableStream(_netStream);
            /*_reader=new BinaryReader(new UnclosableStream(_stream));
            _writer=new BinaryWriter(new UnclosableStream(_stream));*/
            var ci=new ClientInfo();
            ci.InitialAddress = ci.CurrentAddress = _tcpClient.Client.RemoteEndPoint;
            ci.PopulateEnvironment();

            Formatter = new ProtocolFormatter(_stream,ci, ()=>_tcpClient.Client.Poll(ConnectionSettings.SocketTimeout, SelectMode.SelectRead));
            Formatter.Handshake(ConnectionSettings);
        }
        public override string ConnectionString
        {
            get { return ConnectionSettings.ToString(); }
            set { ConnectionSettings = new ClickHouseConnectionStringBuilder(value); }
        }

        public override string Database => ConnectionSettings.Database;

        public override string DataSource { get; }
#if !NETCOREAPP11
        public override string ServerVersion { get; }

        public override ConnectionState State => Formatter != null ? ConnectionState.Open : ConnectionState.Closed;

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotImplementedException();
        }
        protected override DbCommand CreateDbCommand()
        {
            return new ClickHouseDbCommand(this);
        }
#endif
        public override void ChangeDatabase(string databaseName)
        {
            CreateCommand("USE " + ProtocolFormatter.EscapeName(databaseName)).ExecuteNonQuery();
            ConnectionSettings.Database = databaseName;
        }

        public ClickHouseDbCommand CreateCommand()
        {
            return new ClickHouseDbCommand(this);
        }
        public ClickHouseDbCommand CreateCommand(string text)
        {
            return new ClickHouseDbCommand(this,text);
        }

        public ClickHouseConnection CloneWith(string connectionString)
        {
            CheckDisposed();
            ClickHouseConnectionStringBuilder csb = new ClickHouseConnectionStringBuilder(connectionString);
            if (csb.Password == null && ConnectionSettings.Password != null)
            {
                csb.Password = ConnectionSettings.Password;
            }
            return new ClickHouseConnection(csb);
        }
        private void CheckDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(typeof(ClickHouseConnection).Name);
            }
        }
    }
}
