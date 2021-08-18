using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Synchrony.Core.DataModels;

namespace Synchrony.Core
{
    public class SynchronyDbContext
    {
        public event EventHandler<AddEventArgs> Add;
        public event EventHandler<DeleteEventArgs> Delete;
        public event EventHandler<UpdateEventArgs> Update;
        private string connectionString { get; }
        private int timeout { get; }
        public SynchronyDbContext(string connectionString, int timeout)
        {
            this.connectionString = connectionString;
            this.timeout = timeout;
        }
        public async Task Initialize(CancellationToken cancellationToken)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                SqlCommand command = con.CreateCommand();
                command.CommandText = "SELECT DB_ID('Synchrony')";
                await con.OpenAsync(cancellationToken);
                object value = await command.ExecuteScalarAsync(cancellationToken);
                if (value == null)
                {
                    command = con.CreateCommand();
                    command.CommandText = "CREATE DATABASE Synchrony; USE [Synchrony]; CREATE TABLE ChangeDataTracker ( Id INT IDENTITY(1,1) PRIMARY KEY NOTT NULL, [Table] NVARCHAR(256) NOT NULL, [LastRead] DATETIME NOT NULL);";
                    await command.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }
        public async IAsyncEnumerable<ChangeTable> GetChangeTableAsync(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumerationCancellation)
        {
            DataTable data = new DataTable();
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                SqlCommand command = con.CreateCommand();
                command.CommandText = "SELECT * FROM [cdc].[change_tables] A JOIN [cdc].[captured_columns] B ON A.object_id = B.object_id";
                command.CommandTimeout = timeout;
                await con.OpenAsync(cancellationToken);
                data.Load(await command.ExecuteReaderAsync(CommandBehavior.CloseConnection, cancellationToken));
            }
            var query = from row in data.AsEnumerable()
                        group row by row.Field<int>("object_id");
            foreach (var row in query)
            {
                var firstRow = row.FirstOrDefault();
                ChangeTable changeTable = new ChangeTable();
                changeTable.ObjectId = firstRow.Field<int>("object_id");
                changeTable.Version = firstRow.Field<int>("version");
                changeTable.SourceObjectId = firstRow.Field<int>("source_object_id");
                changeTable.CaptureInstance = firstRow.Field<string>("capture_instance");
                changeTable.RoleName = firstRow.Field<string>("role_name");
                changeTable.Columns = row.Select(x => x.Field<string>("column_name")).ToArray();
                yield return changeTable;
            }
        }
        public async Task GetChanges(CancellationToken cancellationToken)
        {
            Queue<Task> tasks = new Queue<Task>();
            await foreach (var i in GetChangeTableAsync(cancellationToken, cancellationToken))
            {
                DataTable dataTable = new DataTable();
                using (SqlConnection con = new SqlConnection(connectionString))
                {
                    SqlCommand command = con.CreateCommand();
                    command.CommandText = $"SELECT * FROM cdc.{i.CaptureInstance}_CT ChangeSet JOIN [Synchrony].dbo.ChangeDataTracker PreviousRead on sys.fn_cdc_map_lsn_to_time(ChangeSet.__$start_lsn) > PreviousRead.LastRead WHERE PreviousRead.[Table] = '{i.CaptureInstance}';";
                    command.CommandTimeout = timeout;
                    await con.OpenAsync(cancellationToken);
                    dataTable.Load(await command.ExecuteReaderAsync(CommandBehavior.CloseConnection, cancellationToken));
                }
                tasks.Enqueue(ProcessChanges(dataTable, i, cancellationToken));
            }
            await Task.WhenAll(tasks);
        }
        private async Task ProcessChanges(DataTable table, ChangeTable changeTable, CancellationToken cancellationToken)
        {

            if (table.Rows.Count > 0)
            {
                var query = from row in table.AsEnumerable()
                            group row by BitConverter.ToString(row.Field<byte[]>("__$seqval"));
                foreach (var i in query)
                {
                    if (i.Count() == 1)
                    {
                        DataRow row = i.FirstOrDefault();
                        Dictionary<string, object> record = new Dictionary<string, object>();
                        foreach (var x in changeTable.Columns)
                        {
                            record.Add(x, row.Field<object>(x));
                        }
                        int operaton = row.Field<int>("__$operation");
                        switch (operaton)
                        {
                            case 1:
                                {
                                    Delete?.Invoke(this, new DeleteEventArgs(changeTable.CaptureInstance, record));
                                    break;
                                }
                            case 2:
                                {
                                    Add?.Invoke(this, new AddEventArgs(changeTable.CaptureInstance, record));
                                    break;
                                }
                        }
                    }
                    else
                    {
                        Dictionary<string, object> oldRecord = new Dictionary<string, object>();
                        Dictionary<string, object> newRecord = new Dictionary<string, object>();
                        foreach (var x in i)
                        {
                            int operaton = x.Field<int>("__$operation");
                            switch (operaton)
                            {
                                case 3:
                                    {
                                        foreach (var y in changeTable.Columns)
                                        {
                                            oldRecord.Add(y, x.Field<object>(y));
                                        }
                                        break;
                                    }
                                case 4:
                                    {
                                        foreach (var y in changeTable.Columns)
                                        {
                                            newRecord.Add(y, x.Field<object>(y));
                                        }
                                        break;
                                    }
                            }
                        }
                        Update?.Invoke(this, new UpdateEventArgs(changeTable.CaptureInstance, oldRecord, newRecord));
                    }
                }
                using (SqlConnection con = new SqlConnection(connectionString))
                {
                    SqlCommand command = con.CreateCommand();
                    command.CommandText = $"UPDATE Synchrony.dbo.ChangeDataTracker SET [LastRead] = GETDATE() WHERE [Table] = '{changeTable.CaptureInstance}'";
                    await con.OpenAsync(cancellationToken);
                    object value = await command.ExecuteScalarAsync(cancellationToken);
                }
            }
            else
            {
                using (SqlConnection con = new SqlConnection(connectionString))
                {
                    SqlCommand command = con.CreateCommand();
                    command.CommandText = $@"
                    IF (SELECT TOP 1 1 FROM Synchrony.dbo.ChangeDataTracker WHERE [Table] ='{changeTable.CaptureInstance}') IS NULL
                        BEGIN
                            INSERT INTO Synchrony.dbo.ChangeDataTracker VALUES('{changeTable.CaptureInstance}', GETDATE())
                        END
                    ";
                    await con.OpenAsync(cancellationToken);
                    object value = await command.ExecuteScalarAsync(cancellationToken);
                }
            }
        }
    }
}