using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Synchrony.Core.Abstraction;

namespace Synchrony.Core
{
    public class DTLContext : IDTL
    {
        private DataTable insertedData;
        public IMap Next => GetMapper();
        private HashSet<Mapper> mappers = new HashSet<Mapper>();
        private HashSet<string> autoIncrementKeys = new HashSet<string>();
        private readonly string sourceConnectionString;
        private readonly string destinationConnectionString;
        private readonly string destinationTableName;
        private readonly string sourceQuery;
        private string originalIdColumnName;
        private SqlDbType originalIdColumnType;
        public DTLContext(string sourceConnectionString, string destinationConnectionString, string destinationTableName, string sourceQuery)
        {
            this.sourceConnectionString = sourceConnectionString;
            this.destinationConnectionString = destinationConnectionString;
            this.destinationTableName = destinationTableName;
            this.sourceQuery = sourceQuery;
        }
        public async Task ApplyAsync(CancellationToken cancellationToken)
        {
            DataTable dataTable = new DataTable();
            using (SqlConnection con = new SqlConnection(sourceConnectionString))
            {
                SqlCommand command = con.CreateCommand();
                command.CommandText = sourceQuery;
                await con.OpenAsync(cancellationToken);
                dataTable.Load(await command.ExecuteReaderAsync(CommandBehavior.CloseConnection, cancellationToken));
            }
            using (SqlConnection con = new SqlConnection(destinationConnectionString))
            {
                using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(con))
                {
                    sqlBulkCopy.DestinationTableName = destinationTableName;
                    foreach (var i in mappers)
                    {
                        SqlBulkCopyColumnMapping sqlBulkCopyColumnMapping = new SqlBulkCopyColumnMapping(i.FromField, i.ToField);
                        sqlBulkCopy.ColumnMappings.Add(sqlBulkCopyColumnMapping);
                    }
                    if (!string.IsNullOrEmpty(originalIdColumnName))
                    {
                        SqlBulkCopyColumnMapping sqlBulkCopyColumnMapping = new SqlBulkCopyColumnMapping(originalIdColumnName, "__$ref");
                        sqlBulkCopy.ColumnMappings.Add(sqlBulkCopyColumnMapping);
                    }
                    SqlCommand command = con.CreateCommand();
                    command.CommandText = $"ALTER TABLE {destinationTableName} ADD __$ref {originalIdColumnType.ToString()}";
                    await con.OpenAsync(cancellationToken);
                    await command.ExecuteNonQueryAsync(cancellationToken);
                    await sqlBulkCopy.WriteToServerAsync(dataTable, cancellationToken);
                    dataTable.Clear();
                    dataTable.Dispose();
                    insertedData = new DataTable();
                    command = con.CreateCommand();
                    command.CommandText = $"SELECT * FROM {destinationTableName}";
                    insertedData.Load(await command.ExecuteReaderAsync(cancellationToken));
                    command = con.CreateCommand();
                    command.CommandText = $"ALTER TABLE {destinationTableName} DROP COLUMN __$ref";
                    await command.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }
        public IMap GetMapper()
        {
            Mapper mapper = new Mapper(this);
            mappers.Add(mapper);
            return mapper;
        }

        public void UseOriginalId(string columnName, SqlDbType sqlDbType)
        {
            originalIdColumnName = columnName;
            originalIdColumnType = sqlDbType; ;
        }

        public DataRow GetInsertedRowFor<T>(T oldId)
        {
            return insertedData.AsEnumerable().Where(x=> x.Field<T>("__$ref").Equals(oldId)).FirstOrDefault();
        }
    }
}