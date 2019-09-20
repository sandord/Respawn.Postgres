using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Npgsql;

namespace Respawn.Postgres
{
    internal static class PostgresHelper
    {
        private const string PostgresSystemDatabase = "postgres";

        static readonly IReadOnlyDictionary<string, string> _columnQueryOrder = new Dictionary<string, string>
        {
            {"n", "pg_namespace"},
            {"f", "pg_attribute"},
            {"p", "pg_constraint"},
            {"g", "pg_class"},
            {"c", "pg_class"},
            {"t", "pg_type"},
        };

        internal static string ExtractDatabaseName(string connectionString)
        {
            try
            {
                var name = new NpgsqlConnectionStringBuilder(connectionString).Database;

                if (string.IsNullOrEmpty(name))
                {
                    throw new InvalidOperationException("Extracted database name is null or empty.");
                }

                return name;
            }
            catch (Exception exception)
            {
                throw new InvalidOperationException(
                    "The provided connection string is probably invalid since no database name could be extracted from it.",
                    exception);
            }
        }

        internal static bool GetDatabaseExists(string connectionString, string databaseName = null, int? commandTimeout = null)
        {
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString);

            if (string.IsNullOrEmpty(databaseName))
            {
                databaseName = connectionStringBuilder.Database;
            }

            connectionStringBuilder.Database = PostgresSystemDatabase;
            var systemConnectionString = connectionStringBuilder.ConnectionString;

            return GetDatabaseExistsInternal(systemConnectionString, databaseName, commandTimeout);
        }

        internal static void CopyDatabase(string connectionString, string targetDatabaseName, string sourceDatabaseName = null, int? commandTimeout = null, bool autoCreateExtensions = false)
        {
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString);

            if (string.IsNullOrEmpty(sourceDatabaseName))
            {
                sourceDatabaseName = connectionStringBuilder.Database;
            }

            connectionStringBuilder.Database = PostgresSystemDatabase;
            var systemConnectionString = connectionStringBuilder.ConnectionString;

            CloseClientConnections(systemConnectionString, sourceDatabaseName, commandTimeout);
            CloseClientConnections(systemConnectionString, targetDatabaseName, commandTimeout);

            DropDatabaseIfExistsInternal(systemConnectionString, targetDatabaseName, commandTimeout, autoCreateExtensions);
            CopyDatabaseInternal(systemConnectionString, targetDatabaseName, sourceDatabaseName, commandTimeout);
        }

        internal static void CreateExtensionIfNotExists(string connectionString, string extensionName, int? commandTimeout = null)
        {
            ValidateDatabaseEntityName(extensionName);

            using (var connection = new NpgsqlConnection(connectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    if (commandTimeout.HasValue)
                    {
                        command.CommandTimeout = commandTimeout.Value;
                    }

                    command.CommandText = $"create extension if not exists {extensionName};";
                    command.ExecuteNonQuery();
                }
            }
        }

        static List<DatabaseColumn> GetDatabaseColumns(NpgsqlCommand command)
        {
            command.CommandText = @"
                select 
	                t.table_name,
	                c.column_name
                from information_schema.tables t
                left join information_schema.columns c 
	                on t.table_schema = c.table_schema 
	                and t.table_name = c.table_name
                where (table_type = 'VIEW' and t.table_name = 'columns')
                or t.table_type = 'BASE TABLE' and t.table_name in (
	                'pg_class',
	                'pg_type',
	                'pg_attrdef',
	                'pg_namespace',
	                'pg_constraint'
                );
            ";

            var columns = new List<DatabaseColumn>();
            using (var reader = command.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        columns.Add(new DatabaseColumn
                        {
                            TableName = reader.GetString(0),
                            ColumnName = reader.GetString(1)
                        });
                    }
                }
            }

            Debug.Assert(columns.Count == 136, "Not all columns were read!");

            return columns;
        }

        internal static decimal GetDatabaseStructureHash(string connectionString, int? commandTimeout = null)
        {
            using (var connection = new NpgsqlConnection(connectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    if (commandTimeout.HasValue)
                    {
                        command.CommandTimeout = commandTimeout.Value;
                    }

                    var columns = GetDatabaseColumns(command);

                    var builder = new StringBuilder();
                    builder.AppendLine("SELECT SUM(('x' || md5(");

                    // We use a fixed order for types and then sort their columns by name
                    // to ensure consistency between queries

                    var first = true;
                    foreach (var item in _columnQueryOrder)
                    {
                        var sorted = columns.Where(x => item.Value.Equals(x.TableName, StringComparison.OrdinalIgnoreCase))
                            .OrderBy(x => x.ColumnName)
                            .ToList();

                        foreach (var column in sorted)
                        {
                            if (first)
                            {
                                builder.AppendFormat("\n\t\tcoalesce({0}.{1}::text, '')", item.Key, column.ColumnName);
                                first = false;
                            }
                            else
                            {
                                builder.AppendFormat("\n\t\t|| ' ' || coalesce({0}.{1}::text, '')", item.Key, column.ColumnName);
                            }
                        }
                    }

                    builder.AppendLine(@"
    ))::bit(64)::bigint)
FROM pg_attribute f
JOIN pg_class c ON c.oid = f.attrelid
JOIN pg_type t ON t.oid = f.atttypid
LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum  
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace  
LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY (p.conkey)  
LEFT JOIN pg_class AS g ON p.confrelid = g.oid;");

                    command.CommandText = builder.ToString();

                    return (decimal)(command.ExecuteScalar() ?? throw new InvalidOperationException("Could not determine database structure hash."));
                }
            }
        }

        internal static void ClearAllPools() => NpgsqlConnection.ClearAllPools();

        private static void ValidateDatabaseEntityName(string entityName)
        {
            if (entityName == null || !entityName.All(n => char.IsLetterOrDigit(n) || n == '-' || n == '_'))
            {
                throw new InvalidOperationException("Invalid character(s) found in database entity name.");
            }
        }

        private static bool GetDatabaseExistsInternal(string systemConnectionString, string databaseName, int? commandTimeout)
        {
            ValidateDatabaseEntityName(databaseName);

            using (var connection = new NpgsqlConnection(systemConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    if (commandTimeout.HasValue)
                    {
                        command.CommandTimeout = commandTimeout.Value;
                    }

                    command.CommandText = $"SELECT 1 FROM pg_database WHERE datname='{databaseName}';";
                    return command.ExecuteScalar() != null;
                }
            }
        }

        private static void CloseClientConnections(string connectionString, string databaseName, int? commandTimeout)
        {
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString);

            if (string.IsNullOrEmpty(databaseName))
            {
                databaseName = connectionStringBuilder.Database;
            }

            connectionStringBuilder.Database = PostgresSystemDatabase;
            var systemConnectionString = connectionStringBuilder.ConnectionString;

            ValidateDatabaseEntityName(databaseName);

            using (var connection = new NpgsqlConnection(systemConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    if (commandTimeout.HasValue)
                    {
                        command.CommandTimeout = commandTimeout.Value;
                    }

                    command.CommandText = $@"
                        select pg_terminate_backend(pg_stat_activity.pid)
                        from pg_stat_activity
                        where pg_stat_activity.datname = '{databaseName}' and pid<> pg_backend_pid();";

                    command.ExecuteNonQuery();
                }
            }
        }

        private static void CopyDatabaseInternal(string systemConnectionString, string targetDatabaseName, string sourceDatabaseName, int? commandTimeout)
        {
            ValidateDatabaseEntityName(targetDatabaseName);
            ValidateDatabaseEntityName(sourceDatabaseName);

            using (var connection = new NpgsqlConnection(systemConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    if (commandTimeout.HasValue)
                    {
                        command.CommandTimeout = commandTimeout.Value;
                    }

                    command.CommandText = $"create database \"{targetDatabaseName}\" template \"{sourceDatabaseName}\";";
                    command.ExecuteNonQuery();
                }
            }
        }

        private static void DropDatabaseIfExistsInternal(string systemConnectionString, string databaseName, int? commandTimeout, bool autoCreateExtensions = false)
        {
            if (autoCreateExtensions)
            {
                CreateExtensionIfNotExists(systemConnectionString, "dblink", commandTimeout);
            }

            using (var connection = new NpgsqlConnection(systemConnectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();

                if (commandTimeout.HasValue)
                {
                    command.CommandTimeout = commandTimeout.Value;
                }

                command.CommandText = $"select exists (select 1 from pg_database where datname='{databaseName}')";
                // ReSharper disable once PossibleNullReferenceException
                var result = (bool)command.ExecuteScalar();
                if (result)
                {
                    command.CommandText = $"drop database \"{databaseName}\"";
                    command.ExecuteNonQuery();
                }
            }
        }

        class DatabaseColumn
        {
            public string TableName { get; set; }
            public string ColumnName { get; set; }
        }
    }
}
