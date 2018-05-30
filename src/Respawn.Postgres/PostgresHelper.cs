using System;
using System.Linq;
using Npgsql;

namespace Respawn.Postgres
{
    internal static class PostgresHelper
    {
        private const string PostgresSystemDatabase = "postgres";

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

        internal static bool GetDatabaseExists(string connectionString, string databaseName = null)
        {
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString);

            if (string.IsNullOrEmpty(databaseName))
            {
                databaseName = connectionStringBuilder.Database;
            }

            connectionStringBuilder.Database = PostgresSystemDatabase;
            var systemConnectionString = connectionStringBuilder.ConnectionString;

            return GetDatabaseExistsInternal(systemConnectionString, databaseName);
        }


        internal static void CopyDatabaseIfNotExists(string connectionString, string targetDatabaseName, string sourceDatabaseName = null)
        {
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString);

            if (string.IsNullOrEmpty(sourceDatabaseName))
            {
                sourceDatabaseName = connectionStringBuilder.Database;
            }

            // Create a connection string that connects to the system (postgres) database.
            connectionStringBuilder.Database = "postgres";
            var systemConnectionString = connectionStringBuilder.ConnectionString;

            if (!GetDatabaseExistsInternal(targetDatabaseName, systemConnectionString))
            {
                CloseClientConnections(systemConnectionString, sourceDatabaseName);
                CloseClientConnections(systemConnectionString, targetDatabaseName);

                CopyDatabaseInternal(systemConnectionString, targetDatabaseName, sourceDatabaseName);
            }
        }

        internal static void CreateExtensionIfNotExists(string connectionString, string extensionName)
        {
            ValidateDatabaseEntityName(extensionName);

            using (var connection = new NpgsqlConnection(connectionString))
            {
                connection.Open();
                
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"create extension if not exists {extensionName};";
                    command.ExecuteNonQuery();
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

        private static bool GetDatabaseExistsInternal(string systemConnectionString, string databaseName)
        {
            ValidateDatabaseEntityName(databaseName);

            using (var connection = new NpgsqlConnection(systemConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"SELECT 1 FROM pg_database WHERE datname='{databaseName}';";
                    return command.ExecuteScalar() != null;
                }
            }
        }

        private static void CloseClientConnections(string connectionString, string databaseName)
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
                    command.CommandText = $@"
                        select pg_terminate_backend(pg_stat_activity.pid)
                        from pg_stat_activity
                        where pg_stat_activity.datname = '{databaseName}' and pid<> pg_backend_pid();";

                    command.ExecuteNonQuery();
                }
            }
        }

        private static void CopyDatabaseInternal(string systemConnectionString, string targetDatabaseName, string sourceDatabaseName)
        {
            ValidateDatabaseEntityName(targetDatabaseName);
            ValidateDatabaseEntityName(sourceDatabaseName);

            using (var connection = new NpgsqlConnection(systemConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"create database {targetDatabaseName} template {sourceDatabaseName};";
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
