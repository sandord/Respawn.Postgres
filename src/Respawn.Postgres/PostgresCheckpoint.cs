using System;
using System.Threading.Tasks;
using Npgsql;

namespace Respawn.Postgres
{
    public class PostgresCheckpoint
    {
        public const string CacheDatabasePostfix = "__respawn_cache";

        private readonly Checkpoint _checkpoint;

        public string[] TablesToIgnore
        {
            get => _checkpoint.TablesToIgnore;
            set => _checkpoint.TablesToIgnore = value;
        }

        public string[] SchemasToInclude
        {
            get => _checkpoint.SchemasToInclude;
            set => _checkpoint.SchemasToInclude = value;
        }

        public string[] SchemasToExclude
        {
            get => _checkpoint.SchemasToExclude;
            set => _checkpoint.SchemasToExclude = value;
        }

        public int? CommandTimeout
        {
            get => _checkpoint.CommandTimeout;
            set => _checkpoint.CommandTimeout = value;
        }

        public bool AutoCreateExtensions { get; set; }

        public PostgresCheckpoint()
        {
            _checkpoint = new Checkpoint
            {
                DbAdapter = DbAdapter.Postgres
            };
        }

        public async Task Reset(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString)) throw new ArgumentException("Null or empty.", nameof(connectionString));

            var databaseName = PostgresHelper.ExtractDatabaseName(connectionString);
            var cacheDatabaseName = databaseName + CacheDatabasePostfix;

            if (AutoCreateExtensions)
            {
                CreatePostgresExtensions(connectionString);
            }

            var cacheDatabaseExists = PostgresHelper.GetDatabaseExists(connectionString, cacheDatabaseName, CommandTimeout);
            var cacheDatabaseIsUpToDate = cacheDatabaseExists && CheckCacheDatabaseIsUpToDate(connectionString, cacheDatabaseName);

            if (cacheDatabaseExists && cacheDatabaseIsUpToDate)
            {
                // Copy cache database onto main database.
                PostgresHelper.CopyDatabaseIfNotExists(connectionString, databaseName, cacheDatabaseName, CommandTimeout);
            }
            else
            {
                await ResetDatabase(connectionString);

                // Copy main database onto cache database.
                PostgresHelper.CopyDatabaseIfNotExists(connectionString, cacheDatabaseName, commandTimeout: CommandTimeout);
            }

            // Clear the connection pools because there may be connections in them which were broken by a PostgresHelper.CloseClientConnections call.
            PostgresHelper.ClearAllPools();
        }

        private void CreatePostgresExtensions(string connectionString)
        {
            PostgresHelper.CreateExtensionIfNotExists(connectionString, "dblink", CommandTimeout);
        }

        private bool CheckCacheDatabaseIsUpToDate(string connectionString, string cacheDatabaseName)
        {
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString)
            {
                Database = cacheDatabaseName
            };

            var cacheDatabaseConnectionString = connectionStringBuilder.ConnectionString;

            var databaseStructureHash = PostgresHelper.GetDatabaseStructureHash(connectionString, CommandTimeout);
            var cacheDatabaseStructureHash = PostgresHelper.GetDatabaseStructureHash(cacheDatabaseConnectionString, CommandTimeout);

            return databaseStructureHash == cacheDatabaseStructureHash;
        }

        private async Task ResetDatabase(string connectionString)
        {
            using (var connection = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync();
                await _checkpoint.Reset(connection);
            }
        }
    }
}
