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

            connectionStringBuilder.Database = PostgresSystemDatabase;
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

        internal static long GetDatabaseStructureHash(string connectionString)
        {
            using (var connection = new NpgsqlConnection(connectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                    SELECT SUM(('x' || md5(
                             coalesce(n.nspname, '') ||
                      ' ' || coalesce(n.nspowner::text, '') ||
                      ' ' || coalesce(n.nspacl::text, '') ||
                      ' ' || coalesce(f.attrelid::text, '') ||
                      ' ' || coalesce(f.attname, '') ||
                      ' ' || coalesce(f.atttypid::text, '') ||
                      ' ' || coalesce(f.attstattarget::text, '') ||
                      ' ' || coalesce(f.attlen::text, '') ||
                      ' ' || coalesce(f.attnum::text, '') ||
                      ' ' || coalesce(f.attndims::text, '') ||
                      ' ' || coalesce(f.attcacheoff::text, '') ||
                      ' ' || coalesce(f.atttypmod::text, '') ||
                      ' ' || coalesce(f.attbyval::text, '') ||
                      ' ' || coalesce(f.attstorage, '') ||
                      ' ' || coalesce(f.attalign, '') ||
                      ' ' || coalesce(f.attnotnull::text, '') ||
                      ' ' || coalesce(f.atthasdef::text, '') ||
                      ' ' || coalesce(f.attidentity, '') ||
                      ' ' || coalesce(f.attisdropped::text, '') ||
                      ' ' || coalesce(f.attislocal::text, '') ||
                      ' ' || coalesce(f.attinhcount::text, '') ||
                      ' ' || coalesce(f.attcollation::text, '') ||
                      ' ' || coalesce(f.attacl::text, '') ||
                      ' ' || coalesce(f.attoptions::text, '') ||
                      ' ' || coalesce(f.attfdwoptions::text, '') ||
                      ' ' || coalesce(p.conname, '') ||
                      ' ' || coalesce(p.connamespace::text, '') ||
                      ' ' || coalesce(p.contype, '') ||
                      ' ' || coalesce(p.condeferrable::text, '') ||
                      ' ' || coalesce(p.condeferred::text, '') ||
                      ' ' || coalesce(p.convalidated::text, '') ||
                      ' ' || coalesce(p.conrelid::text, '') ||
                      ' ' || coalesce(p.contypid::text, '') ||
                      ' ' || coalesce(p.conindid::text, '') ||
                      ' ' || coalesce(p.confrelid::text, '') ||
                      ' ' || coalesce(p.confupdtype, '') ||
                      ' ' || coalesce(p.confdeltype, '') ||
                      ' ' || coalesce(p.confmatchtype, '') ||
                      ' ' || coalesce(p.conislocal::text, '') ||
                      ' ' || coalesce(p.coninhcount::text, '') ||
                      ' ' || coalesce(p.connoinherit::text, '') ||
                      ' ' || coalesce(p.conkey::text, '') ||
                      ' ' || coalesce(p.confkey::text, '') ||
                      ' ' || coalesce(p.conpfeqop::text, '') ||
                      ' ' || coalesce(p.conppeqop::text, '') ||
                      ' ' || coalesce(p.conffeqop::text, '') ||
                      ' ' || coalesce(p.conexclop::text, '') ||
                      ' ' || coalesce(p.conbin::text, '') ||
                      ' ' || coalesce(p.consrc, '') ||
                      ' ' || coalesce(g.relname, '') ||
                      ' ' || coalesce(g.relnamespace::text, '') ||
                      ' ' || coalesce(g.reltype::text, '') ||
                      ' ' || coalesce(g.reloftype::text, '') ||
                      ' ' || coalesce(g.relowner::text, '') ||
                      ' ' || coalesce(g.relam::text, '') ||
                      ' ' || coalesce(g.relfilenode::text, '') ||
                      ' ' || coalesce(g.reltablespace::text, '') ||
                      ' ' || coalesce(g.relpages::text, '') ||
                      ' ' || coalesce(g.reltuples::text, '') ||
                      ' ' || coalesce(g.relallvisible::text, '') ||
                      ' ' || coalesce(g.reltoastrelid::text, '') ||
                      ' ' || coalesce(g.relhasindex::text, '') ||
                      ' ' || coalesce(g.relisshared::text, '') ||
                      ' ' || coalesce(g.relpersistence, '') ||
                      ' ' || coalesce(g.relkind, '') ||
                      ' ' || coalesce(g.relnatts::text, '') ||
                      ' ' || coalesce(g.relchecks::text, '') ||
                      ' ' || coalesce(g.relhasoids::text, '') ||
                      ' ' || coalesce(g.relhaspkey::text, '') ||
                      ' ' || coalesce(g.relhasrules::text, '') ||
                      ' ' || coalesce(g.relhastriggers::text, '') ||
                      ' ' || coalesce(g.relhassubclass::text, '') ||
                      ' ' || coalesce(g.relrowsecurity::text, '') ||
                      ' ' || coalesce(g.relforcerowsecurity::text, '') ||
                      ' ' || coalesce(g.relispopulated::text, '') ||
                      ' ' || coalesce(g.relreplident, '') ||
                      ' ' || coalesce(g.relispartition::text, '') ||
                      ' ' || coalesce(g.relfrozenxid::text, '') ||
                      ' ' || coalesce(g.relminmxid::text, '') ||
                      ' ' || coalesce(g.relacl::text, '') ||
                      ' ' || coalesce(g.reloptions::text, '') ||
                      ' ' || coalesce(g.relpartbound::text, '') ||
                      ' ' || coalesce(n.nspname, '') ||
                      ' ' || coalesce(n.nspowner::text, '') ||
                      ' ' || coalesce(n.nspacl::text, '') ||
                      ' ' || coalesce(c.relname, '') ||
                      ' ' || coalesce(c.relnamespace::text, '') ||
                      ' ' || coalesce(c.reltype::text, '') ||
                      ' ' || coalesce(c.reloftype::text, '') ||
                      ' ' || coalesce(c.relowner::text, '') ||
                      ' ' || coalesce(c.relam::text, '') ||
                      ' ' || coalesce(c.relfilenode::text, '') ||
                      ' ' || coalesce(c.reltablespace::text, '') ||
                      ' ' || coalesce(c.relpages::text, '') ||
                      ' ' || coalesce(c.reltuples::text, '') ||
                      ' ' || coalesce(c.relallvisible::text, '') ||
                      ' ' || coalesce(c.reltoastrelid::text, '') ||
                      ' ' || coalesce(c.relhasindex::text, '') ||
                      ' ' || coalesce(c.relisshared::text, '') ||
                      ' ' || coalesce(c.relpersistence, '') ||
                      ' ' || coalesce(c.relkind, '') ||
                      ' ' || coalesce(c.relnatts::text, '') ||
                      ' ' || coalesce(c.relchecks::text, '') ||
                      ' ' || coalesce(c.relhasoids::text, '') ||
                      ' ' || coalesce(c.relhaspkey::text, '') ||
                      ' ' || coalesce(c.relhasrules::text, '') ||
                      ' ' || coalesce(c.relhastriggers::text, '') ||
                      ' ' || coalesce(c.relhassubclass::text, '') ||
                      ' ' || coalesce(c.relrowsecurity::text, '') ||
                      ' ' || coalesce(c.relforcerowsecurity::text, '') ||
                      ' ' || coalesce(c.relispopulated::text, '') ||
                      ' ' || coalesce(c.relreplident, '') ||
                      ' ' || coalesce(c.relispartition::text, '') ||
                      ' ' || coalesce(c.relfrozenxid::text, '') ||
                      ' ' || coalesce(c.relminmxid::text, '') ||
                      ' ' || coalesce(c.relacl::text, '') ||
                      ' ' || coalesce(c.reloptions::text, '') ||
                      ' ' || coalesce(c.relpartbound::text, '') ||
                      ' ' || coalesce(t.typname, '') ||
                      ' ' || coalesce(t.typnamespace::text, '') ||
                      ' ' || coalesce(t.typowner::text, '') ||
                      ' ' || coalesce(t.typlen::text, '') ||
                      ' ' || coalesce(t.typbyval::text, '') ||
                      ' ' || coalesce(t.typtype, '') ||
                      ' ' || coalesce(t.typcategory, '') ||
                      ' ' || coalesce(t.typispreferred::text, '') ||
                      ' ' || coalesce(t.typisdefined::text, '') ||
                      ' ' || coalesce(t.typdelim, '') ||
                      ' ' || coalesce(t.typrelid::text, '') ||
                      ' ' || coalesce(t.typarray::text, '') ||
                      ' ' || coalesce(t.typinput::text, '') ||
                      ' ' || coalesce(t.typoutput::text, '') ||
                      ' ' || coalesce(t.typreceive::text, '') ||
                      ' ' || coalesce(t.typsend::text, '') ||
                      ' ' || coalesce(t.typmodin::text, '') ||
                      ' ' || coalesce(t.typmodout::text, '') ||
                      ' ' || coalesce(t.typanalyze::text, '') ||
                      ' ' || coalesce(t.typalign, '') ||
                      ' ' || coalesce(t.typstorage, '') ||
                      ' ' || coalesce(t.typnotnull::text, '') ||
                      ' ' || coalesce(t.typbasetype::text, '') ||
                      ' ' || coalesce(t.typtypmod::text, '') ||
                      ' ' || coalesce(t.typndims::text, '') ||
                      ' ' || coalesce(t.typcollation::text, '') ||
                      ' ' || coalesce(t.typdefaultbin::text, '') ||
                      ' ' || coalesce(t.typdefault, '') ||
                      ' ' || coalesce(t.typacl::text, '') ||
                      ' ' || coalesce(d.adrelid::text, '') ||
                      ' ' || coalesce(d.adnum::text, '') ||
                      ' ' || coalesce(d.adbin::text, '') ||
                      ' ' || coalesce(d.adsrc, '')
                      ))::bit(64)::bigint)
                    FROM pg_attribute f  
                        JOIN pg_class c ON c.oid = f.attrelid  
                        JOIN pg_type t ON t.oid = f.atttypid  
                        LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum  
                        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace  
                        LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND f.attnum = ANY (p.conkey)  
                        LEFT JOIN pg_class AS g ON p.confrelid = g.oid;";

                    return (long)(command.ExecuteScalar() ?? throw new InvalidOperationException("Could not determine database structure hash."));
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
