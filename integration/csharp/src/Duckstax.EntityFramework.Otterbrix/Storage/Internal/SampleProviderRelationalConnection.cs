namespace Duckstax.EntityFramework.Otterbrix.Storage.Internal;

using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Sqlite.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;

public class SampleProviderRelationalConnection : RelationalConnection, ISampleProviderRelationalConnection
{
    /// <summary>
    /// Derived class to expose protected methods of SQLite's SqliteTypeMappingSource class.
    /// </summary>
    private class SqliteRelationalConnectionDerived : SqliteRelationalConnection
    {
        public SqliteRelationalConnectionDerived(RelationalConnectionDependencies dependencies, IRawSqlCommandBuilder rawSqlCommandBuilder, IDiagnosticsLogger<DbLoggerCategory.Infrastructure> logger)
            : base(dependencies, rawSqlCommandBuilder, logger)
        {

        }

        public DbConnection CreateDbConnection_Exposed()
            => CreateDbConnection();
    }

    private readonly SqliteRelationalConnectionDerived sqliteRelationalConnection;

    public SampleProviderRelationalConnection(
        RelationalConnectionDependencies dependencies, 
        IRawSqlCommandBuilder rawSqlCommandBuilder, 
        IDiagnosticsLogger<DbLoggerCategory.Infrastructure> logger)
        : base(dependencies)
    {
        sqliteRelationalConnection = new(dependencies, rawSqlCommandBuilder, logger);
    }

    //TODO: Delete this property in your own provider, because this is exposed since DI isn't available for the SQLite classes and it was needed by our SampleProviderDatabaseCreator class
    public SqliteRelationalConnection SqliteRelationalConnection => sqliteRelationalConnection;

    protected override DbConnection CreateDbConnection()
        => sqliteRelationalConnection.CreateDbConnection_Exposed();

}
