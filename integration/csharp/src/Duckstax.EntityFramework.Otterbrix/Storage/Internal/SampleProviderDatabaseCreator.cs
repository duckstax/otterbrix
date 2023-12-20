namespace Duckstax.EntityFramework.Otterbrix.Storage.Internal;

using Microsoft.EntityFrameworkCore.Sqlite.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;

public class SampleProviderDatabaseCreator : RelationalDatabaseCreator
{
    private readonly SqliteDatabaseCreator sqliteDatabaseCreator;
    private readonly ISampleProviderRelationalConnection relationalConnection;
    private readonly IRawSqlCommandBuilder rawSqlCommandBuilder;

    public SampleProviderDatabaseCreator(
        RelationalDatabaseCreatorDependencies dependencies,
        ISampleProviderRelationalConnection relationalConnection,
        IRawSqlCommandBuilder rawSqlCommandBuilder)
        : base(dependencies)
    {
        sqliteDatabaseCreator = new(dependencies, relationalConnection.SqliteRelationalConnection, rawSqlCommandBuilder);

        this.relationalConnection = relationalConnection;
        this.rawSqlCommandBuilder = rawSqlCommandBuilder;
    }

    public override void Create()
        => sqliteDatabaseCreator.Create();

    public override void Delete()
        => sqliteDatabaseCreator.Delete();

    public override bool Exists()
        => sqliteDatabaseCreator.Exists();

    public override bool HasTables()
        => sqliteDatabaseCreator.HasTables();
}
