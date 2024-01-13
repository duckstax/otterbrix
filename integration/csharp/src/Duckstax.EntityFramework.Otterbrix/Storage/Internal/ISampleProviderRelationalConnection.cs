namespace Duckstax.EntityFramework.Otterbrix.Storage.Internal;

using Microsoft.EntityFrameworkCore.Sqlite.Storage.Internal;
using Microsoft.EntityFrameworkCore.Storage;

public interface ISampleProviderRelationalConnection : IRelationalConnection
{
    //TODO: Delete this property in your own provider, because this is exposed since DI isn't available for the SQLite classes and it was needed by our SampleProviderDatabaseCreator class
    SqliteRelationalConnection SqliteRelationalConnection { get; }


    //TODO: Add methods for any special types of conntections needed.
    //Examples:
    // - an admin connection https://github.dev/npgsql/efcore.pg/blob/3e4ef719b350f9426c03586884cd75ddf3c3d645/src/EFCore.PG/Storage/Internal/INpgsqlRelationalConnection.cs#L17
    // - connection to the master database https://github.dev/PomeloFoundation/Pomelo.EntityFrameworkCore.MySql/blob/e8377890f1baffbb52eebbce9f5f14687789a83d/src/EFCore.MySql/Storage/Internal/IMySqlRelationalConnection.cs#L18
    // - readonly connections https://github.dev/dotnet/efcore/blob/38f69c6c9a773e3395f507d7e054f40d55962ced/src/EFCore.Sqlite.Core/Storage/Internal/ISqliteRelationalConnection.cs#L26
}
