namespace Duckstax.EntityFramework.Otterbrix.Query.Internal;

using System.Data.Common;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Sqlite.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;

public class SampleProviderQueryStringFactory : IRelationalQueryStringFactory
{
	private readonly SqliteQueryStringFactory sqliteQueryStringFactory;

    public SampleProviderQueryStringFactory(IRelationalTypeMappingSource typeMapper)
	{
        sqliteQueryStringFactory = new(typeMapper);
    }

    public virtual string Create(DbCommand command) => sqliteQueryStringFactory.Create(command);
}
