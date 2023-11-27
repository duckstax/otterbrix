namespace Duckstax.EntityFramework.Ottergon.Query.Internal;

using Microsoft.EntityFrameworkCore.Query;

public class SampleProviderQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
	public SampleProviderQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies)
    {
        Dependencies = dependencies;
    }

    /// <summary>
    ///     Relational provider-specific dependencies for this service.
    /// </summary>
    protected virtual QuerySqlGeneratorDependencies Dependencies { get; }

    public virtual QuerySqlGenerator Create()
        => new SampleProviderQuerySqlGenerator(Dependencies);
}
