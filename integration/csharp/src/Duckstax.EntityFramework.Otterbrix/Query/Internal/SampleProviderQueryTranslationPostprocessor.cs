namespace Duckstax.EntityFramework.Otterbrix.Query.Internal;

using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Sqlite.Query.Internal;

public class SampleProviderQueryTranslationPostprocessor : RelationalQueryTranslationPostprocessor
{
    private readonly SqliteQueryTranslationPostprocessor sqliteQueryTranslationPostprocessor;

	public SampleProviderQueryTranslationPostprocessor(
        QueryTranslationPostprocessorDependencies dependencies,
        RelationalQueryTranslationPostprocessorDependencies relationalDependencies,
        QueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        sqliteQueryTranslationPostprocessor = new(dependencies, relationalDependencies, queryCompilationContext);
    }

    public override Expression Process(Expression query) => sqliteQueryTranslationPostprocessor.Process(query);
}
