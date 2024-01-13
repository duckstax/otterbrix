namespace Duckstax.EntityFramework.Otterbrix.Query.Internal;

using Microsoft.EntityFrameworkCore.Query;

public class SampleProviderSqlTranslatingExpressionVisitorFactory : IRelationalSqlTranslatingExpressionVisitorFactory
{
    public SampleProviderSqlTranslatingExpressionVisitorFactory(RelationalSqlTranslatingExpressionVisitorDependencies dependencies)
    {
        Dependencies = dependencies;
    }

    /// <summary>
    ///     Relational provider-specific dependencies for this service.
    /// </summary>
    protected virtual RelationalSqlTranslatingExpressionVisitorDependencies Dependencies { get; }

    public virtual RelationalSqlTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext, QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        => new SampleProviderSqlTranslatingExpressionVisitor(Dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor);

}
