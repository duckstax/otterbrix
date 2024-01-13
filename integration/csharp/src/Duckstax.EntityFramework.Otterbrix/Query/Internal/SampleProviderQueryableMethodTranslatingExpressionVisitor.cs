namespace Duckstax.EntityFramework.Otterbrix.Query.Internal;

using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Sqlite.Query.Internal;

public class SampleProviderQueryableMethodTranslatingExpressionVisitor : RelationalQueryableMethodTranslatingExpressionVisitor
{
    private class SqliteQueryableMethodTranslatingExpressionVisitorDerived : SqliteQueryableMethodTranslatingExpressionVisitor
    {
        public SqliteQueryableMethodTranslatingExpressionVisitorDerived(
            QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
            RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies,
            QueryCompilationContext queryCompilationContext)
            : base(dependencies, relationalDependencies, queryCompilationContext)
        {
        }

        public QueryableMethodTranslatingExpressionVisitor CreateSubqueryVisitor_Exposed() => CreateSubqueryVisitor();
        public ShapedQueryExpression? TranslateOrderBy_Exposed(ShapedQueryExpression source, LambdaExpression keySelector, bool ascending)
            => TranslateOrderBy(source, keySelector, ascending);
        public ShapedQueryExpression? TranslateThenBy_Exposed(ShapedQueryExpression source, LambdaExpression keySelector, bool ascending)
            => TranslateThenBy(source, keySelector, ascending);
    }

    private readonly SqliteQueryableMethodTranslatingExpressionVisitorDerived sqliteQueryableMethodTranslatingExpressionVisitor;

    public SampleProviderQueryableMethodTranslatingExpressionVisitor(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies,
        QueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        sqliteQueryableMethodTranslatingExpressionVisitor = new(dependencies, relationalDependencies, queryCompilationContext);
    }

    protected override QueryableMethodTranslatingExpressionVisitor CreateSubqueryVisitor()
        => sqliteQueryableMethodTranslatingExpressionVisitor.CreateSubqueryVisitor_Exposed();

    protected override ShapedQueryExpression? TranslateOrderBy(ShapedQueryExpression source, LambdaExpression keySelector, bool ascending)
        => sqliteQueryableMethodTranslatingExpressionVisitor.TranslateOrderBy_Exposed(source, keySelector, ascending);

    protected override ShapedQueryExpression? TranslateThenBy(ShapedQueryExpression source, LambdaExpression keySelector, bool ascending)
        => sqliteQueryableMethodTranslatingExpressionVisitor.TranslateThenBy_Exposed(source, keySelector, ascending);
}
