namespace Duckstax.EntityFramework.Otterbrix.Metadata.Internal;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Sqlite.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Sqlite.Storage.Internal;

public class otterbrixAnnotationProvider : RelationalAnnotationProvider
{
    private readonly SqliteAnnotationProvider sqliteAnnotationProvider;

    /// <summary>
    ///     This is an internal API that supports the Entity Framework Core infrastructure and not subject to
    ///     the same compatibility standards as public APIs. It may be changed or removed without notice in
    ///     any release. You should only use it directly in your code with extreme caution and knowing that
    ///     doing so can result in application failures when updating to a new Entity Framework Core release.
    /// </summary>
    public otterbrixAnnotationProvider(RelationalAnnotationProviderDependencies dependencies)
    : base(dependencies)
    {
        sqliteAnnotationProvider = new(dependencies);
    }

    public override IEnumerable<IAnnotation> For(IRelationalModel model, bool designTime) => sqliteAnnotationProvider.For(model, designTime);

    public override IEnumerable<IAnnotation> For(IColumn column, bool designTime) => sqliteAnnotationProvider.For(column, designTime);

}
