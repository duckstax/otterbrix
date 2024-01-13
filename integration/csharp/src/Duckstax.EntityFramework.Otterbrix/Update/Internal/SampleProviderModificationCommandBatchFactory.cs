namespace Duckstax.EntityFramework.Otterbrix.Update.Internal;

using Microsoft.EntityFrameworkCore.Update;

internal class SampleProviderModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    public SampleProviderModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
    {
        Dependencies = dependencies;
    }

    /// <summary>
    ///     Relational provider-specific dependencies for this service.
    /// </summary>
    protected virtual ModificationCommandBatchFactoryDependencies Dependencies { get; }

    public virtual ModificationCommandBatch Create()
        => new SingularModificationCommandBatch(Dependencies);
}
