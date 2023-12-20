namespace Duckstax.EntityFramework.otterbrix.FunctionalTests.TestUtilities
{
    using Duckstax.EntityFramework.otterbrix.Extensions;
    using Microsoft.EntityFrameworkCore.TestUtilities;
    using Microsoft.Extensions.DependencyInjection;

    public class SampleProviderTestStoreFactory : RelationalTestStoreFactory
    {
        public static SampleProviderTestStoreFactory Instance { get; } = new SampleProviderTestStoreFactory();

        public override TestStore Create(string storeName)
            => SampleProviderTestStore.Create(storeName);

        public override TestStore GetOrCreate(string storeName)
            => SampleProviderTestStore.GetOrCreate(storeName);

        public override IServiceCollection AddProviderServices(IServiceCollection serviceCollection)
            => serviceCollection.AddEntityFrameworkSampleProvider();
    }
}
