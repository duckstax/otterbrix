namespace Duckstax.EntityFramework.Otterbrix.Infrastructure.Internal;

using Duckstax.EntityFramework.Otterbrix.Extensions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

public class SampleProviderOptionsExtension : RelationalOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public SampleProviderOptionsExtension() 
    {
    }

    public SampleProviderOptionsExtension(SampleProviderOptionsExtension copyFrom) : base(copyFrom)
    {
        //TODO: Copy all option properties of copyFrom into the properties of this class.
    }

    //TODO: Provide readonly properties for each option needed for this provider

    //TODO: Provider a fluent style method to set option property.  Example: WithPostgresVersion() method at https://github.com/npgsql/efcore.pg/blob/main/src/EFCore.PG/Infrastructure/Internal/NpgsqlOptionsExtension.cs#L186

    public override DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    public override void ApplyServices(IServiceCollection services)
        => services.AddEntityFrameworkSampleProvider();

    protected override RelationalOptionsExtension Clone() => new SampleProviderOptionsExtension(this);

    private sealed class ExtensionInfo : RelationalExtensionInfo
    {
        private int? _serviceProviderHash;

        public ExtensionInfo(IDbContextOptionsExtension extension)
            : base(extension)
        {
        }

        public override int GetServiceProviderHashCode()
        {
            if (_serviceProviderHash == null)
            {
                var hashCode = new HashCode();
                hashCode.Add(base.GetServiceProviderHashCode());

                //TODO: Add to hashCode each option property.  Example: https://github.com/npgsql/efcore.pg/blob/main/src/EFCore.PG/Infrastructure/Internal/NpgsqlOptionsExtension.cs#L414
                //  hashCode.Add(Extension.PostgresVersion);

                _serviceProviderHash = hashCode.ToHashCode();
            }

            return _serviceProviderHash.Value;

        }

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            //TODO: Add to debugInfo each option property as a string of its GetHashCode() value.  Example: https://github.com/npgsql/efcore.pg/blob/main/src/EFCore.PG/Infrastructure/Internal/NpgsqlOptionsExtension.cs#L433
            //  debugInfo["ServantSoftware.EntityFrameworkCore.SampleProvider:" + nameof(NpgsqlDbContextOptionsBuilder.SetPostgresVersion)]
            //    = (Extension.PostgresVersion?.GetHashCode() ?? 0).ToString(CultureInfo.InvariantCulture);
        }
    }
}
