using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.TestUtilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Duckstax.EntityFramework.Otterbrix.FunctionalTests.TestUtilities;

using Duckstax.EntityFramework.Otterbrix.Extensions;
using Duckstax.EntityFramework.Otterbrix.Infrastructure;

public class SampleProviderTestStore : RelationalTestStore
{
    public static SampleProviderTestStore GetOrCreate(string name, bool sharedCache = true)
        => new(name);

    public static SampleProviderTestStore Create(string name, bool sharedCache = true)
        => new(name, shared: false);

    private SampleProviderTestStore(string name, bool shared = true)
        : base(name, shared)
    {

	}

    public override void Clean(DbContext context)
        => context.Database.EnsureClean();

    public override DbContextOptionsBuilder AddProviderOptions(DbContextOptionsBuilder builder)
        => builder.UseotterbrixProvider(builder => AddOptions(builder));


    public static void AddOptions(SampleProviderDbContextOptionsBuilder builder)
    {
        //TODO: Add any options to builder.  Example: https://github.dev/PomeloFoundation/Pomelo.EntityFrameworkCore.MySql/blob/e8377890f1baffbb52eebbce9f5f14687789a83d/test/EFCore.MySql.FunctionalTests/TestUtilities/MySqlTestStore.cs#L118
        //      Other option is to create an method extension to SampleProviderDbContextOptionsBuilder and call it instead of having this method.  Example: https://github.dev/npgsql/efcore.pg/blob/3e4ef719b350f9426c03586884cd75ddf3c3d645/test/EFCore.PG.FunctionalTests/TestUtilities/NpgsqlDbContextOptionsBuilderExtensions.cs#L7
    }
}
