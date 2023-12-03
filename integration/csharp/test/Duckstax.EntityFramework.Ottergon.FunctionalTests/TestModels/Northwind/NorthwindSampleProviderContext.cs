﻿using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;

namespace Duckstax.EntityFramework.Ottergon.FunctionalTests.TestModels.Northwind;

public class NorthwindSampleProviderContext : NorthwindRelationalContext
{
	public NorthwindSampleProviderContext(DbContextOptions options)
        : base(options)
    {

	}
}