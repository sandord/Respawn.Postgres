using Xunit;
using Xunit.Sdk;

namespace Respawn.Postgres.IntegrationTests
{
    [XunitTestCaseDiscoverer("Respawn.Postgres.IntegrationTests.SkipOnAppVeyorTestDiscoverer", "Respawn.Postgres.IntegrationTests")]
    public class SkipOnAppVeyorAttribute : FactAttribute
    {
    }
}
