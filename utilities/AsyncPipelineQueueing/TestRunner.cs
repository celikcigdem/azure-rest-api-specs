using System;
using System.Threading.Tasks;
using AsyncPipelineQueueing.Tests;

namespace AsyncPipelineQueueing
{
    public class TestRunner
    {
        public static async Task Main(string[] args)
        {
            await PriorityAsyncPipelineQueueTests.RunTests(args);
        }
    }
}
