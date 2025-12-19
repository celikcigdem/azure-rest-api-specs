using System;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncPipelineQueueing
{
    /// <summary>
    /// Example demonstrating the usage of PriorityAsyncPipelineQueue.
    /// </summary>
    public class ExampleUsage
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("=== Priority Async Pipeline Queue Example ===\n");

            // Create a queue with max 2 concurrent pipelines
            using var queue = new PriorityAsyncPipelineQueue(maxConcurrentPipelines: 2);

            // Example 1: Basic usage
            await Example1_BasicUsage(queue);
            
            await Task.Delay(1000);
            
            // Example 2: Priority-based execution
            await Example2_PriorityExecution(queue);
            
            await Task.Delay(1000);
            
            // Example 3: Handling exceptions
            await Example3_ExceptionHandling(queue);

            Console.WriteLine("\n=== Examples completed ===");
        }

        /// <summary>
        /// Example 1: Basic usage of the queue.
        /// </summary>
        private static async Task Example1_BasicUsage(PriorityAsyncPipelineQueue queue)
        {
            Console.WriteLine("Example 1: Basic Usage");
            Console.WriteLine("----------------------");

            // Simulate processing a data pipeline
            var success = await queue.EnqueuePipelineAsync(
                CustomerPriority.Normal,
                async (ct) =>
                {
                    Console.WriteLine("  Pipeline started: Processing data...");
                    await Task.Delay(500, ct);
                    Console.WriteLine("  Pipeline completed: Data processed successfully");
                });

            Console.WriteLine($"  Result: {(success ? "Success" : "Failed")}");
            Console.WriteLine();
        }

        /// <summary>
        /// Example 2: Priority-based execution.
        /// </summary>
        private static async Task Example2_PriorityExecution(PriorityAsyncPipelineQueue queue)
        {
            Console.WriteLine("Example 2: Priority-Based Execution");
            Console.WriteLine("------------------------------------");

            var tasks = new[]
            {
                // Start with low priority pipelines to fill capacity
                Task.Run(async () =>
                {
                    Console.WriteLine("  [Low Priority] Customer A: Enqueuing pipeline...");
                    var success = await queue.EnqueuePipelineAsync(
                        CustomerPriority.Low,
                        async (ct) =>
                        {
                            Console.WriteLine("  [Low Priority] Customer A: Pipeline executing...");
                            await Task.Delay(1000, ct);
                            Console.WriteLine("  [Low Priority] Customer A: Pipeline completed");
                        });
                    Console.WriteLine($"  [Low Priority] Customer A: {(success ? "Success" : "Cancelled")}");
                }),
                
                Task.Run(async () =>
                {
                    Console.WriteLine("  [Normal Priority] Customer B: Enqueuing pipeline...");
                    var success = await queue.EnqueuePipelineAsync(
                        CustomerPriority.Normal,
                        async (ct) =>
                        {
                            Console.WriteLine("  [Normal Priority] Customer B: Pipeline executing...");
                            await Task.Delay(800, ct);
                            Console.WriteLine("  [Normal Priority] Customer B: Pipeline completed");
                        });
                    Console.WriteLine($"  [Normal Priority] Customer B: {(success ? "Success" : "Cancelled")}");
                }),
                
                // Wait a bit, then add high priority
                Task.Run(async () =>
                {
                    await Task.Delay(200);
                    Console.WriteLine("  [High Priority] Customer C: Enqueuing pipeline (should preempt)...");
                    var success = await queue.EnqueuePipelineAsync(
                        CustomerPriority.High,
                        async (ct) =>
                        {
                            Console.WriteLine("  [High Priority] Customer C: Pipeline executing...");
                            await Task.Delay(300, ct);
                            Console.WriteLine("  [High Priority] Customer C: Pipeline completed");
                        });
                    Console.WriteLine($"  [High Priority] Customer C: {(success ? "Success" : "Cancelled")}");
                })
            };

            await Task.WhenAll(tasks);
            Console.WriteLine();
        }

        /// <summary>
        /// Example 3: Exception handling in pipelines.
        /// </summary>
        private static async Task Example3_ExceptionHandling(PriorityAsyncPipelineQueue queue)
        {
            Console.WriteLine("Example 3: Exception Handling");
            Console.WriteLine("------------------------------");

            try
            {
                var success = await queue.EnqueuePipelineAsync(
                    CustomerPriority.Normal,
                    async (ct) =>
                    {
                        Console.WriteLine("  Pipeline started: Processing...");
                        await Task.Delay(200, ct);
                        Console.WriteLine("  Pipeline encountered an error!");
                        throw new InvalidOperationException("Simulated pipeline error");
                    });

                Console.WriteLine($"  Result: {(success ? "Success" : "Failed")}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  Exception caught: {ex.GetType().Name} - {ex.Message}");
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Example showing monitoring of queue state.
        /// </summary>
        public static async Task MonitoringExample()
        {
            Console.WriteLine("Monitoring Example");
            Console.WriteLine("------------------");

            using var queue = new PriorityAsyncPipelineQueue(maxConcurrentPipelines: 3);

            // Start a monitoring task
            var monitoringCts = new CancellationTokenSource();
            var monitoringTask = Task.Run(async () =>
            {
                while (!monitoringCts.Token.IsCancellationRequested)
                {
                    Console.WriteLine($"  Queue Status - Running: {queue.RunningCount}, Queued: {queue.QueuedCount}");
                    await Task.Delay(500, monitoringCts.Token);
                }
            }, monitoringCts.Token);

            // Enqueue several pipelines
            var tasks = new Task[10];
            for (int i = 0; i < 10; i++)
            {
                int taskId = i;
                tasks[i] = queue.EnqueuePipelineAsync(
                    CustomerPriority.Normal,
                    async (ct) =>
                    {
                        await Task.Delay(1000, ct);
                    });
                await Task.Delay(100); // Space them out
            }

            await Task.WhenAll(tasks);
            monitoringCts.Cancel();

            try
            {
                await monitoringTask;
            }
            catch (OperationCanceledException)
            {
                // Expected
            }

            Console.WriteLine("  All pipelines completed");
            Console.WriteLine();
        }
    }
}
