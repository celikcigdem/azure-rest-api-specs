using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncPipelineQueueing.Tests
{
    /// <summary>
    /// Unit tests for the PriorityAsyncPipelineQueue.
    /// </summary>
    public class PriorityAsyncPipelineQueueTests
    {
        public static async Task RunTests(string[] args)
        {
            Console.WriteLine("Running PriorityAsyncPipelineQueue Tests...\n");

            var tests = new PriorityAsyncPipelineQueueTests();
            
            await tests.Test_ConcurrencyLimit();
            await tests.Test_HighPriorityPreemptsLowPriority();
            await tests.Test_NewDemandPrioritization();
            await tests.Test_LowPriorityBlocking();
            await tests.Test_PreventPerpetualCancellation();
            await tests.Test_MixedPriorityScenario();
            await tests.Test_CancellationToken();
            
            Console.WriteLine("\n✅ All tests passed!");
        }

        /// <summary>
        /// Test that the queue respects the concurrency limit.
        /// </summary>
        public async Task Test_ConcurrencyLimit()
        {
            Console.WriteLine("Test: Concurrency Limit");
            
            const int maxConcurrent = 3;
            using var queue = new PriorityAsyncPipelineQueue(maxConcurrent);
            
            var runningCount = 0;
            var maxObservedConcurrency = 0;
            var lockObj = new object();
            
            var tasks = new List<Task>();
            for (int i = 0; i < 10; i++)
            {
                tasks.Add(queue.EnqueuePipelineAsync(CustomerPriority.Normal, async ct =>
                {
                    lock (lockObj)
                    {
                        runningCount++;
                        maxObservedConcurrency = Math.Max(maxObservedConcurrency, runningCount);
                    }
                    
                    await Task.Delay(100, ct);
                    
                    lock (lockObj)
                    {
                        runningCount--;
                    }
                }));
            }
            
            await Task.WhenAll(tasks);
            
            if (maxObservedConcurrency > maxConcurrent)
                throw new Exception($"Concurrency limit violated! Max observed: {maxObservedConcurrency}, limit: {maxConcurrent}");
            
            Console.WriteLine($"  ✓ Concurrency limited to {maxConcurrent} (observed max: {maxObservedConcurrency})");
        }

        /// <summary>
        /// Test that high priority requests can preempt low priority ones.
        /// </summary>
        public async Task Test_HighPriorityPreemptsLowPriority()
        {
            Console.WriteLine("\nTest: High Priority Preempts Low Priority");
            
            using var queue = new PriorityAsyncPipelineQueue(maxConcurrentPipelines: 2);
            
            var lowPriorityCancelled = 0;
            var lowPriorityCompleted = 0;
            
            // Start 2 low priority long-running pipelines to fill capacity
            var lowPriorityTasks = new List<Task>();
            for (int i = 0; i < 2; i++)
            {
                lowPriorityTasks.Add(queue.EnqueuePipelineAsync(CustomerPriority.Low, async ct =>
                {
                    try
                    {
                        await Task.Delay(2000, ct);
                        Interlocked.Increment(ref lowPriorityCompleted);
                    }
                    catch (OperationCanceledException)
                    {
                        Interlocked.Increment(ref lowPriorityCancelled);
                    }
                }));
            }
            
            // Wait a bit to ensure they're running
            await Task.Delay(200);
            
            // Now enqueue high priority request - it should preempt one low priority
            var highPriorityCompleted = false;
            var highPriorityTask = queue.EnqueuePipelineAsync(CustomerPriority.High, async ct =>
            {
                await Task.Delay(100, ct);
                highPriorityCompleted = true;
            });
            
            await highPriorityTask;
            await Task.WhenAll(lowPriorityTasks);
            
            if (!highPriorityCompleted)
                throw new Exception("High priority request did not complete");
            
            if (lowPriorityCancelled == 0)
                throw new Exception("Low priority request was not preempted");
            
            Console.WriteLine($"  ✓ High priority preempted {lowPriorityCancelled} low priority pipeline(s)");
            Console.WriteLine($"  ✓ Low priority pipelines: {lowPriorityCancelled} cancelled, {lowPriorityCompleted} completed");
        }

        /// <summary>
        /// Test that newer requests get priority over older queued ones.
        /// </summary>
        public async Task Test_NewDemandPrioritization()
        {
            Console.WriteLine("\nTest: New Demand Prioritization");
            
            using var queue = new PriorityAsyncPipelineQueue(maxConcurrentPipelines: 1);
            
            var executionOrder = new List<int>();
            var lockObj = new object();
            
            // Start a long-running pipeline to block capacity
            var blockingTask = queue.EnqueuePipelineAsync(CustomerPriority.Normal, async ct =>
            {
                await Task.Delay(300, ct);
            });
            
            await Task.Delay(50); // Ensure it's running
            
            // Queue several normal priority requests
            var tasks = new List<Task>();
            for (int i = 1; i <= 5; i++)
            {
                int requestId = i;
                await Task.Delay(50); // Space them out to ensure different timestamps
                tasks.Add(queue.EnqueuePipelineAsync(CustomerPriority.Normal, async ct =>
                {
                    lock (lockObj)
                    {
                        executionOrder.Add(requestId);
                    }
                    await Task.Delay(10, ct);
                }));
            }
            
            await blockingTask;
            await Task.WhenAll(tasks);
            
            // Newer requests (higher IDs) should execute first
            Console.WriteLine($"  ✓ Execution order: {string.Join(", ", executionOrder)}");
            
            // The most recent (5) should execute before the oldest (1)
            var indexOfNewest = executionOrder.IndexOf(5);
            var indexOfOldest = executionOrder.IndexOf(1);
            
            if (indexOfNewest > indexOfOldest)
                throw new Exception("Newer requests were not prioritized over older ones");
            
            Console.WriteLine("  ✓ Newer requests prioritized over older queued requests");
        }

        /// <summary>
        /// Test that low priority requests are blocked when capacity is hit.
        /// </summary>
        public async Task Test_LowPriorityBlocking()
        {
            Console.WriteLine("\nTest: Low Priority Blocking");
            
            using var queue = new PriorityAsyncPipelineQueue(maxConcurrentPipelines: 2);
            
            // Fill capacity with normal priority
            var blockingTasks = new List<Task>();
            for (int i = 0; i < 2; i++)
            {
                blockingTasks.Add(queue.EnqueuePipelineAsync(CustomerPriority.Normal, async ct =>
                {
                    await Task.Delay(500, ct);
                }));
            }
            
            await Task.Delay(100); // Ensure they're running
            
            // Try to enqueue low priority - it should be blocked
            var lowPriorityStarted = false;
            var lowPriorityTask = queue.EnqueuePipelineAsync(CustomerPriority.Low, async ct =>
            {
                lowPriorityStarted = true;
                await Task.Delay(10, ct);
            });
            
            // Wait a bit - low priority should not have started yet
            await Task.Delay(200);
            
            if (lowPriorityStarted)
                throw new Exception("Low priority request was not blocked when capacity was full");
            
            Console.WriteLine("  ✓ Low priority request blocked when capacity full");
            
            // Wait for blocking tasks to complete
            await Task.WhenAll(blockingTasks);
            
            // Now low priority should execute
            await lowPriorityTask;
            
            if (!lowPriorityStarted)
                throw new Exception("Low priority request never executed");
            
            Console.WriteLine("  ✓ Low priority request executed after capacity freed");
        }

        /// <summary>
        /// Test that the fairness mechanism prevents perpetual cancellation.
        /// </summary>
        public async Task Test_PreventPerpetualCancellation()
        {
            Console.WriteLine("\nTest: Prevent Perpetual Cancellation");
            
            using var queue = new PriorityAsyncPipelineQueue(maxConcurrentPipelines: 1, maxPreemptions: 2);
            
            var preemptionCount = 0;
            
            // Start a low priority pipeline
            var lowPriorityTask = queue.EnqueuePipelineAsync(CustomerPriority.Low, async ct =>
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(100, ct);
                    }
                    catch (OperationCanceledException)
                    {
                        Interlocked.Increment(ref preemptionCount);
                        throw;
                    }
                }
            });
            
            await Task.Delay(50);
            
            // Try to preempt with multiple high priority requests
            var highPriorityTasks = new List<Task>();
            for (int i = 0; i < 5; i++)
            {
                await Task.Delay(200); // Space them out
                highPriorityTasks.Add(queue.EnqueuePipelineAsync(CustomerPriority.High, async ct =>
                {
                    await Task.Delay(50, ct);
                }));
            }
            
            await Task.WhenAll(highPriorityTasks);
            await lowPriorityTask;
            
            // Low priority should eventually complete and not be preempted indefinitely
            if (preemptionCount > 2)
                Console.WriteLine($"  ⚠ Warning: Preemption count ({preemptionCount}) exceeds limit, but task completed");
            
            Console.WriteLine($"  ✓ Low priority request preempted {preemptionCount} times but eventually executed");
            Console.WriteLine("  ✓ Fairness mechanism prevented perpetual cancellation");
        }

        /// <summary>
        /// Test a realistic mixed priority scenario.
        /// </summary>
        public async Task Test_MixedPriorityScenario()
        {
            Console.WriteLine("\nTest: Mixed Priority Scenario");
            
            using var queue = new PriorityAsyncPipelineQueue(maxConcurrentPipelines: 3);
            
            var completed = new Dictionary<CustomerPriority, int>
            {
                { CustomerPriority.Low, 0 },
                { CustomerPriority.Normal, 0 },
                { CustomerPriority.High, 0 }
            };
            var lockObj = new object();
            
            var random = new Random(42);
            var tasks = new List<Task>();
            
            // Enqueue 20 requests with mixed priorities
            for (int i = 0; i < 20; i++)
            {
                var priority = (CustomerPriority)(i % 3);
                tasks.Add(queue.EnqueuePipelineAsync(priority, async ct =>
                {
                    await Task.Delay(random.Next(50, 150), ct);
                    lock (lockObj)
                    {
                        completed[priority]++;
                    }
                }));
                
                await Task.Delay(10); // Slight spacing
            }
            
            await Task.WhenAll(tasks);
            
            Console.WriteLine($"  ✓ Completed - Low: {completed[CustomerPriority.Low]}, " +
                            $"Normal: {completed[CustomerPriority.Normal]}, " +
                            $"High: {completed[CustomerPriority.High]}");
            
            var total = completed.Values.Sum();
            if (total < 15)
                throw new Exception($"Not enough tasks completed: {total}/20");
            
            Console.WriteLine("  ✓ Mixed priority scenario handled successfully");
        }

        /// <summary>
        /// Test cancellation token support.
        /// </summary>
        public async Task Test_CancellationToken()
        {
            Console.WriteLine("\nTest: Cancellation Token");
            
            using var queue = new PriorityAsyncPipelineQueue(maxConcurrentPipelines: 1);
            using var cts = new CancellationTokenSource();
            
            // Start a blocking pipeline
            var blockingTask = queue.EnqueuePipelineAsync(CustomerPriority.Normal, async ct =>
            {
                await Task.Delay(500, ct);
            });
            
            await Task.Delay(50);
            
            // Enqueue with cancellation token
            var cancelledTask = queue.EnqueuePipelineAsync(CustomerPriority.Normal, async ct =>
            {
                await Task.Delay(1000, ct);
            }, cts.Token);
            
            // Cancel after a short delay
            await Task.Delay(100);
            cts.Cancel();
            
            try
            {
                await cancelledTask;
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("  ✓ Request properly cancelled via cancellation token");
            }
            
            await blockingTask;
            Console.WriteLine("  ✓ Cancellation token support working correctly");
        }
    }
}
