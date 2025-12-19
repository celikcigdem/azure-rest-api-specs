using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncPipelineQueueing
{
    /// <summary>
    /// A priority-based async pipeline queue that manages concurrent execution with:
    /// - Configurable concurrency limits
    /// - Priority-based execution (high priority customers first)
    /// - New demand prioritization
    /// - Low priority pipeline cancellation
    /// - Request blocking for low priority when at capacity
    /// - Fairness mechanism to prevent perpetual cancellation
    /// </summary>
    public class PriorityAsyncPipelineQueue : IDisposable
    {
        private readonly int _maxConcurrentPipelines;
        private readonly int _maxPreemptions;
        private readonly SemaphoreSlim _semaphore;
        private readonly ConcurrentDictionary<Guid, PipelineRequest> _runningPipelines;
        private readonly ConcurrentQueue<PipelineRequest> _waitingQueue;
        private readonly object _lock = new object();
        private bool _disposed;

        /// <summary>
        /// Gets the current number of running pipelines.
        /// </summary>
        public int RunningCount => _runningPipelines.Count;

        /// <summary>
        /// Gets the current number of queued pipelines.
        /// </summary>
        public int QueuedCount => _waitingQueue.Count;

        /// <summary>
        /// Initializes a new instance of the PriorityAsyncPipelineQueue.
        /// </summary>
        /// <param name="maxConcurrentPipelines">Maximum number of pipelines that can run concurrently.</param>
        /// <param name="maxPreemptions">Maximum times a request can be preempted before it's protected from cancellation.</param>
        public PriorityAsyncPipelineQueue(int maxConcurrentPipelines, int maxPreemptions = 2)
        {
            if (maxConcurrentPipelines <= 0)
                throw new ArgumentException("Must be greater than zero", nameof(maxConcurrentPipelines));
            if (maxPreemptions < 0)
                throw new ArgumentException("Must be non-negative", nameof(maxPreemptions));

            _maxConcurrentPipelines = maxConcurrentPipelines;
            _maxPreemptions = maxPreemptions;
            _semaphore = new SemaphoreSlim(maxConcurrentPipelines, maxConcurrentPipelines);
            _runningPipelines = new ConcurrentDictionary<Guid, PipelineRequest>();
            _waitingQueue = new ConcurrentQueue<PipelineRequest>();
        }

        /// <summary>
        /// Enqueues a pipeline for execution with the specified priority.
        /// </summary>
        /// <param name="priority">Priority of the customer making the request.</param>
        /// <param name="pipelineFunc">The async pipeline function to execute.</param>
        /// <param name="cancellationToken">Optional cancellation token for the entire operation.</param>
        /// <returns>Task that completes when the pipeline finishes execution.</returns>
        public async Task<bool> EnqueuePipelineAsync(
            CustomerPriority priority, 
            Func<CancellationToken, Task> pipelineFunc,
            CancellationToken cancellationToken = default)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(PriorityAsyncPipelineQueue));

            var request = new PipelineRequest(priority, pipelineFunc);

            // Check if we're at capacity
            bool atCapacity;
            lock (_lock)
            {
                atCapacity = _runningPipelines.Count >= _maxConcurrentPipelines;
            }

            // If at capacity, try to preempt lower priority pipelines for high priority requests
            if (atCapacity && priority == CustomerPriority.High)
            {
                bool preempted = TryPreemptLowerPriorityPipeline(priority);
                if (preempted)
                {
                    // Space was made, proceed to execute
                    return await ExecutePipelineAsync(request, cancellationToken);
                }
            }

            // If low priority and at capacity, block the request
            if (atCapacity && priority == CustomerPriority.Low)
            {
                // Queue it and wait for capacity
                _waitingQueue.Enqueue(request);
                
                // Wait for space to become available or cancellation
                using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken, request.CancellationTokenSource.Token))
                {
                    try
                    {
                        await request.CompletionSource.Task.WaitAsync(linkedCts.Token);
                        return request.CompletionSource.Task.Result;
                    }
                    catch (OperationCanceledException)
                    {
                        return false;
                    }
                }
            }

            // For normal priority or when not at capacity, queue and process
            if (atCapacity)
            {
                _waitingQueue.Enqueue(request);
                using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken, request.CancellationTokenSource.Token))
                {
                    try
                    {
                        await request.CompletionSource.Task.WaitAsync(linkedCts.Token);
                        return request.CompletionSource.Task.Result;
                    }
                    catch (OperationCanceledException)
                    {
                        return false;
                    }
                }
            }

            // Execute immediately if capacity available
            return await ExecutePipelineAsync(request, cancellationToken);
        }

        /// <summary>
        /// Tries to preempt a lower priority pipeline to make room for a higher priority one.
        /// Respects the preemption limit to prevent perpetual cancellation.
        /// </summary>
        private bool TryPreemptLowerPriorityPipeline(CustomerPriority incomingPriority)
        {
            lock (_lock)
            {
                // Find a suitable pipeline to preempt:
                // 1. Must be lower priority than incoming request
                // 2. Must not have exceeded preemption limit (to prevent perpetual cancellation)
                // 3. Prefer pipelines with fewer preemptions and older ones
                var candidateToPreempt = _runningPipelines.Values
                    .Where(p => p.Priority < incomingPriority && p.PreemptionCount < _maxPreemptions)
                    .OrderBy(p => p.PreemptionCount)
                    .ThenBy(p => p.CreatedAt)
                    .FirstOrDefault();

                if (candidateToPreempt != null)
                {
                    // Cancel the pipeline
                    candidateToPreempt.CancellationTokenSource.Cancel();
                    candidateToPreempt.PreemptionCount++;
                    
                    // Re-queue the preempted pipeline so it can be retried
                    _waitingQueue.Enqueue(candidateToPreempt);
                    
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Executes a pipeline request.
        /// </summary>
        private async Task<bool> ExecutePipelineAsync(PipelineRequest request, CancellationToken cancellationToken)
        {
            await _semaphore.WaitAsync(cancellationToken);

            try
            {
                // Add to running pipelines
                _runningPipelines.TryAdd(request.Id, request);

                // Execute the pipeline
                using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken, request.CancellationTokenSource.Token))
                {
                    try
                    {
                        await request.PipelineFunc(linkedCts.Token);
                        request.CompletionSource.TrySetResult(true);
                        return true;
                    }
                    catch (OperationCanceledException)
                    {
                        request.CompletionSource.TrySetResult(false);
                        return false;
                    }
                    catch (Exception ex)
                    {
                        request.CompletionSource.TrySetException(ex);
                        return false;
                    }
                }
            }
            finally
            {
                // Remove from running pipelines
                _runningPipelines.TryRemove(request.Id, out _);
                _semaphore.Release();

                // Process next item in queue with priority ordering
                _ = Task.Run(() => ProcessNextQueuedRequest());
            }
        }

        /// <summary>
        /// Processes the next queued request based on priority and newness.
        /// Prioritizes: 1) High priority 2) Newer requests 3) Requests with fewer preemptions
        /// </summary>
        private async Task ProcessNextQueuedRequest()
        {
            if (_waitingQueue.IsEmpty)
                return;

            PipelineRequest? nextRequest = null;

            lock (_lock)
            {
                // Gather all waiting requests
                var waitingRequests = new List<PipelineRequest>();
                while (_waitingQueue.TryDequeue(out var req))
                {
                    waitingRequests.Add(req);
                }

                if (waitingRequests.Count == 0)
                    return;

                // Sort by priority (descending), then by creation time (descending for newer first),
                // then by preemption count (ascending to favor less preempted ones)
                nextRequest = waitingRequests
                    .OrderByDescending(r => r.Priority)
                    .ThenByDescending(r => r.CreatedAt)
                    .ThenBy(r => r.PreemptionCount)
                    .First();

                // Re-queue the others
                foreach (var req in waitingRequests.Where(r => r.Id != nextRequest.Id))
                {
                    _waitingQueue.Enqueue(req);
                }
            }

            if (nextRequest != null)
            {
                try
                {
                    var result = await ExecutePipelineAsync(nextRequest, CancellationToken.None);
                    nextRequest.CompletionSource.TrySetResult(result);
                }
                catch (Exception ex)
                {
                    nextRequest.CompletionSource.TrySetException(ex);
                }
            }
        }

        /// <summary>
        /// Disposes of the queue and cancels all pending operations.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            // Cancel all running pipelines
            foreach (var pipeline in _runningPipelines.Values)
            {
                pipeline.CancellationTokenSource.Cancel();
            }

            // Cancel all queued pipelines
            while (_waitingQueue.TryDequeue(out var request))
            {
                request.CancellationTokenSource.Cancel();
                request.CompletionSource.TrySetCanceled();
            }

            _semaphore?.Dispose();
        }
    }
}
