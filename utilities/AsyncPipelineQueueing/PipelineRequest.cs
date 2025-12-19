using System;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncPipelineQueueing
{
    /// <summary>
    /// Represents a request to execute an async pipeline with associated metadata.
    /// </summary>
    public class PipelineRequest
    {
        /// <summary>
        /// Unique identifier for this pipeline request.
        /// </summary>
        public Guid Id { get; }
        
        /// <summary>
        /// Priority of the customer making this request.
        /// </summary>
        public CustomerPriority Priority { get; }
        
        /// <summary>
        /// Timestamp when this request was created.
        /// </summary>
        public DateTime CreatedAt { get; }
        
        /// <summary>
        /// The actual async pipeline function to execute.
        /// </summary>
        public Func<CancellationToken, Task> PipelineFunc { get; }
        
        /// <summary>
        /// Cancellation token source for this pipeline execution.
        /// </summary>
        public CancellationTokenSource CancellationTokenSource { get; }
        
        /// <summary>
        /// Task completion source to signal completion or failure.
        /// </summary>
        public TaskCompletionSource<bool> CompletionSource { get; }
        
        /// <summary>
        /// Counter for how many times this request has been preempted/cancelled.
        /// Used to prevent perpetual cancellation.
        /// </summary>
        public int PreemptionCount { get; set; }
        
        public PipelineRequest(CustomerPriority priority, Func<CancellationToken, Task> pipelineFunc)
        {
            Id = Guid.NewGuid();
            Priority = priority;
            CreatedAt = DateTime.UtcNow;
            PipelineFunc = pipelineFunc ?? throw new ArgumentNullException(nameof(pipelineFunc));
            CancellationTokenSource = new CancellationTokenSource();
            CompletionSource = new TaskCompletionSource<bool>();
            PreemptionCount = 0;
        }
    }
}
