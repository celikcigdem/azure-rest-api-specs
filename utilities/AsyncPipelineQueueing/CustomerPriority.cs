namespace AsyncPipelineQueueing
{
    /// <summary>
    /// Represents the priority level of a customer for pipeline execution.
    /// </summary>
    public enum CustomerPriority
    {
        /// <summary>
        /// Low priority customer - can be blocked or cancelled when capacity is reached.
        /// </summary>
        Low = 0,
        
        /// <summary>
        /// Normal priority customer - standard processing.
        /// </summary>
        Normal = 1,
        
        /// <summary>
        /// High priority customer - gets precedence over lower priority requests.
        /// </summary>
        High = 2
    }
}
