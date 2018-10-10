using System;

namespace Rebus.Config
{
    /// <summary>
    /// Contains additional options for configuring Rebus internals
    /// </summary>
    public class Options
    {
        /// <summary>
        /// This is the default number of workers that will be started, unless <see cref="NumberOfWorkers"/> is set to something else
        /// </summary>
        public const int DefaultNumberOfWorkers = 1;

        /// <summary>
        /// This is the default number of concurrent read operations on queue allowed, unless <see cref="MaxReadParallelism"/> is set to something else
        /// </summary>
        public const int DefaultMaxReadParallelism = 4;

        /// <summary>
        /// This is the default timeout for workers to finish running active handlers, unless <see cref="WorkerShutdownTimeout" /> is set to something else.
        /// </summary>
        /// <value>1 minute per default.</value>
        public static readonly TimeSpan DefaultWorkerShutdownTimeout = TimeSpan.FromMinutes(1);

        /// <summary>
        /// This is the default due timeouts poll interval which will be used unless overridde by <see cref="DueTimeoutsPollInterval"/>
        /// </summary>
        public static readonly TimeSpan DefaultDueTimeoutsPollInterval = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Constructs the options with the default settings
        /// </summary>
        public Options()
        {
            NumberOfWorkers = DefaultNumberOfWorkers;
            MaxReadParallelism = DefaultMaxReadParallelism;
            DueTimeoutsPollInterval = DefaultDueTimeoutsPollInterval;
            WorkerShutdownTimeout = DefaultWorkerShutdownTimeout;
        }

        /// <summary>
        /// Configures maximum the number of tasks. If thread-based workers are used, this is the number of threads that will be created.
        /// </summary>
        public int NumberOfWorkers { get; set; }

        /// <summary>
        /// Configures the maximum parallelism of reading queue allowed.
        /// </summary>
        public int MaxReadParallelism { get; set; }

        /// <summary>
        /// Gets/sets the address to use if an external timeout manager is to be used
        /// </summary>
        public string ExternalTimeoutManagerAddressOrNull { get; set; }

        /// <summary>
        /// Gets/sets the poll interval when checking for due timeouts
        /// </summary>
        public TimeSpan DueTimeoutsPollInterval { get; set; }

        /// <summary>
        /// Gets/sets the maximum timeout for workers to finish running active handlers after being signaled to stop.
        /// </summary>
        public TimeSpan WorkerShutdownTimeout { get; set; }

        /// <summary>
        /// Gets/sets the name of the bus. If this is left unset, bus instances will be named with numbers.
        /// </summary>
        public string OptionalBusName { get; set; }
    }
}