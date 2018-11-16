using Rebus.Bus;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Pipeline;
using Rebus.TasksCoordinator;
using Rebus.Threading;
using Rebus.Transport;
using System;

namespace Rebus.Workers.ThreadPoolBased
{
    /// <summary>
    /// Implementation of <see cref="IWorkersCoordinatorFactory"/> that uses worker threads to do synchronous receive of messages, dispatching
    /// received messages to the threadpool.
    /// </summary>
    public class WorkersCoordinatorFactory : IWorkersCoordinatorFactory
    {
        readonly ITransport _transport;
        readonly IRebusLoggerFactory _rebusLoggerFactory;
        readonly IPipelineInvoker _pipelineInvoker;
        readonly Options _options;
        readonly Func<RebusBus> _busGetter;
        readonly IBackoffStrategy _backoffStrategy;
        readonly ILog _log;

        /// <summary>
        /// Creates the worker factory
        /// </summary>
        public WorkersCoordinatorFactory(ITransport transport, IRebusLoggerFactory rebusLoggerFactory, IPipelineInvoker pipelineInvoker, Options options, Func<RebusBus> busGetter, BusLifetimeEvents busLifetimeEvents, IBackoffStrategy backoffStrategy)
        {
            if (transport == null) throw new ArgumentNullException(nameof(transport));
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            if (pipelineInvoker == null) throw new ArgumentNullException(nameof(pipelineInvoker));
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (busGetter == null) throw new ArgumentNullException(nameof(busGetter));
            if (busLifetimeEvents == null) throw new ArgumentNullException(nameof(busLifetimeEvents));
            if (backoffStrategy == null) throw new ArgumentNullException(nameof(backoffStrategy));
            _transport = transport;
            _rebusLoggerFactory = rebusLoggerFactory;
            _pipelineInvoker = pipelineInvoker;
            _options = options;
            _busGetter = busGetter;
            _backoffStrategy = backoffStrategy;
            _log = _rebusLoggerFactory.GetLogger<WorkersCoordinatorFactory>();
         
            if (_options.MaxReadParallelism < 1)
            {
                throw new ArgumentException($"Max read parallelism is {_options.MaxReadParallelism} which is an invalid value");
            }

            if (options.WorkerShutdownTimeout < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException($"Cannot use '{options.WorkerShutdownTimeout}' as worker shutdown timeout as it");
            }
        }

        /// <summary>
        /// Creates a new workersCoordinator with the given <paramref name="name"/>
        /// </summary>
        public IWorkersCoordinator CreateWorkersCoordinator(string name, int desiredNumberOfWorkers)
        {
            if (name == null) throw new ArgumentNullException(nameof(name));

            var owningBus = _busGetter();
            var readerFactory = new MessageReaderFactory(_rebusLoggerFactory, owningBus, 
                _transport, _pipelineInvoker, _backoffStrategy);
            var coordinator = new WorkersCoordinator(name: name, 
                maxWorkersCount: desiredNumberOfWorkers,
                messageReaderFactory: readerFactory, 
                rebusLoggerFactory: _rebusLoggerFactory, 
                maxReadParallelism: this._options.MaxReadParallelism,
                asyncReadThrottling: this._options.AsyncReadThrottling,
                shutdownTimeout: this._options.WorkerShutdownTimeout
                );
            coordinator.Start();
            return coordinator;
        }

    }
}