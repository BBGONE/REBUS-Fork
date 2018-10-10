using Rebus.Bus;
using Rebus.Logging;
using Rebus.Pipeline;
using Rebus.TasksCoordinator.Interface;
using Rebus.Transport;
using Rebus.Workers.ThreadPoolBased;

namespace Rebus.TasksCoordinator
{
    public class MessageReaderFactory : IMessageReaderFactory
    {
        readonly ILog _log;
        readonly RebusBus _owningBus;
        readonly ITransport _transport;
        readonly IPipelineInvoker _pipelineInvoker;
        readonly IBackoffStrategy _backoffStrategy;

        public MessageReaderFactory(IRebusLoggerFactory rebusLoggerFactory,
            RebusBus owningBus,
            ITransport transport,
            IPipelineInvoker pipelineInvoker,
            IBackoffStrategy backoffStrategy)
        {
            this._log = rebusLoggerFactory.GetLogger<RebusMessageReader>();
            _transport = transport;
            _pipelineInvoker = pipelineInvoker;
            _owningBus = owningBus;
            _backoffStrategy = backoffStrategy;
        }

        public IMessageReader CreateReader(long taskId, WorkersCoordinator coordinator)
        {
            return new RebusMessageReader(taskId, coordinator, _log, _transport,
                _backoffStrategy, _pipelineInvoker, _owningBus);
        }
    }
}
