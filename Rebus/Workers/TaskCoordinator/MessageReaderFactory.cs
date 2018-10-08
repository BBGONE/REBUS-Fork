﻿using Rebus.Bus;
using Rebus.Logging;
using Rebus.Pipeline;
using TasksCoordinator.Interface;
using Rebus.Transport;
using Rebus.Workers.ThreadPoolBased;

namespace TasksCoordinator
{
    public class MessageReaderFactory : IMessageReaderFactory
    {
        readonly ILog _log;
        readonly RebusBus _owningBus;
        readonly ITransport _transport;
        readonly IPipelineInvoker _pipelineInvoker;
        readonly IASyncBackoffStrategy _backoffStrategy;

        public MessageReaderFactory(IRebusLoggerFactory rebusLoggerFactory,
            RebusBus owningBus,
            ITransport transport,
            IPipelineInvoker pipelineInvoker,
            IASyncBackoffStrategy backoffStrategy)
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
