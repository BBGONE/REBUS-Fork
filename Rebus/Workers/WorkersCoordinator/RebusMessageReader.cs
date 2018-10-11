using Rebus.Bus;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Pipeline;
using Rebus.TasksCoordinator.Interface;
using Rebus.Transport;
using Rebus.Workers.ThreadPoolBased;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.TasksCoordinator
{
    public class RebusMessageReader : MessageReader<TransportMessage, ITransactionContext>
    {
        #region Private Fields
        readonly ITransport _transport;
        readonly IPipelineInvoker _pipelineInvoker;
        readonly RebusBus _owningBus;
        readonly IBackoffStrategy _backoffStrategy;
        #endregion

        public RebusMessageReader(long taskId, 
            ITaskCoordinatorAdvanced tasksCoordinator, 
            ILog log, 
            ITransport transport,
            IBackoffStrategy backoffStrategy,
            IPipelineInvoker pipelineInvoker,
            RebusBus owningBus
            ) :
            base(taskId, tasksCoordinator, log)
        {
            _transport = transport;
            _pipelineInvoker = pipelineInvoker;
            _owningBus = owningBus;
            _backoffStrategy = backoffStrategy;
        }

        protected override async Task<TransportMessage> ReadMessage(bool isPrimaryReader, long taskId, CancellationToken token, ITransactionContext context)
        {
            if (context == null)
            {
                throw new ArgumentNullException(nameof(context), "ITransactionContext context == null");
            }
            TransportMessage msg;
            try
            {
                msg = await _transport.Receive(context, token);
            }
            catch (OperationCanceledException)
            {
                // it's fine - just a sign that we are shutting down
                throw;
            }
            catch (Exception exception)
            {
                Log.Warn("An error occurred when attempting to receive the next message: {exception}", exception);
                if (IsPrimaryReader)
                {
                    await _backoffStrategy.WaitErrorAsync(token);
                }
                return null;
            }
            token.ThrowIfCancellationRequested();

            return msg;
        }

        protected override async Task<MessageProcessingResult> DispatchMessage(TransportMessage message, long taskId, CancellationToken token, ITransactionContext context)
        {
            if (context == null)
            {
                throw new ArgumentNullException(nameof(context), "ITransactionContext context == null");
            }
            var stepContext = new IncomingStepContext(message, context);
            await _pipelineInvoker.Invoke(stepContext);
            return new MessageProcessingResult { isRollBack = false };
        }

        protected override async Task<int> DoWork(bool isPrimaryReader, CancellationToken token)
        {
            int cnt = 0;
            TransportMessage message = null;
            TransactionContext context;
            var disposable = await this.Coordinator.WaitReadAsync();
            try
            {
                context = new TransactionContextWithOwningBus(_owningBus);
            }
            catch
            {
                disposable.Dispose();
                throw;
            }

            using (context)
            {
                try
                {
                    message = await this.ReadMessage(isPrimaryReader, this.taskId, token, context).ConfigureAwait(false);
                }
                finally
                {
                    disposable.Dispose();
                }

                cnt = message == null ? 0 : 1;

                if (cnt == 0)
                {
                    context.Dispose();
                    if (isPrimaryReader)
                    {
                        await _backoffStrategy.WaitNoMessageAsync(token);
                    }

                    return 0;
                }

                // Console.WriteLine($"TaskID: {taskId} {IsPrimaryReader} THREAD:{Thread.CurrentThread.ManagedThreadId} TasksCount:{this.Coordinator.TasksCount}");

                _backoffStrategy.Reset();

                this.Coordinator.OnBeforeDoWork(this);
                try
                {
                    await ProcessMessage(context, message);
                }
                finally
                {
                    this.Coordinator.OnAfterDoWork(this);
                }
            } // using Transaction Context
       
            return cnt;
        }

        async Task ProcessMessage(TransactionContext context, TransportMessage transportMessage)
        {
            try
            {
                AmbientTransactionContext.SetCurrent(context);

                var stepContext = new IncomingStepContext(transportMessage, context);
                await _pipelineInvoker.Invoke(stepContext);

                try
                {
                    await context.Complete();
                }
                catch (Exception exception)
                {
                    Log.Error(exception, "An error occurred when attempting to complete the transaction context");
                }
            }
            catch (OperationCanceledException exception)
            {
                context.Abort();

                Log.Error(exception, "Worker was aborted while handling message {messageLabel}", transportMessage.GetMessageLabel());
            }
            catch (Exception exception)
            {
                context.Abort();

                Log.Error(exception, "Unhandled exception while handling message {messageLabel}", transportMessage.GetMessageLabel());
            }
            finally
            {
                AmbientTransactionContext.SetCurrent(null);
            }
        }
    }
}