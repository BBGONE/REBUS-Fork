using Rebus.Activation;
using Rebus.Bus;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Timer = System.Timers.Timer;
using System.Threading.Tasks.Schedulers;

#pragma warning disable 1998

namespace Rebus.Transports.Showdown.Core
{
    public class ShowdownRunner : IDisposable
    {
        const int MessageCount = 1000;
        readonly int MaxNumberOfWorkers;
        private static readonly Task NOOP = Task.FromResult(0);
        // For long running tasks - uses its own thread pool to schedule tasks
        private WorkStealingTaskScheduler _customTaskScheduler;
        // changing it changes the work type to test it on
        private readonly bool IS_LONG_RUN;
        readonly BuiltinHandlerActivator _adapter = new BuiltinHandlerActivator();

        public ShowdownRunner(Action<IHandlerActivator> configure, bool isLongRun = false, int MaxNumberOfWorkers = 10) {
            this.IS_LONG_RUN = isLongRun;
            this.MaxNumberOfWorkers = MaxNumberOfWorkers;
            configure(_adapter);
        }

        #region TASK HANDLERS - DIFFERENT TYPES
        private void CPU_TASK(TestMessage message, int cnt = 5000)
        {
            for (int i = 0; i < cnt; ++i)
            {
                message.Data = System.Text.Encoding.UTF8.GetBytes($@"If you're blue and you don't know where to go to
Why don't you go where harlem flits
Puttin' on the Ritz {cnt}");
            }
        }

        private async Task HandleLongRunMessage(TestMessage message, CancellationToken token = default(CancellationToken))
        { 
            // SHORT SYNCHRONOUS TASK - execute as is on the default thread (from thread pool)
            CPU_TASK(message, 50);
            // IO BOUND ASYNCH TASK - used as is
            await Task.Delay(50);
            // BUT WRAP the LONG SYNCHRONOUS TASK inside the Task which is scheduled on the custom thread pool (to save threadpool threads)
            await Task.Factory.StartNew(() => {
                CPU_TASK(message, 100000);
            }, token, TaskCreationOptions.DenyChildAttach, _customTaskScheduler?? TaskScheduler.Default);
        }

        private async Task HandleShortRunMessage(TestMessage message, CancellationToken token = default(CancellationToken))
        {
            // SHORT SYNCHRONOUS TASK - execute as is on the default thread (from thread pool)
            CPU_TASK(message, 50);
        }
        #endregion

        public async Task Run(string showdownName)
        {
            _customTaskScheduler = new WorkStealingTaskScheduler();
            try
            {
                using (var printTimer = new Timer())
                {
                    var sentMessagesCount = 0;
                    var receivedMessagesCount = 0;
                    printTimer.Interval = 5000;
                    printTimer.Elapsed +=
                        delegate
                        {
                            Print("Sent {0} messages. Received {1} messages.", sentMessagesCount, receivedMessagesCount);
                        };
                    printTimer.Start();

                    Print(@"----------------------------------------------------------------------
Running showdown: {0} ----------------------------------------------------------------------",
                        showdownName);
                    
                    var receivedMessageIds = new ConcurrentDictionary<int, int>();
                    var receivedMessages = 0;

                    Print("Stopping all workers in receiver");
                    var receiverBus = (RebusBus)_adapter.Bus;
                    receiverBus.SetNumberOfWorkers(0);

                    await Task.Delay(TimeSpan.FromSeconds(2));

                    Print("Sending {0} messages from sender to receiver", MessageCount);

                    var senderWatch = Stopwatch.StartNew();

                    await Task.WhenAll(Enumerable.Range(0, MessageCount)
                     .Select(async i =>
                        {
                            var message = new TestMessage { MessageId = i };
                            receivedMessageIds[message.MessageId] = 0;
                            await _adapter.Bus.SendLocal(message);
                            Interlocked.Increment(ref sentMessagesCount);
                        }));

                    var totalSecondsSending = senderWatch.Elapsed.TotalSeconds;
                    Print("Sending {0} messages took {1:0.0} s ({2:0.0} msg/s)",
                        MessageCount, totalSecondsSending, MessageCount / totalSecondsSending);

                    var resetEvent = new ManualResetEvent(false);

                    _adapter.Handle<TestMessage>(async message =>
                    {
                        var result = Interlocked.Increment(ref receivedMessages);
                        if (IS_LONG_RUN)
                            await HandleLongRunMessage(message);
                        else
                            await HandleShortRunMessage(message);
                        if (result == MessageCount)
                        {
                            resetEvent.Set();
                        }
                        Interlocked.Increment(ref receivedMessagesCount);
                    });


                    Print("Starting receiver with {0} workers", MaxNumberOfWorkers);

                    var receiverWatch = Stopwatch.StartNew();
                    receiverBus.Advanced.Workers.SetNumberOfWorkers(MaxNumberOfWorkers);

                    resetEvent.WaitOne();
                    var totalSecondsReceiving = receiverWatch.Elapsed.TotalSeconds;

                    await Task.Delay(TimeSpan.FromSeconds(2));

                    printTimer.Stop();
                    Print("Receiving {0} messages took {1:0.0} s ({2:0.0} msg/s)",
                        MessageCount, totalSecondsReceiving, MessageCount / totalSecondsReceiving);

                }
            }
            catch (Exception e)
            {
                _customTaskScheduler?.Dispose();
                Print("Error: {0}", e);
            }
        }

   
        void Print(string message, params object[] objs)
        {
            Console.WriteLine(message, objs);
        }

        public void Dispose()
        {
            if (_disposing || _disposed) return;

            lock (this)
            {
                if (_disposing || _disposed) return;

                try
                {
                    _disposing = true;
                    _adapter.Dispose();
                }
                finally
                {
                    _disposed = true;
                    _disposing = false;
                }
            }
        }

        bool _disposed;
        bool _disposing;


    }
}
