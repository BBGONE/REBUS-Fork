using Rebus.Activation;
using Rebus.Bus;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Schedulers;
using Timer = System.Timers.Timer;

#pragma warning disable 1998

namespace Rebus.Transports.Showdown
{
    public class MessageReceiver : IDisposable
    {
        readonly int MaxNumberOfWorkers;
        private static readonly Task NOOP = Task.FromResult(0);
        // For long running tasks - uses its own thread pool to schedule tasks
        private static WorkStealingTaskScheduler _customTaskScheduler = new WorkStealingTaskScheduler();
        // changing it changes the work type to test it on
        private readonly bool _isLongRun;
        private TaskCompletionSource<bool> _tcs;
        // receive by this receiver
        private volatile int receivedCount = 0;
        // receive by ALL receivers
        public static volatile int totalReceivedCount = 0;
        readonly BuiltinHandlerActivator _adapter;
        readonly string name;

        public MessageReceiver(string name, Action<IHandlerActivator> configure, 
            int maxNumberOfWorkers, 
            int messageCount,
            TaskCompletionSource<bool> tcs,
            bool isLongRun = false
            )
        {
            this.name = name;
            this._isLongRun = isLongRun;
            this.MaxNumberOfWorkers = maxNumberOfWorkers;
            this._tcs = tcs;
            _adapter = new BuiltinHandlerActivator();
            _adapter.Handle<TestMessage>(async message =>
            {
                // Console.WriteLine($"Name: {name} ManagedThreadId: {Thread.CurrentThread.ManagedThreadId} isLongRun: {isLongRun}");
                Interlocked.Increment(ref receivedCount);
                int total = Interlocked.Increment(ref totalReceivedCount);
                if (_isLongRun)
                    await HandleLongRunMessage(message);
                else
                    await HandleShortRunMessage(message);
                if (total == messageCount)
                {
                    var task = Task.Run(() =>
                    {
                        this._tcs.TrySetResult(true);
                    });
                }
            });
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
            // VERY SHORT SYNCHRONOUS TASK - execute as is on the default thread (from thread pool)
            CPU_TASK(message, 1);
        }
        #endregion

        public async Task Receive()
        {
            try
            {
                using (var printTimer = new Timer())
                {
                    Print(@"----------------------------------------------------------------------
Running Receive: {0} ----------------------------------------------------------------------", name);

                    var receiverBus = (RebusBus)_adapter.Bus;
                    receiverBus.SetNumberOfWorkers(MaxNumberOfWorkers);
                    Print("Starting receiver with {0} workers", MaxNumberOfWorkers);

                    var receiverWatch = Stopwatch.StartNew();
                    printTimer.Interval = 5000;
                    printTimer.Elapsed += delegate
                    {
                        var _timeReceiving = receiverWatch.Elapsed.TotalMilliseconds;
                        Print($"{name} TotalReceived: {totalReceivedCount} BusReceived:  {receivedCount} time: {_timeReceiving:0.0} ms");
                    };
                    printTimer.Start();

                    /*
                    var receiverBus = (RebusBus)_adapter.Bus;
                    receiverBus.Advanced.Workers.SetNumberOfWorkers(MaxNumberOfWorkers);
                    */


                    await _tcs.Task;

                    receiverWatch.Stop();
                    var timeReceiving = receiverWatch.Elapsed.TotalMilliseconds;
                    Print($"{name} TotalReceived: {totalReceivedCount} BusReceived:  {receivedCount} time: {timeReceiving / 1000:0.0} sec speed: {(totalReceivedCount / timeReceiving) * 1000:0.00} msg/sec");
                }
            }
            catch (Exception e)
            {
                Print("Error Receiving messages: {0}", e);
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
