using Rebus.Activation;
using Rebus.Bus;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable 1998

namespace Rebus.Transports.Showdown
{
    public class MessageSender : IDisposable
    {
        readonly BuiltinHandlerActivator _adapter;
        readonly string _transportKind;

        public MessageSender(string transportKind, Action<IHandlerActivator> configure)
        {
            _transportKind = transportKind;
            _adapter = new BuiltinHandlerActivator();
            _adapter.Handle<TestMessage>(async message =>
            {
                throw new Exception("Sender must not receive messages");
            });
            configure(_adapter);
        }

        public async Task Send(int messageCount)
        {
            try
            {
                var sentMessagesCount = 0;
                Print($"Sending using {_transportKind} {messageCount} messages from sender to receiver");

                var senderWatch = Stopwatch.StartNew();

                var range = Enumerable.Range(0, messageCount);

                sentMessagesCount =  await SendRangeWithPartitioning(range, messageCount, partionCount: 10);
                // sentMessagesCount = await SendRangeWithLinqSelect(range);

                senderWatch.Stop();

                var totalSecondsSending = senderWatch.Elapsed.TotalSeconds;
                Print("Sending {0} messages took {1:0.0} s ({2:0.0} msg/s)",
                    sentMessagesCount, totalSecondsSending, sentMessagesCount / totalSecondsSending);
            }
            catch (Exception e)
            {
                Print("Error Sending Messages: {0}", e);
            }
        }

        async Task<int> SendRangeWithPartitioning(IEnumerable<int> range, int messageCount, int partionCount = 10)
        {
            var sentMessagesCount = 0;
            var partions = PartitionCollection(range, partionCount: partionCount);
            int partionSize = messageCount / partionCount;

            List<Task> tasks = new List<Task>();
            foreach (var partion in partions)
            {
                //Console.WriteLine("Partion: {0}", group.Key);
                var localPartion = partion;
                // for each partition send messages to the queue in its own task
                var task1 = SendPartition(partion, partionSize);
                var task2 = task1.ContinueWith((t) => {
                    Interlocked.Add(ref sentMessagesCount, t.Result);
                }, TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnRanToCompletion);
                tasks.Add(task2);
            }

            await Task.WhenAll(tasks);
            return sentMessagesCount;
        }

        async Task<int> SendRangeWithLinqSelect(IEnumerable<int> range)
        {
            var sentMessagesCount = 0;

            var tasks = Task.WhenAll(range.Select(async i =>
            {
                var message = new TestMessage { MessageId = i };
                await _adapter.Bus.SendLocal(message);
                Interlocked.Increment(ref sentMessagesCount);
            }));

            // wait until all messages  will be sent
            await Task.WhenAll(tasks);
            return sentMessagesCount;
        }

        IEnumerable<IGrouping<int,T>> PartitionCollection<T>(IEnumerable<T> collection, int partionCount)
        {
            IEnumerable<IGrouping<int, T>> groups = collection.Select((item, index) => new
            {
                item,
                index
            }).GroupBy(group => group.index % partionCount, element => element.item);
            return groups;
        }

        Task<int> SendPartition(IGrouping<int,int> partion, int partionSize)
        {
            var senderBus = (RebusBus)_adapter.Bus;

            int sentMessagesCount = 0;

            var task = Task.Run(async () =>
            {
                int cnt = 0;
                foreach (var num in partion)
                {
                    ++cnt;
                    var message = new TestMessage { MessageId = num + (partion.Key * partionSize) };
                    await senderBus.SendLocal(message);
                    if (cnt % 500 == 0) await Task.Yield();

                    /*
                    Random rand = new Random(Guid.NewGuid().GetHashCode());
                    await senderBus.DeferLocal(TimeSpan.FromSeconds(rand.Next(1, 60)), message);
                    */
                    Interlocked.Increment(ref sentMessagesCount);
                }

                Console.WriteLine($"Patrition #{partion.Key} Size: {cnt} offset: {partion.Key * partionSize}");
                return sentMessagesCount;
            });

            return task;
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
