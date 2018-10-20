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

        public async Task Send(int messageCount = 1000)
        {
            try
            {
                var sentMessagesCount = 0;
                Print($"Sending using {_transportKind} {messageCount} messages from sender to receiver");

                var senderBus = (RebusBus)_adapter.Bus;
                var senderWatch = Stopwatch.StartNew();

                // partition the range in 10 parts
                int partionCount = 10;
                var groups = Enumerable.Range(0, messageCount).Select((item, index) => new
                {
                    item,
                    index
                }).GroupBy(group => group.index % partionCount, element => element.item);

                List<Task> tasks = new List<Task>();
                foreach (var group in groups)
                {
                    //Console.WriteLine("Partion: {0}", group.Key);

                    var localGroup = group;
                    // for each partition send messages to the queue in its own task
                    var task = Task.Run(async () =>
                    {
                        int cnt = 0;
                        foreach (var num in localGroup)
                        {
                            ++cnt;
                            var message = new TestMessage { MessageId = num + (localGroup.Key  * partionCount) };
                            await senderBus.SendLocal(message);
                            Interlocked.Increment(ref sentMessagesCount);
                        }

                        Console.WriteLine($"Patrition #{localGroup.Key} Size: {cnt}");
                    });

                    tasks.Add(task);
                }

                // wait until all messages  will be sent
                await Task.WhenAll(tasks);
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
