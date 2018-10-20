using Rebus.Activation;
using Rebus.Config;
using Rebus.Transport.FileSystem;
using Rebus.Transport.InMem;
using System;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Rebus.Transports.Showdown
{
    public class RunShowDown
    {
        const string QueueName = "test_showdown";
        const string SqlServerConnectionString = "Data Source=.;Initial Catalog=rebus2_test;Integrated Security=True;Connection Timeout=5";
        private static InMemNetwork inMemNetwork;

        public static async Task Run(TransportKind transportKind, int receiversCount = 3, int readParallelism = 4, int numberOfWorkers = 10, bool isLongRun = false)
        {
            int messageCount = _GetMeassageCount(transportKind);

            if (transportKind == TransportKind.SqlServer)
            {
                _PurgeInputQueue();
            }
            if (transportKind == TransportKind.InMemory)
            {
                 inMemNetwork = new InMemNetwork();
            }

            await _SendMessages(transportKind, messageCount);
            await _ReceiveMessages(transportKind, receiversCount, readParallelism, numberOfWorkers, messageCount, isLongRun);
        }

        static async Task _SendMessages(TransportKind transportKind, int messageCount)
        {
            Action<IHandlerActivator> configureSenderAdapter = _GetConfigureAdapterCallBack(transportKind, 1, 0);

            using (var sender = new MessageSender(transportKind.ToString(), configure: configureSenderAdapter))
            {
                await sender.Send(messageCount);
            }
        }

        static async Task _ReceiveMessages(TransportKind transportKind, int receiversCount, int readParallelism, int numberOfWorkers, int messageCountToReceive, bool isLongRun)
        {
            Action<IHandlerActivator> configureReceiverAdapter = _GetConfigureAdapterCallBack(transportKind, readParallelism, 0);
            MessageReceiver.totalReceivedCount = 0;
            MessageReceiver[] messageReceivers = new MessageReceiver[receiversCount];
            Task[] receiveTasks = new Task[receiversCount];
            var tcs = new TaskCompletionSource<bool>();
            for (int i = 0; i < receiversCount; ++i)
            {
                string name = $"{transportKind}:#{i + 1}";
                messageReceivers[i] = new MessageReceiver(name, configure: configureReceiverAdapter,
                               maxNumberOfWorkers: numberOfWorkers,
                               messageCount: messageCountToReceive,
                               tcs: tcs,
                               isLongRun: isLongRun);
            }

            try
            {
                for (int i = 0; i < receiversCount; ++i)
                {
                    receiveTasks[i] = messageReceivers[i].Receive();
                }

                await Task.WhenAll(receiveTasks);
                await Task.Delay(1000);
            }
            finally
            {

                for (int i = 0; i < receiversCount; ++i)
                {
                    try
                    {
                        messageReceivers[i].Dispose();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error disposing messageRecever: {ex.Message}");
                    }
                }
            }
        }


        static Action<IHandlerActivator> _GetConfigureAdapterCallBack(TransportKind transportKind, int readParallelism, int numberOfWorkers)
        {
           return (adapter) => {
               var rebusConfigurer = Configure.With(adapter);

               rebusConfigurer.Logging(l => l.None());

               rebusConfigurer.Transport(t => {
                   switch (transportKind)
                   {
                       case TransportKind.SqlServer:
                           t.UseSqlServer(SqlServerConnectionString, QueueName);
                           break;
                       case TransportKind.FileSystem:
                           t.UseFileSystem(@"c:\DATA\REBUS\QUEUES\", QueueName);
                           break;
                       case TransportKind.InMemory:
                           t.UseInMemoryTransport(inMemNetwork, QueueName);
                           break;
                   }
               });

               rebusConfigurer.Options(o =>
                {
                    o.SetNumberOfWorkers(numberOfWorkers);
                    o.SetMaxReadParallelism(readParallelism);
                });

               rebusConfigurer.Start();
            };
        }

        static int _GetMeassageCount(TransportKind transportKind)
        {
            int messageCount = 0;
            switch (transportKind)
            {
                case TransportKind.SqlServer:
                    messageCount = 5000;
                    break;
                case TransportKind.FileSystem:
                    messageCount = 5000;
                    break;
                case TransportKind.InMemory:
                    messageCount = 200000;
                    break;
                default:
                    messageCount = 1000;
                    break;
            }
            return messageCount;
        }

        static void _PurgeInputQueue()
        {
            using (var connection = new SqlConnection(SqlServerConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"IF (OBJECT_ID('[{QueueName}]', 'U') IS NOT NULL) DROP TABLE [{QueueName}]";
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
