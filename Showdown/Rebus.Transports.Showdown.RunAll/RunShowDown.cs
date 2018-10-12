using Rebus.Activation;
using Rebus.Config;
using Rebus.Transport.FileSystem;
using Rebus.Transport.InMem;
using Rebus.Transports.Showdown.Core;
using System;
using System.Data.SqlClient;

namespace Rebus.Transports.Showdown
{
    public class RunShowDown
    {
        const string QueueName = "test_showdown";
        const string SqlServerConnectionString = "Data Source=.;Initial Catalog=rebus2_test;Integrated Security=True;Connection Timeout=5";

        public static void Run(TransportKind transportKind, int readParallelism, int numberOfWorkers)
        {
            if (transportKind == TransportKind.SqlServer)
                PurgeInputQueue();
            int messageCount = 0;
            switch (transportKind)
            {
                case TransportKind.SqlServer:
                    messageCount = 1000;
                    break;
                case TransportKind.FileSystem:
                    messageCount = 1000;
                    break;
                case TransportKind.InMemory:
                    messageCount = 100000;
                    break;
                default:
                    messageCount = 1000;
                    break;
            }

            Action<IHandlerActivator> configureAdapter = (adapter) => {
                Configure.With(adapter)
                .Logging(l => l.None())
                .Transport(t => {
                    switch (transportKind)
                    {
                        case TransportKind.SqlServer:
                            t.UseSqlServer(SqlServerConnectionString, QueueName);
                            break;
                        case TransportKind.FileSystem:
                            t.UseFileSystem(@"c:\DATA\REBUS\QUEUES", QueueName);
                            break;
                        case TransportKind.InMemory:
                            t.UseInMemoryTransport(new InMemNetwork(), QueueName);
                            break;
                    }
                })
                .Options(o =>
                 {
                     o.SetNumberOfWorkers(0);
                     o.SetMaxReadParallelism(readParallelism);
                 })
                .Start();
            };

           
            using (var runner = new ShowdownRunner(configure: configureAdapter, 
                isLongRun: false, 
                MaxNumberOfWorkers: numberOfWorkers,
                MessageCount: messageCount))
            {
                runner.Run($"{transportKind}").Wait();
            }
        }

        static void PurgeInputQueue()
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
