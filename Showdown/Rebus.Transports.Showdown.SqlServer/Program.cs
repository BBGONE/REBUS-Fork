using Rebus.Activation;
using Rebus.Config;
using Rebus.Transports.Showdown.Core;
using System;
using System.Data.SqlClient;

namespace Rebus.Transports.Showdown.SqlServer
{
    public class Program
    {
        const string QueueName = "test_showdown";
        const string SqlServerConnectionString = "Data Source=.;Initial Catalog=rebus2_test;Integrated Security=True;Connection Timeout=5";
        
        public static void Main()
        {
            Action<IHandlerActivator> configureAdapter = (adapter) => {
                Configure.With(adapter)
                .Logging(l => l.None())
                .Transport(t => t.UseSqlServer(SqlServerConnectionString, QueueName))
                .Options(o =>
                 {
                     o.SetNumberOfWorkers(0);
                     o.SetMaxReadParallelism(4);
                 })
                .Start();
            };

            PurgeInputQueue();

            using (var runner = new ShowdownRunner(configure: configureAdapter, isLongRun: false, MaxNumberOfWorkers: 10))
            {
                runner.Run(typeof(Program).Namespace).Wait();
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
