using System;

namespace Rebus.Transports.Showdown.RunAll
{
    class Program
    {
        static void Main()
        {
            RunShowDown.Run(transportKind: TransportKind.InMemory, busCount: 1, readParallelism: 20, numberOfWorkers: 20, isLongRun: false).Wait();
            Console.WriteLine("Showdown complete, press any key to continue....");
            Console.ReadKey();
        }
    }
}
