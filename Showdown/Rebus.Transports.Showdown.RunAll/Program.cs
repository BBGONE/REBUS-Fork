using System;

namespace Rebus.Transports.Showdown.RunAll
{
    class Program
    {
        static void Main()
        {
            RunShowDown.Run(transportKind: TransportKind.FileSystem, busCount: 2, readParallelism: 20, numberOfWorkers: 10, isLongRun: false).Wait();
            Console.WriteLine("Showdown complete, press any key to continue....");
            Console.ReadKey();
        }
    }
}
