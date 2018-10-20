﻿using System;

namespace Rebus.Transports.Showdown.RunAll
{
    class Program
    {
        static void Main()
        {
            RunShowDown.Run(transportKind: TransportKind.FileSystem, receiversCount: 2, readParallelism: 4, numberOfWorkers: 10, isLongRun: false).Wait();
            Console.WriteLine("Showdown complete, press any key to continue....");
            Console.ReadKey();
        }
    }
}
