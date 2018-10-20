using System;
using System.Threading;

namespace Rebus.Transport.FileSystem
{
    class FileNameGenerator
    {
        private int _incrementingCounter = 0;
        readonly string _transportId;

        public FileNameGenerator() {
            // Generate unique transport id
            _transportId =  TransportHelper.GenerateID();
        }


        public string GetNextFileName()
        {
            Interlocked.CompareExchange(ref _incrementingCounter, 0, 99999);
            string ticks = TransportHelper.GetTimeTicks();
            string seqnum = Interlocked.Increment(ref _incrementingCounter).ToString().PadLeft(5, '0');
            return $"b{ticks}{seqnum}_{_transportId}.json";
        }
    }
}
