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
            _transportId = GenerateID();
        }

        static class RandomLetter
        {
            static Random _random = new Random(Guid.NewGuid().GetHashCode());
            public static char GetLetter()
            {
                char[] chars = "abcdefghijklmnopqrstuvwxyz1234567890".ToCharArray();
                int num = _random.Next(0, chars.Length); // Zero to chars.Length -1
                return chars[num];
            }
        }

        private static string GenerateID()
        {
            var charsToRemove = new char[] { '/', '+', '=' };
            var replacement = new char[] { RandomLetter.GetLetter(), RandomLetter.GetLetter(), RandomLetter.GetLetter() };
            string str = Convert.ToBase64String(Guid.NewGuid().ToByteArray()).Substring(0, 22);
            int len = str.Length;
            for (int i = 0; i < charsToRemove.Length; ++i)
            {
                str = str.Replace(charsToRemove[i], replacement[i]);
            }
            return str;
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
