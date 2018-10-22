using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace Rebus.Transport.FileSystem
{
    class FileQueue
    {
        const int CACHE_SIZE = 1000;
        const int DEFER_CACHE_SIZE = 500;
        const int BACKOFF_NOMSG_MSEC = 500;
        const int BACKOFF_DEFERED_MSEC = 5000;
        private int _accessCounter;
        readonly string _inputQueue;
        readonly HashSet<string> _filesCache;
        readonly ConcurrentQueue<string> _filesQueue;
        readonly QueueRegister _queueRegister;
        readonly object _filesCacheLock = new object();
        private long _lastNoMessage = DateTime.Now.AddHours(-12).Ticks;
        private long _lastDefered = DateTime.Now.AddHours(-12).Ticks;

        public FileQueue(string inputQueue, QueueRegister queueRegister)
        {
            _inputQueue = inputQueue;
            _queueRegister = queueRegister;
            _filesCache = new HashSet<string>();
            _filesQueue = new ConcurrentQueue<string>();
            _accessCounter = 0;
        }


        private bool _TryDequeue(out string fullPath)
        {
            fullPath = null;
            var dirName = _queueRegister.EnsureQueueInitialized(this._inputQueue);
            string fileName;
            bool loopAgain = false;
            do
            {
                loopAgain = false;
                fullPath = null;
                fileName = null;
                string tempPath = null;

                lock (this._filesCacheLock)
                {
                    if (this._filesQueue.TryDequeue(out fileName))
                    {
                        tempPath = Path.Combine(dirName, fileName);
                        this._filesCache.Remove(fileName);
                    }
                    else
                    {
                        return false;
                    }

                }
               
               
                // Check if the file is defered (skip them if not ready for processing)
                if (TransportHelper.GetFileUtcDate(fileName) > DateTime.Now.ToUniversalTime())
                {
                    loopAgain = true;
                }
                else if (!TransportHelper.RenameToTempWithLock(tempPath, out fullPath))
                {
                    // this file is used by somebody else (try to get another one)
                    loopAgain = true;
                }
            } while (loopAgain);

            return true;
        }

        private int _TryFillCache(List<string> tempStore, string dirName, CancellationToken cancellationToken)
        {
            DirectoryInfo info = new DirectoryInfo(dirName);
            // Important, ToList completes enumeration and allows to hold filesCacheLock shorter
            var files = info.EnumerateFiles("b*.json").OrderBy(p => p.Name).Take(CACHE_SIZE).ToList();
            int cnt = 0;

            foreach (var file in files)
            {
                lock (this._filesCacheLock)
                {
                    if (!this._filesCache.Contains(file.Name))
                    {
                        tempStore.Add(file.Name);
                        this._filesCache.Add(file.Name);
                        ++cnt;
                    }
                }
                cancellationToken.ThrowIfCancellationRequested();
            }

            return cnt;
        }

        private int _TryFillCacheDefered(List<string> tempStore, string dirName, CancellationToken cancellationToken)
        {
            DirectoryInfo info = new DirectoryInfo(dirName);
            var files = info.EnumerateFiles("d*.json").OrderBy(p => p.Name).Take(DEFER_CACHE_SIZE).ToList();
            int cnt = 0;

            foreach (var file in files)
            {
                lock (this._filesCacheLock)
                {
                    if (!this._filesCache.Contains(file.Name))
                    {
                        tempStore.Add(file.Name);
                        this._filesCache.Add(file.Name);
                        ++cnt;
                    }
                }
                cancellationToken.ThrowIfCancellationRequested();
            }

            return cnt;
        }

        /// <summary>
        /// Gets the full path to the next file to be received or null if no files
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public string Dequeue(CancellationToken cancellationToken)
        {
            string fullPath = null;

            if (!this._TryDequeue(out fullPath))
            {
                // allow only one thread at a time to fill the cache (it is done without locking)
                if (Interlocked.CompareExchange(ref _accessCounter, 1, 0) > 0)
                    return null;
                try
                {
                    // try again, maybe another thread already added items to the queue
                    if (!this._TryDequeue(out fullPath))
                    {
                        TimeSpan lastNoMessage = TimeSpan.FromTicks(DateTime.Now.Ticks - this._lastNoMessage);
                        if (lastNoMessage.TotalMilliseconds > BACKOFF_NOMSG_MSEC)
                        {
                            var dirName = _queueRegister.EnsureQueueInitialized(this._inputQueue);
                            List<string> tempStore = new List<string>();

                            int cnt = this._TryFillCache(tempStore, dirName, cancellationToken);

                            // Don't check it too often
                            TimeSpan lastDefered = TimeSpan.FromTicks(DateTime.Now.Ticks - this._lastDefered);
                            if (lastDefered.TotalMilliseconds > BACKOFF_DEFERED_MSEC)
                            {
                                this._TryFillCacheDefered(tempStore, dirName, cancellationToken);
                                this._lastDefered = DateTime.Now.Ticks;
                            }

                            foreach (string name in tempStore.OrderBy(name => TransportHelper.GetFileUtcDate(name)))
                            {
                                this._filesQueue.Enqueue(name);
                            }

                            if (cnt == 0)
                            {
                                this._lastNoMessage = DateTime.Now.Ticks;
                            }
                            this._TryDequeue(out fullPath);
                        }
                    }
                }
                finally
                {
                    Interlocked.CompareExchange(ref _accessCounter, 0, 1);
                }
            }

            return fullPath;
        }
    }
}
