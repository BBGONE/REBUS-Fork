using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.Transport.FileSystem
{
    class FileQueue
    {
        const int CACHE_SIZE = 1000;
        const int BACKOFF_MSEC = 1000;
        private int _accessCounter;
        readonly string _inputQueue;
        readonly HashSet<string> _filesCache;
        readonly ConcurrentQueue<string> _filesQueue;
        readonly QueueRegister _queueRegister;
        readonly object _filesCacheLock = new object();
        private long _lastNoMessage = DateTime.Now.AddHours(-1).Ticks;

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
               
                /*
                // Check if the file is defered (skip them)
                if (TransportHelper.GetFileDate(fileName) > DateTime.Now.ToUniversalTime())
                {
                    loopAgain = true;
                }
                */

                if (!TransportHelper.RenameToTempWithLock(tempPath, out fullPath))
                {
                    // this file is used by somebody else (try to get another one)
                    loopAgain = true;
                }
            } while (loopAgain);

            return true;
        }

        private int _TryFillCache(string dirName, CancellationToken cancellationToken)
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
                        this._filesQueue.Enqueue(file.Name);
                        this._filesCache.Add(file.Name);
                        ++cnt;
                    }
                }
                cancellationToken.ThrowIfCancellationRequested();
            }

            return cnt;
        }

        private int _TryFillCacheDefered(string dirName, CancellationToken cancellationToken)
        {
            DirectoryInfo info = new DirectoryInfo(dirName);
            var files = info.EnumerateFiles("d*.json").Where(p=> TransportHelper.GetFileDate(p.Name) <= DateTime.Now.ToUniversalTime()).OrderBy(p => p.Name).Take(CACHE_SIZE).ToList();
            int cnt = 0;

            foreach (var file in files)
            {
                lock (this._filesCacheLock)
                {
                    if (!this._filesCache.Contains(file.Name))
                    {
                        this._filesQueue.Enqueue(file.Name);
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
                        if (lastNoMessage.TotalMilliseconds > BACKOFF_MSEC)
                        {
                            var dirName = _queueRegister.EnsureQueueInitialized(this._inputQueue);
                            int cnt = this._TryFillCache(dirName, cancellationToken);
                            cnt += _TryFillCacheDefered(dirName, cancellationToken);

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
