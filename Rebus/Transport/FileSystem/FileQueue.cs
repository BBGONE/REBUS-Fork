using Rebus.Threading;
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
        const int BACKOFF_MSEC = 500;
        readonly string _inputQueue;
        readonly HashSet<string> _filesCache;
        readonly ConcurrentQueue<string> _filesQueue;
        readonly AsyncBottleneck _exclusivelock;
        readonly QueueRegister _queueRegister;
        readonly object _filesCacheLock = new object();
        private long _lastNoMessage = DateTime.Now.AddHours(-1).Ticks;

        public FileQueue(string inputQueue, QueueRegister queueRegister)
        {
            _inputQueue = inputQueue;
            _queueRegister = queueRegister;
            _filesCache = new HashSet<string>();
            _filesQueue = new ConcurrentQueue<string>();
            _exclusivelock = new AsyncBottleneck(1);
        }


        private bool _TryDequeue(out string fullPath)
        {
            fullPath = null;
            var dirName = _queueRegister.EnsureQueueInitialized(this._inputQueue);
            string fileName;
            lock (this._filesCacheLock)
            {
                if (this._filesQueue.TryDequeue(out fileName))
                {
                    this._filesCache.Remove(fileName);
                    fullPath = Path.Combine(dirName, fileName);
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Gets the full path to the next file to be received or null if no files
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<string> Dequeue(CancellationToken cancellationToken)
        {
            string fullPath = null;
            bool loopAgain = false;
            do
            {
                if (!this._TryDequeue(out fullPath))
                {
                    IDisposable locker = await this._exclusivelock.Enter(cancellationToken);
                    try
                    {
                        // try again, maybe another thread already added items to the queue
                        if (!this._TryDequeue(out fullPath))
                        {
                            TimeSpan lastNoMessage = TimeSpan.FromTicks(DateTime.Now.Ticks - this._lastNoMessage);
                            if (lastNoMessage.TotalMilliseconds > BACKOFF_MSEC)
                            {
                                var dirName = _queueRegister.EnsureQueueInitialized(this._inputQueue);
                                DirectoryInfo info = new DirectoryInfo(dirName);
                                // Important, ToList completes enumeration and allows to hold filesCacheLock shorter
                                var files = info.EnumerateFiles("b*.json").OrderBy(p => p.Name).Take(CACHE_SIZE).ToList();
                                int cnt = 0;

                                foreach (var file in files)
                                {
                                    // other threads can receive while this thread adds files to the cache
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
                        locker.Dispose();
                    }
                }

                // nothing to receive
                if (string.IsNullOrEmpty(fullPath))
                    return null;

                string newFullPath;
                // renaming helps to lock the file from other file processors
                if (FileSystemHelper.RenameToTemp(fullPath, out newFullPath))
                {
                    fullPath = newFullPath;
                    loopAgain = false;
                }
                else
                {
                    // if we can not rename the file then it's gone (probably processed already)
                    loopAgain = true;
                }
            } while (loopAgain);

            return fullPath;
        }
    }
}
