using Rebus.Exceptions;
using System;
using System.Collections.Concurrent;
using System.IO;

namespace Rebus.Transport.FileSystem
{
    class QueueRegister
    {
        readonly ConcurrentDictionary<string, string> _initializedQueues;
        readonly string _baseDirectory;
        readonly object _queueInitLock = new object();

        public QueueRegister(string baseDirectory)
        {
            _baseDirectory = baseDirectory;
            _initializedQueues = new ConcurrentDictionary<string, string>();
        }

        public string BaseDirectory => _baseDirectory;

        public string EnsureQueueInitialized(string queueName)
        {
            string directory;
            if (_initializedQueues.TryGetValue(queueName, out directory))
                return directory;

            lock (this._queueInitLock)
            {
                // double check to prevent race
                if (_initializedQueues.TryGetValue(queueName, out directory))
                    return directory;

                directory = GetDirectoryForQueueNamed(queueName);

                if (Directory.Exists(directory))
                {
                    _initializedQueues.TryAdd(queueName, directory);
                    return directory;
                }

                Exception caughtException = null;
                try
                {
                    Directory.CreateDirectory(directory);
                }
                catch (Exception exception)
                {
                    caughtException = exception;
                }

                if (caughtException != null && !Directory.Exists(directory))
                {
                    throw new RebusApplicationException(
                        caughtException, $"Could not initialize directory '{directory}' for queue named '{queueName}'");
                }

                _initializedQueues.TryAdd(queueName, directory);
                return directory;
            }
        }

        public string GetDirectoryForQueueNamed(string queueName)
        {
            return Path.Combine(_baseDirectory, queueName);
        }
    }
}
