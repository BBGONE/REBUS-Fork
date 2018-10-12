using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Rebus.Bus;
using Rebus.Exceptions;
using Rebus.Messages;
using Rebus.Threading;
using Rebus.Time;
#pragma warning disable 1998

namespace Rebus.Transport.FileSystem
{
    /// <summary>
    /// Transport implementation that uses the file system to send/receive messages.
    /// </summary>
    public class FileSystemTransport : ITransport, IInitializable, ITransportInspector
    {
        static readonly JsonSerializerSettings SuperSecretSerializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };
        static readonly Encoding FavoriteEncoding = Encoding.UTF8;
        const int BACKOFF_INTERVAL_MSEC = 500;
        readonly Guid _transportId = Guid.NewGuid();

        readonly ConcurrentDictionary<string, object> _messagesBeingHandled = new ConcurrentDictionary<string, object>();
        readonly ConcurrentBag<string> _queuesAlreadyInitialized = new ConcurrentBag<string>();
        readonly string _baseDirectory;
        readonly string _inputQueue;
        readonly ConcurrentQueue<string> _filesQueue;
        private DateTime _lastQueueCheck= DateTime.Now;
        private AsyncBottleneck _queuelock;

        long _incrementingCounter = 0;

        /// <summary>
        /// Constructs the file system transport to create "queues" as subdirectories of the specified base directory.
        /// While it is apparent that <seealso cref="_baseDirectory"/> must be a valid directory name, please note that 
        /// <seealso cref="_inputQueue"/> must not contain any invalid path either.
        /// </summary>
        public FileSystemTransport(string baseDirectory, string inputQueue)
        {
            // Console.WriteLine("Transport created");
            _baseDirectory = baseDirectory;

            if (inputQueue == null) return;

            EnsureQueueNameIsValid(inputQueue);

            _inputQueue = inputQueue;
            _filesQueue = new ConcurrentQueue<string>();
            _queuelock = new AsyncBottleneck(1);
        }

        /// <summary>
        /// Creates a "queue" (i.e. a directory) with the given name
        /// </summary>
        public void CreateQueue(string address)
        {
            EnsureQueueInitialized(address);
        }

        /// <summary>
        /// Sends the specified message to the logical queue specified by <paramref name="destinationQueueName"/> by writing
        /// a JSON serialied text to a file in the corresponding directory. The actual write operation is delayed until
        /// the commit phase of the queue transaction unless we're non-transactional, in which case it is written immediately.
        /// </summary>
        public async Task Send(string destinationQueueName, TransportMessage message, ITransactionContext context)
        {
            EnsureQueueInitialized(destinationQueueName);
            var destinationDirectory = GetDirectoryForQueueNamed(destinationQueueName);

            var serializedMessage = Serialize(message);
            var fileName = GetNextFileName();
            var fullPath = Path.Combine(destinationDirectory, fileName);

            context.OnCommitted(async () =>
            {
                using (var stream = new FileStream(fullPath, FileMode.CreateNew, FileAccess.Write, FileShare.Write, 1024 * 64, true))
                {
                    var bytes = FavoriteEncoding.GetBytes(serializedMessage);
                    await stream.WriteAsync(bytes, 0, bytes.Length);
                }
            });
        }

        private static bool _RenameFile(string fullPath, string newFileName, out string newFilePath)
        {
            newFilePath = string.Empty;
            try
            {
                string dirName = Path.GetDirectoryName(fullPath);
                string fileName = Path.GetFileName(fullPath);
                newFilePath = Path.Combine(dirName, newFileName);
                FileInfo fileInfo = new FileInfo(fullPath);
                fileInfo.MoveTo(newFilePath);
                return true;
            }
            catch (IOException)
            {
                // the file is gone
                newFilePath = string.Empty;
                return false;
            }
        }

        /// <summary>
        /// change the first letter from any to t (means temporary)
        /// </summary>
        /// <param name="fullPath"></param>
        /// <returns></returns>
        private static bool _RenameToTemp(string fullPath, out string newFilePath)
        {
            string fileName = Path.GetFileName(fullPath);
            string newFileName = $"t{fileName.Substring(1)}";
            return _RenameFile(fullPath, newFileName, out newFilePath);
        }

        /// <summary>
        /// change the first letter from any to e (means error)
        /// </summary>
        /// <param name="fullPath"></param>
        /// <returns></returns>
        private static bool _RenameToError(string fullPath, out string newFilePath)
        {
            string fileName = Path.GetFileName(fullPath);
            string newFileName = $"e{fileName.Substring(1)}";
            return _RenameFile(fullPath, newFileName, out newFilePath);
        }

        private string _GetRecievedFilePath(CancellationToken cancellationToken) {
            string fullPath = null;
            bool loopAgain = false;
            do
            {
                if (!this._filesQueue.TryDequeue(out fullPath))
                {
                    using(var locker = this._queuelock.Enter(cancellationToken))
                    {
                        // try again, maybe another thread already added items to the queue
                        if (!this._filesQueue.TryDequeue(out fullPath) && ((DateTime.Now - this._lastQueueCheck).TotalMilliseconds > BACKOFF_INTERVAL_MSEC))
                        {
                            IEnumerable<string> fileNames = Directory.EnumerateFiles(GetDirectoryForQueueNamed(_inputQueue), "b*.json")
                            .OrderBy(f => f).Take(10000);

                            foreach (var name in fileNames)
                            {
                                if (_messagesBeingHandled.TryAdd(name, new object()))
                                    this._filesQueue.Enqueue(name);
                            }

                            this._lastQueueCheck = DateTime.Now;
                            this._filesQueue.TryDequeue(out fullPath);
                        }
                    }
                }

                // nothing to receive
                if (string.IsNullOrEmpty(fullPath))
                    return null;

                string newFullPath;
                if (_RenameToTemp(fullPath, out newFullPath))
                {
                    // remove before changing the fullPath
                    _messagesBeingHandled.TryRemove(fullPath, out var _);
                    fullPath = newFullPath;
                    loopAgain = false;
                }
                else
                {
                    // if we can not rename the file then it's gone (probably processed already)
                    // renaming helps to lock the file from other file processors
                    _messagesBeingHandled.TryRemove(fullPath, out var _);
                    loopAgain = true;
                }
            } while (loopAgain);

            return fullPath;
        }

        /// <summary>
        /// Receives the next message from the logical input queue by loading the next file from the corresponding directory,
        /// deserializing it, deleting it when the transaction is committed.
        /// </summary>
        public async Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
        {
            string fullPath = this._GetRecievedFilePath(cancellationToken);
            if (string.IsNullOrEmpty(fullPath))
                return null;
            var jsonText = await ReadAllText(fullPath);
            var receivedTransportMessage = Deserialize(jsonText);
            if (receivedTransportMessage.Headers.TryGetValue(Headers.TimeToBeReceived, out var timeToBeReceived))
            {
                var maxAge = TimeSpan.Parse(timeToBeReceived);

                var creationTimeUtc = File.GetCreationTimeUtc(fullPath);
                var nowUtc = RebusTime.Now.UtcDateTime;

                var messageAge = nowUtc - creationTimeUtc;

                if (messageAge > maxAge)
                {
                    File.Delete(fullPath);
                    return null;
                }
            }

            context.OnCompleted(async () => File.Delete(fullPath));
            context.OnAborted(async () => _RenameToError(fullPath, out var _));
            return receivedTransportMessage;
        }

        static async Task<string> ReadAllText(string fullPath)
        {
            using (var stream1 = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.None, 1024 * 64, true))
            using (var reader = new StreamReader(stream1, FavoriteEncoding, false, 1024 * 64, true))
            {
                return await reader.ReadToEndAsync();
                
            }
        }

        /// <summary>
        /// Gets the logical input queue name which for this transport correponds to a subdirectory of the specified base directory.
        /// For other transports, this is a global "address", but for this transport the address space is confined to the base directory.
        /// Therefore, the global address is the same as the input queue name.
        /// </summary>
        public string Address => _inputQueue;

        /// <summary>
        /// Ensures that the "queue" is initialized (i.e. that the corresponding subdirectory exists).
        /// </summary>
        public void Initialize()
        {
            if (_inputQueue == null) return;

            EnsureQueueInitialized(_inputQueue);
        }

        /// <summary>
        /// Gets the number of messages waiting in this "queue"
        /// </summary>
        public async Task<Dictionary<string, object>> GetProperties(CancellationToken cancellationToken)
        {
            var count = GetCount(cancellationToken);

            return new Dictionary<string, object>
            {
                {TransportInspectorPropertyKeys.QueueLength, count.ToString()}
            };
        }

        int GetCount(CancellationToken cancellationToken)
        {
            var directoryPath = GetDirectoryForQueueNamed(_inputQueue);
            var files = Directory.EnumerateFiles(directoryPath, "b*.json");
            return files.Aggregate(0, (counter, _) => {
                cancellationToken.ThrowIfCancellationRequested();
                return ++counter;
            });
        }

        string GetNextFileName()
        {
            Interlocked.CompareExchange(ref _incrementingCounter, 0, long.MaxValue);
            return $"b{Interlocked.Increment(ref _incrementingCounter).ToString().PadLeft(20,'0')}_{_transportId}.json";
        }

        void EnsureQueueNameIsValid(string queueName)
        {
            var invalidPathCharactersPresentsInQueueName =
                queueName.ToCharArray()
                    .Intersect(Path.GetInvalidPathChars())
                    .ToList();

            if (!invalidPathCharactersPresentsInQueueName.Any())
                return;

            throw new InvalidOperationException(
                $"Cannot use '{_inputQueue}' as an input queue name because it contains the following invalid characters: {string.Join(", ", invalidPathCharactersPresentsInQueueName.Select(c => $"'{c}'"))}");
        }

        void EnsureQueueInitialized(string queueName)
        {
            if (_queuesAlreadyInitialized.Contains(queueName)) return;

            var directory = GetDirectoryForQueueNamed(queueName);

            if (Directory.Exists(directory)) return;

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

            // if an exception occurred but the directory exists now, it must have been a race... we're good
            _queuesAlreadyInitialized.Add(queueName);
        }

        string GetDirectoryForQueueNamed(string queueName)
        {
            return Path.Combine(_baseDirectory, queueName);
        }

        static string Serialize(TransportMessage message)
        {
            return JsonConvert.SerializeObject(message, SuperSecretSerializerSettings);
        }

        static TransportMessage Deserialize(string serialiedMessage)
        {
            return JsonConvert.DeserializeObject<TransportMessage>(serialiedMessage, SuperSecretSerializerSettings);
        }
    }
}