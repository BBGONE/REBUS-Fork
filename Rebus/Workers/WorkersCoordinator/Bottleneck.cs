using Rebus.TasksCoordinator.Interface;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Rebus.TasksCoordinator
{
    public class Bottleneck
    {
        readonly SemaphoreSlim _semaphore;

        public Bottleneck(int maxParallelOperationsToAllow) => _semaphore = new SemaphoreSlim(maxParallelOperationsToAllow);

        public async Task<IDisposable> EnterAsync(CancellationToken cancellationToken) 
        {
            await _semaphore.WaitAsync(cancellationToken);

            return new Releaser(_semaphore);
        }

        public IWaitResult Enter(CancellationToken cancellationToken, int timeout = 50)
        {
            bool res = _semaphore.Wait(timeout, cancellationToken);

            return new Releaser(_semaphore, res);
        }

        class Releaser : IWaitResult
        {
            readonly SemaphoreSlim _semaphore;

            public Releaser(SemaphoreSlim semaphore, bool waitResult = true)
            {
                _semaphore = semaphore;
                Result = waitResult;
            }

            public bool Result { get; }
            public void Dispose() => _semaphore.Release();
        }
    }
}