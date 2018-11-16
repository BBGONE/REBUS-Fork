using System;
using System.Threading.Tasks;

namespace Rebus.TasksCoordinator.Interface
{
    public interface ITaskCoordinatorAdvanced : ITaskCoordinator
    {
        bool StartNewTask();
        bool IsSafeToRemoveReader(IMessageReader reader, bool workDone);
        bool IsPrimaryReader(IMessageReader reader);

        void OnBeforeDoWork(IMessageReader reader);
        void OnAfterDoWork(IMessageReader reader);
        Task<IDisposable> ReadThrottleAsync(bool isPrimaryReader);
        IDisposable ReadThrottle(bool isPrimaryReader);
        bool AsyncReadThrottling { get; }
    }
}
