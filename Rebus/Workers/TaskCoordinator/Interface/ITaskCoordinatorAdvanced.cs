using System;
using System.Threading.Tasks;

namespace TasksCoordinator.Interface
{
    public interface ITaskCoordinatorAdvanced : ITaskCoordinator
    {
        void StartNewTask();
        bool IsSafeToRemoveReader(IMessageReader reader, bool workDone);
        bool IsPrimaryReader(IMessageReader reader);

        void OnBeforeDoWork(IMessageReader reader);
        void OnAfterDoWork(IMessageReader reader);
        Task<IDisposable> WaitReadAsync();
    }
}
