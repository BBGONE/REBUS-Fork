namespace Rebus.TasksCoordinator.Interface
{
    public interface IMessageReaderFactory
    {
        IMessageReader CreateReader(long taskId, WorkersCoordinator coordinator);
    }
}
