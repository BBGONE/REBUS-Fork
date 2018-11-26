using Rebus.Handlers;
using Rebus.Transport;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
// ReSharper disable UnusedTypeParameter

namespace Rebus.Pipeline.Receive
{
    /// <summary>
    /// Wrapper of the handler that is ready to invoke
    /// </summary>
    public abstract class HandlerInvoker
    {        
        static readonly ConcurrentDictionary<string, bool> CanBeInitiatedByCache = new ConcurrentDictionary<string, bool>();

           /// <summary>
        /// Key under which the handler invoker will stash itself in the <see cref="ITransactionContext.Items"/>
        /// during the invocation of the wrapped handler
        /// </summary>
        public const string CurrentHandlerInvokerItemsKey = "current-handler-invoker";

        /// <summary>
        /// Method to call in order to invoke this particular handler
        /// </summary>
        public abstract Task Invoke();

        /// <summary>
        /// Marks this handler as one to skip, i.e. calling this method will make the invoker ignore the call to <see cref="Invoke"/>
        /// </summary>
        public abstract void SkipInvocation();

        /// <summary>
        /// Gets the contained handler object (which is probably an implementation of <see cref="IHandleMessages"/>, but you should
        /// not depend on it!)
        /// </summary>
        public abstract object Handler { get; }

		/// <summary>
		/// <c>true</c> if the handler will be invoked, as per normal, <c>false</c> if <see cref="SkipInvocation"/> has been called or the invoke will otherwise be skipped
		/// </summary>
		public abstract bool WillBeInvoked { get; }
    }

    /// <summary>
    /// Derivation of the <see cref="HandlerInvoker"/> that has the message type
    /// </summary>
    public class HandlerInvoker<TMessage> : HandlerInvoker
    {
        readonly Func<Task> _action;
        readonly object _handler;
        readonly ITransactionContext _transactionContext;
        bool _invokeHandler = true;

        /// <summary>
        /// Constructs the invoker
        /// </summary>
        public HandlerInvoker(Func<Task> action, object handler, ITransactionContext transactionContext)
        {
            _action = action ?? throw new ArgumentNullException(nameof(action));
            _handler = handler ?? throw new ArgumentNullException(nameof(handler));
            _transactionContext = transactionContext ?? throw new ArgumentNullException(nameof(transactionContext));
        }

        /// <summary>
        /// Gets the contained handler object
        /// </summary>
        public override object Handler => _handler;


	    /// <inheritdoc />
	    public override bool WillBeInvoked => _invokeHandler;

        /// <summary>
        /// Invokes the handler within this handler invoker
        /// </summary>
        public override async Task Invoke()
        {
            if (!_invokeHandler) return;

            try
            {
                _transactionContext.Items[CurrentHandlerInvokerItemsKey] = this;

                await _action();
            }
            finally
            {
                _transactionContext.Items.TryRemove(CurrentHandlerInvokerItemsKey, out _);
            }
        }

        /// <summary>
        /// Marks this handler invoker to skip its invocation, causing it to do nothin when <see cref="Invoke"/> is called
        /// </summary>
        public override void SkipInvocation() => _invokeHandler = false;
    }
}