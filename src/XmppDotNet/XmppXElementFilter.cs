namespace XmppDotNet
{
    using System;
    using System.Collections.Concurrent;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Transport;
    using Xml;
    using Xmpp;
    using Xmpp.Base;

    public class XmppXElementFilter
    {
        public static int DefaultTimeout { get; set; } = TimeConstants.TwoMinutes;

        private readonly ConcurrentDictionary<
            Func<XmppXElement, bool>,
            Action<XmppXElement>
        > handlers = new ConcurrentDictionary<Func<XmppXElement, bool>, Action<XmppXElement>>();

        private readonly ITransport transport;

        public XmppXElementFilter(ITransport transport)
        {
            this.transport = transport;

            transport.XmlReceived.Subscribe(el =>
            {
                // Snapshot to avoid issues while handlers are being modified concurrently.
                foreach (var kvp in handlers.ToArray())
                {
                    var predicate = kvp.Key;
                    if (el.IsMatch(predicate))
                    {
                        var action = kvp.Value;

                        // Run handler on threadpool (matches your previous behavior).
                        _ = Task.Run(() => action.Invoke(el));
                    }
                }
            });
        }

        protected XmppXElementFilter Handle<T>(Action<XmppXElement> action)
            where T : XmppXElement
        {
            return Handle(el => el.OfType<T>(), action);
        }

        protected XmppXElementFilter Handle(
            Func<XmppXElement, bool> predicate,
            Action<XmppXElement> action
        )
        {
            if (predicate == null)
                return this;
            Contract.Requires<ArgumentNullException>(action != null, nameof(action));

            if (!handlers.TryAdd(predicate, action))
                throw new ArgumentException(
                    "A handler for the specified predicate is already registered.",
                    nameof(predicate)
                );

            return this;
        }

        protected XmppXElementFilter UnHandle(Func<XmppXElement, bool> predicate)
        {
            if (predicate == null)
                return this;

            _ = handlers.TryRemove(predicate, out _);
            return this;
        }

        #region << SendAsync members >>

        #region << SendAsync XmppXElement members >>
        protected async Task SendAsync(XmppXElement el)
        {
            await transport.SendAsync(el).ConfigureAwait(false);
        }

        public async Task<XmppXElement> SendAsync<T1, T2, T3>(
            XmppXElement el,
            CancellationToken cancellationToken
        )
            where T1 : XmppXElement
            where T2 : XmppXElement
            where T3 : XmppXElement
        {
            return await SendAsync<T1, T2, T3>(el, DefaultTimeout, cancellationToken)
                .ConfigureAwait(false);
        }

        public async Task<XmppXElement> SendAsync<T1, T2, T3>(
            XmppXElement el,
            int timeout,
            CancellationToken cancellationToken
        )
            where T1 : XmppXElement
            where T2 : XmppXElement
            where T3 : XmppXElement
        {
            static bool Predicate(XmppXElement e) => e.OfTypeAny<T1, T2, T3>();
            return await SendAsync<XmppXElement>(
                    () => SendAsync(el),
                    Predicate,
                    timeout,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        public async Task<T> SendAsync<T>(XmppXElement el)
            where T : XmppXElement => await SendAsync<T>(el, DefaultTimeout).ConfigureAwait(false);

        public async Task<T> SendAsync<T>(XmppXElement el, int timeout)
            where T : XmppXElement
        {
            static bool Predicate(XmppXElement e) => e.OfType<T>();
            return await SendAsync<T>(() => SendAsync(el), Predicate, timeout)
                .ConfigureAwait(false);
        }

        public async Task<T> SendAsync<T>(XmppXElement el, CancellationToken cancellationToken)
            where T : XmppXElement =>
            await SendAsync<T>(el, DefaultTimeout, cancellationToken).ConfigureAwait(false);

        public async Task<T> SendAsync<T>(
            XmppXElement el,
            int timeout,
            CancellationToken cancellationToken
        )
            where T : XmppXElement
        {
            static bool Predicate(XmppXElement e) => e.OfType<T>();
            return await SendAsync<T>(() => SendAsync(el), Predicate, timeout, cancellationToken)
                .ConfigureAwait(false);
        }

        public async Task<T> SendAsync<T>(
            XmppXElement el,
            Func<XmppXElement, bool> predicate,
            int timeout,
            CancellationToken cancellationToken
        )
            where T : XmppXElement
        {
            return await SendAsync<T>(() => SendAsync(el), predicate, timeout, cancellationToken)
                .ConfigureAwait(false);
        }

        public async Task<XmppXElement> SendAsync<T1, T2>(XmppXElement el)
            where T1 : XmppXElement
            where T2 : XmppXElement =>
            await SendAsync<T1, T2>(el, DefaultTimeout).ConfigureAwait(false);

        public async Task<XmppXElement> SendAsync<T1, T2>(XmppXElement el, int timeout)
            where T1 : XmppXElement
            where T2 : XmppXElement =>
            await SendAsync<T1, T2>(el, timeout, CancellationToken.None).ConfigureAwait(false);

        public async Task<XmppXElement> SendAsync<T1, T2>(
            XmppXElement el,
            CancellationToken cancellationToken
        )
            where T1 : XmppXElement
            where T2 : XmppXElement =>
            await SendAsync<T1, T2>(el, DefaultTimeout, cancellationToken).ConfigureAwait(false);

        public async Task<XmppXElement> SendAsync<T1, T2>(
            XmppXElement el,
            int timeout,
            CancellationToken cancellationToken
        )
            where T1 : XmppXElement
            where T2 : XmppXElement
        {
            static bool Predicate(XmppXElement e) => e.OfTypeAny<T1, T2>();
            return await SendAsync<XmppXElement>(
                    () => SendAsync(el),
                    Predicate,
                    timeout,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        public async Task<XmppXElement> SendAsync<T1, T2, T3>(XmppXElement el)
            where T1 : XmppXElement
            where T2 : XmppXElement
            where T3 : XmppXElement =>
            await SendAsync<T1, T2, T3>(el, DefaultTimeout).ConfigureAwait(false);

        public async Task<XmppXElement> SendAsync<T1, T2, T3>(XmppXElement el, int timeout)
            where T1 : XmppXElement
            where T2 : XmppXElement
            where T3 : XmppXElement
        {
            static bool Predicate(XmppXElement e) => e.OfTypeAny<T1, T2, T3>();
            return await SendAsync<XmppXElement>(() => SendAsync(el), Predicate, timeout)
                .ConfigureAwait(false);
        }

        public async Task<XmppXElement> SendAsync<T1, T2, T3, T4>(XmppXElement el)
            where T1 : XmppXElement
            where T2 : XmppXElement
            where T3 : XmppXElement
            where T4 : XmppXElement
        {
            static bool Predicate(XmppXElement e) => e.OfTypeAny<T1, T2, T3, T4>();
            return await SendAsync<XmppXElement>(() => SendAsync(el), Predicate, DefaultTimeout)
                .ConfigureAwait(false);
        }

        public async Task<XmppXElement> SendAsync<T1, T2, T3, T4>(XmppXElement el, int timeout)
            where T1 : XmppXElement
            where T2 : XmppXElement
            where T3 : XmppXElement
            where T4 : XmppXElement
        {
            static bool Predicate(XmppXElement e) => e.OfTypeAny<T1, T2, T3, T4>();
            return await SendAsync<XmppXElement>(() => SendAsync(el), Predicate, timeout)
                .ConfigureAwait(false);
        }

        public async Task<XmppXElement> SendAsync<T1, T2, T3, T4, T5>(XmppXElement el)
            where T1 : XmppXElement
            where T2 : XmppXElement
            where T3 : XmppXElement
            where T4 : XmppXElement
            where T5 : XmppXElement
        {
            static bool Predicate(XmppXElement e) => e.OfTypeAny<T1, T2, T3, T4, T5>();
            return await SendAsync<XmppXElement>(() => SendAsync(el), Predicate, DefaultTimeout)
                .ConfigureAwait(false);
        }

        public async Task<XmppXElement> SendAsync<T1, T2, T3, T4, T5>(XmppXElement el, int timeout)
            where T1 : XmppXElement
            where T2 : XmppXElement
            where T3 : XmppXElement
            where T4 : XmppXElement
            where T5 : XmppXElement
        {
            static bool Predicate(XmppXElement e) => e.OfTypeAny<T1, T2, T3, T4, T5>();
            return await SendAsync<XmppXElement>(() => SendAsync(el), Predicate, timeout)
                .ConfigureAwait(false);
        }
        #endregion

        private async Task<T> SendAsync<T>(
            Func<Task> sendTask,
            Func<XmppXElement, bool> predicate,
            int timeout
        )
            where T : XmppXElement
        {
            return await SendAsync<T>(sendTask, predicate, timeout, CancellationToken.None)
                .ConfigureAwait(false);
        }

        public async Task<T> SendAsync<T>(
            Func<Task> sendTask,
            Func<XmppXElement, bool> predicate,
            int timeout,
            CancellationToken cancellationToken
        )
            where T : XmppXElement
        {
            var tcs = new TaskCompletionSource<T>(
                TaskCreationOptions.RunContinuationsAsynchronously
            );

            // Register handler
            Action<XmppXElement> action = xel =>
            {
                // Ensure we only complete once; also remove handler promptly.
                UnHandle(predicate);
                tcs.TrySetResult(xel as T);
            };

            Handle(predicate, action);

            await sendTask().ConfigureAwait(false);

            try
            {
                var completed = await Task.WhenAny(tcs.Task, Task.Delay(timeout, cancellationToken))
                    .ConfigureAwait(false);
                if (completed == tcs.Task)
                    return await tcs.Task.ConfigureAwait(false);

                cancellationToken.ThrowIfCancellationRequested();
            }
            catch (OperationCanceledException oce)
            {
                UnHandle(predicate);
                tcs.TrySetException(oce);
                return await tcs.Task.ConfigureAwait(false);
            }

            // Timeout
            UnHandle(predicate);
            tcs.TrySetException(new TimeoutException());
            return await tcs.Task.ConfigureAwait(false);
        }

        #endregion

        #region << Send Iq >>
        public async Task<T> SendIqAsync<T>(Iq iq)
            where T : Iq => await SendIqAsync<T>(iq, DefaultTimeout).ConfigureAwait(false);

        public async Task<T> SendIqAsync<T>(Iq iq, int timeout)
            where T : Iq =>
            await SendIqAsync<T>(iq, timeout, CancellationToken.None).ConfigureAwait(false);

        public async Task<T> SendIqAsync<T>(Iq iq, int timeout, CancellationToken cancellationToken)
            where T : Iq
        {
            bool Predicate(XmppXElement e) =>
                e.OfType<T>()
                && e.Cast<T>().Id == iq.Id
                && (e.Cast<T>().Type.IsAnyOf(IqType.Error, IqType.Result));

            return await SendAsync<T>(() => SendAsync(iq), Predicate, timeout, cancellationToken)
                .ConfigureAwait(false);
        }
        #endregion
    }
}
