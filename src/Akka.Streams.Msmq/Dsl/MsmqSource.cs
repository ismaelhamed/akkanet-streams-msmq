using System;
using System.Messaging;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Util;

namespace Akka.Streams.Msmq
{
    public static class MsmqSource
    {
        /// <summary>
        /// Creates a <see cref="Source"/> that receives messages available in the queue, or wait until there
        /// are messages available in the queue, and then emits stream elements of <see cref="Message"/>.
        /// <para>
        /// Depending on whether the queue is transactional or not, this method could behave differently:
        /// <list type="bullet">
        ///   <item>
        ///     <term>Transactional</term>
        ///     <description>Receives each message as a single internal transaction.</description>
        ///   </item>
        ///   <item>
        ///     <term>Non-transactional</term>
        ///     <description>Receives each message outside of a transaction context.</description>
        ///   </item>
        /// </list>
        /// </para>
        /// <para>
        /// Because the internal transaction is committed as soon as the message is emitted downstream,
        /// or there is no transaction whatsoever, this source will only achieve `At-Most-Once` semantics. 
        /// </para>
        /// <para>
        /// This method uses <see cref="MessageQueueSettings.WaitTimeout"/>, so the application might block
        /// the current thread indefinitely if an infinite timeout is specified.
        /// </para>        
        /// <para>
        /// If this method is called on a transactional queue, the message that is received would be returned
        /// to the queue if the transaction is aborted. The message is not permanently removed from the queue until
        /// the transaction is committed.
        /// </para>
        /// </summary>
        /// <param name="settings">Settings to configure the <see cref="MessageQueue"/>.</param> 
        /// <param name="queuePath">The location of the queue referenced by the underlying <see cref="MessageQueue"/>.
        /// The syntax for <paramref name="queuePath"/> depends on the type of queue it points to, as shown in the following
        /// <see href="https://learn.microsoft.com/en-us/dotnet/api/system.messaging.messagequeue.path">table</see>.
        /// </param>
        public static Source<Message, Task<NotUsed>> Default(MessageQueueSettings settings, string queuePath) =>
            Source.Lazily(() =>
            {
                var queue = ResolveMessageQueue(queuePath, settings);
                return Source.Repeat(1)
                    .SelectAsync(1, async _ =>
                    {
                        Message msg = null;
                        try
                        {
                            await Task.Factory.FromAsync(queue.BeginPeek(settings.WaitTimeout), queue.EndPeek)
                                .ConfigureAwait(false);

                            msg = queue.Receive(settings.WaitTimeout, queue.Transactional
                                ? MessageQueueTransactionType.Single
                                : MessageQueueTransactionType.None);
                        }
                        catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                        {
                            // ignore
                        }
                        catch (Exception ex)
                        {
                            // ideally we would only need to do this in case of `MessageQueueErrorCode.StaleHandle`
                            if (ex is MessageQueueException)
                                queue.Close();

                            throw;
                        }

                        return msg ?? Option<Message>.None;
                    })
                    .Collect(option => option.HasValue, option => option.Value);
            });

        /// <summary>
        /// Creates a <see cref="Source"/> that receives messages available in the queue using a
        /// <see cref="MessageQueueTransaction"/>, or wait until there are messages available in the queue,
        /// and then emits stream elements of <see cref="ICommittableMessage"/>.
        /// <para>
        /// This source is useful to achieve `At-Least-Once` semantics, but if you commit the transaction before
        /// processing the message you get `At-Most-Once` semantics.
        /// </para>
        /// <para>
        /// This method uses <see cref="MessageQueueSettings.WaitTimeout"/>, so the application might
        /// block the current thread indefinitely if an infinite timeout is specified.
        /// </para>
        /// <para>
        /// Because this method is called on a transactional queue, the message that is received would be returned
        /// to the queue if the transaction is aborted. The message is not permanently removed from the queue until
        /// the transaction is committed.
        /// </para>
        /// </summary>
        /// <param name="settings">Settings to configure the <see cref="MessageQueue"/>.</param> 
        /// <param name="queuePath">The location of the queue referenced by the underlying <see cref="MessageQueue"/>.
        /// The syntax for <paramref name="queuePath"/> depends on the type of queue it points to, as shown in the following
        /// <see href="https://learn.microsoft.com/en-us/dotnet/api/system.messaging.messagequeue.path">table</see>.
        /// </param>          
        public static Source<ICommittableMessage, Task<NotUsed>> Committable(MessageQueueSettings settings, string queuePath) =>
            Source.Lazily(() =>
            {
                var queue = ResolveMessageQueue(queuePath, settings);
                return Source.Repeat(1)
                    .SelectAsync(1, async _ =>
                    {
                        Message msg = null;
                        MessageQueueTransaction trx = null;
                        try
                        {
                            await Task.Factory.FromAsync(queue.BeginPeek(settings.WaitTimeout), queue.EndPeek)
                                .ConfigureAwait(false);

                            trx = new MessageQueueTransaction();
                            trx.Begin();
                            msg = queue.Receive(settings.WaitTimeout, trx);
                            return new CommittableMessage(msg, trx);
                        }
                        catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                        {
                            trx?.Abort();
                            trx?.Dispose();
                        }
                        catch (Exception ex)
                        {
                            trx?.Abort();
                            trx?.Dispose();

                            // ideally we would only need to do this in case of `MessageQueueErrorCode.StaleHandle`
                            if (ex is MessageQueueException)
                                queue.Close();

                            throw;
                        }

                        return Option<ICommittableMessage>.None;
                    })
                    .Collect(option => option.HasValue, option => option.Value);
            });

        /// <summary>
        /// Creates a <see cref="Source"/> that receives messages available in the queue using a
        /// <see cref="MessageQueueTransaction"/>, or wait until there are messages in the queue,
        /// and then emits stream elements of (<see cref="Message"/>, <see cref="MessageQueueTransaction"/>).
        /// <para>
        /// This source is intended to be used with <seealso cref="MsmqFlow.WithTransactionContext"/>
        /// to achieve `Exactly-Once` semantics.
        /// </para>
        /// <para>
        /// This method uses <see cref="MessageQueueSettings.WaitTimeout"/>, so the application might block
        /// the current thread indefinitely if an infinite timeout is specified.
        /// </para>
        /// <para>
        /// Because this method is called on a transactional queue, the message that is received would be returned
        /// to the queue if the transaction is aborted. The message is not permanently removed from the queue until
        /// the transaction is committed.
        /// </para>
        /// </summary>
        /// <param name="settings">Settings to configure the <see cref="MessageQueue"/>.</param> 
        /// <param name="queuePath">The location of the queue referenced by the underlying <see cref="MessageQueue"/>.
        /// The syntax for <paramref name="queuePath"/> depends on the type of queue it points to, as shown in the following
        /// <see href="https://learn.microsoft.com/en-us/dotnet/api/system.messaging.messagequeue.path">table</see>.
        /// </param> 
        public static SourceWithContext<Message, MessageQueueTransaction, Task<NotUsed>> WithTransactionContext(MessageQueueSettings settings, string queuePath) =>
            Source.Lazily(() =>
            {
                var queue = ResolveMessageQueue(queuePath, settings);
                return Source.Repeat(1)
                    .SelectAsync(1, async _ =>
                    {
                        Message msg = null;
                        MessageQueueTransaction trx = null;
                        try
                        {
                            await Task.Factory.FromAsync(queue.BeginPeek(settings.WaitTimeout), queue.EndPeek)
                                .ConfigureAwait(false);

                            trx = new MessageQueueTransaction();
                            trx.Begin();
                            msg = queue.Receive(settings.WaitTimeout, trx);
                            return (msg, trx);
                        }
                        catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                        {
                            trx?.Abort();
                            trx?.Dispose();
                        }
                        catch (Exception ex)
                        {
                            trx?.Abort();
                            trx?.Dispose();

                            // ideally we would only need to do this in case of `MessageQueueErrorCode.StaleHandle`
                            if (ex is MessageQueueException)
                                queue.Close();

                            throw;
                        }

                        return Option<(Message, MessageQueueTransaction)>.None;
                    })
                    .Collect(option => option.HasValue, option => option.Value);
            })
            .AsSourceWithContext(tuple => tuple.Item2)
            .Select(tuple => tuple.Item1);

        private static MessageQueue ResolveMessageQueue(string path, MessageQueueSettings settings) =>
            new MessageQueue(path, settings.DenySharedReceive, settings.EnableConnectionCache, settings.AccessMode)
            {
                UseJournalQueue = settings.UseJournalQueue,
                Formatter = settings.MessageFormatter,
                MessageReadPropertyFilter = settings.MessagePropertyFilter
            };

        private class CommittableMessage : ICommittableMessage
        {
            private readonly MessageQueueTransaction _transaction;

            public CommittableMessage(Message message, MessageQueueTransaction transaction)
            {
                Message = message;
                _transaction = transaction;
            }

            public Message Message { get; }

            public void Commit()
            {
                _transaction?.Commit();
                _transaction?.Dispose();
            }

            public void Abort()
            {
                _transaction?.Abort();
                _transaction?.Dispose();
            }
        }
    }

    /// <summary>
    /// Commit a MessageQueueTransaction that is included in a <see cref="MsmqSource.CommittableMessage"/>
    /// </summary>
    public interface ICommittableMessage
    {
        public Message Message { get; }

        public void Commit();

        public void Abort();
    }
}