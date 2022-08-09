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
        /// are messages in the queue.
        /// <para>
        /// This method uses <see cref="MessageQueueSettings.WaitTimeout"/>, so the application might block
        /// the current thread indefinitely if an infinite time-out is specified.
        /// </para>
        /// <para>
        /// Depending on whether the referenced <see cref="MessageQueue"/> is transactional or not, this method will:
        /// <para>- receive each message as a single internal transaction.</para>
        /// <para>- receive each message from a non-transactional queue and outside of a transaction context.</para>
        /// </para>
        /// <para>
        /// If this method is called on a transactional queue, the message that is received would be returned
        /// to the queue if the transaction is aborted (i.e., due to a <see cref="MessageQueueException"/>).
        /// </para>
        /// </summary>
        /// <param name="queuePath">Path to the referenced <see cref="MessageQueue"/>.</param>
        /// <param name="settings">Settings to configure the <see cref="MessageQueue"/>.</param>        
        public static Source<Message, Task<NotUsed>> Create(string queuePath, MessageQueueSettings settings) =>
            Source.Lazily(() =>
            {
                var queue = CreateMessageQueue(queuePath, settings);
                return Source.Repeat(NotUsed.Instance)
                    .Select(_ =>
                    {
                        Message msg = null;
                        try
                        {
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
        /// Creates a <see cref="Source"/> that receives messages available in a transactional queue using an
        /// internal transaction context, or wait until there are messages in the queue, and then emits stream
        /// elements of <see cref="ICommittableMessage"/>.
        /// <para>
        /// This method uses <see cref="MessageQueueSettings.WaitTimeout"/>, so the application might
        /// block the current thread indefinitely if an infinite time-out is specified.
        /// </para>
        /// <para>
        /// Because this method is called on a transactional queue, the message that is received would be returned
        /// to the queue if the transaction is aborted. The message is not permanently removed from the queue until
        /// the transaction is committed.
        /// </para>
        /// </summary>
        /// <param name="queuePath">Path to the referenced <see cref="MessageQueue"/>.</param>
        /// <param name="settings">Settings to configure the <see cref="MessageQueue"/>.</param>             
        public static Source<ICommittableMessage, Task<NotUsed>> CreateWithAck(string queuePath, MessageQueueSettings settings) =>
            Source.Lazily(() =>
            {
                var queue = CreateMessageQueue(queuePath, settings);
                return Source.Repeat(NotUsed.Instance)
                    .Select(_ =>
                    {
                        Message msg = null;
                        MessageQueueTransaction trx = null;
                        try
                        {
                            trx = new MessageQueueTransaction();
                            trx.Begin();
                            msg = queue.Receive(settings.WaitTimeout, trx);
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

                        return msg != null
                            ? new CommittableMessage(msg, trx)
                            : Option<ICommittableMessage>.None;
                    })
                    .Collect(option => option.HasValue, option => option.Value);
            });

        /// <summary>
        /// Creates a <see cref="Source"/> that receives messages available in the transactional queue using a
        /// transaction context, or wait until there are messages in the queue, and then emits stream elements
        /// of (<see cref="Message"/>, <see cref="MessageQueueTransaction"/>).
        /// <para>
        /// This method uses <see cref="MessageQueueSettings.WaitTimeout"/>, so the application might block
        /// the current thread indefinitely if an infinite time-out is specified.
        /// </para>
        /// <para>
        /// Because this method is called on a transactional queue, the message that is received would be returned
        /// to the queue if the transaction is aborted. The message is not permanently removed from the queue until
        /// the transaction is committed.
        /// </para>
        /// </summary>
        /// <param name="queuePath">Path to the referenced <see cref="MessageQueue"/>.</param>
        /// <param name="settings">Settings to configure the <see cref="MessageQueue"/>.</param>        
        public static SourceWithContext<Message, MessageQueueTransaction, Task<NotUsed>> CreateWithContext(string queuePath, MessageQueueSettings settings) =>
            Source.Lazily(() =>
            {
                var queue = CreateMessageQueue(queuePath, settings);
                return Source.Repeat(NotUsed.Instance)
                    .Select(_ =>
                    {
                        Message msg = null;
                        MessageQueueTransaction trx = null;
                        try
                        {
                            trx = new MessageQueueTransaction();
                            trx.Begin();
                            msg = queue.Receive(settings.WaitTimeout, trx);
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

                        return msg != null
                            ? (msg, trx)
                            : Option<(Message, MessageQueueTransaction)>.None;
                    })
                    .Collect(option => option.HasValue, option => option.Value);
            })
            .AsSourceWithContext(tuple => tuple.Item2)
            .Select(tuple => tuple.Item1);

        private static MessageQueue CreateMessageQueue(string path, MessageQueueSettings settings) =>
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