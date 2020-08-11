using System;
using System.ComponentModel;
using System.Messaging;
using System.Threading.Tasks;
using Akka.Streams.Stage;

namespace Akka.Streams.Msmq
{
    /// <inheritdoc />
    /// <summary>
    /// Connects to an MSMQ instance upon materialization and sends write messages to the Message Queuing server.
    /// </summary>
    internal class MsmqSinkStage : GraphStageWithMaterializedValue<SinkShape<Message>, Task<Done>>
    {
        public MessageQueue Queue { get; }

        public readonly Inlet<Message> In = new Inlet<Message>("MsmqSink.in");

        // TODO: We probably don't want to accept a queue from the outside
        public MsmqSinkStage(MessageQueue queue) => Queue = queue;

        public override ILogicAndMaterializedValue<Task<Done>> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            var promise = new TaskCompletionSource<Done>();
            var logic = new MsmqSinkStageLogic(this, promise);
            return new LogicAndMaterializedValue<Task<Done>>(logic, promise.Task);
        }

        public override SinkShape<Message> Shape => new SinkShape<Message>(In);
        public override string ToString() => "MsmqSink";

        protected override Attributes InitialAttributes { get; } = Attributes.CreateName("MsmqSink");

        #region Logic

        private sealed class MsmqSinkStageLogic : GraphStageLogic
        {
            private readonly MsmqSinkStage _stage;
            private readonly TaskCompletionSource<Done> _promise;

            public MsmqSinkStageLogic(MsmqSinkStage stage, TaskCompletionSource<Done> promise)
                : base(stage.Shape)
            {
                _stage = stage;
                _promise = promise;

                SetHandler(_stage.In,
                    onPush: () =>
                    {
                        try
                        {
                            var message = Grab(_stage.In);
                            _stage.Queue.Send(message, _stage.Queue.Transactional
                               ? MessageQueueTransactionType.Single
                               : MessageQueueTransactionType.None);
                            TryPull(_stage.In);
                        }
                        catch (Exception ex) when (ex is InvalidEnumArgumentException || ex is MessageQueueException)
                        {
                            _promise.TrySetException(ex);
                            FailStage(ex);
                        }
                    },
                    onUpstreamFinish: () =>
                    {
                        _promise.SetResult(Done.Instance);
                        CompleteStage();
                    },
                    onUpstreamFailure: ex =>
                    {
                        _promise.TrySetException(ex);
                        FailStage(ex); // ??
                    });
            }

            public override void PreStart() =>
                // This requests one element at the Sink startup
                TryPull(_stage.In);
        }

        #endregion
    }
}
