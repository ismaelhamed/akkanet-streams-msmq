using System;
using System.Messaging;
using System.Threading.Tasks;
using Akka.Streams.Stage;

namespace Akka.Streams.Msmq
{
    /// <inheritdoc />
    /// <summary>
    /// Connects to an MSMQ instance upon materialization and sends write messages to the server.
    /// </summary>
    internal class MsmqSinkStage : GraphStageWithMaterializedValue<SinkShape<Message>, Task<Done>>
    {
        public MessageQueue Queue { get; }

        public readonly Inlet<Message> In = new Inlet<Message>("MsmqSink.in");
        public override SinkShape<Message> Shape => new SinkShape<Message>(In);
        protected override Attributes InitialAttributes { get; } = Attributes.CreateName("MsmqSink");

        // TODO: We probably don't want to accept a queue from the outside
        public MsmqSinkStage(MessageQueue queue)
        {
            Queue = queue;
        }

        public override ILogicAndMaterializedValue<Task<Done>> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            var promise = new TaskCompletionSource<Done>();
            var logic = new MsmqSinkStageLogic(this, Shape, promise);
            return new LogicAndMaterializedValue<Task<Done>>(logic, promise.Task);
        }

        public override string ToString() => "MsmqSink";
    }

    internal class MsmqSinkStageLogic : GraphStageLogic
    {
        private readonly MsmqSinkStage _stage;
        private readonly TaskCompletionSource<Done> _promise;

        public MsmqSinkStageLogic(MsmqSinkStage stage, Shape shape, TaskCompletionSource<Done> promise)
            : base(shape)
        {
            _stage = stage;
            _promise = promise;

            SetHandler(_stage.In,
                onPush: () =>
                {
                    var message = Grab(_stage.In);
                    _stage.Queue.Send(message, _stage.Queue.Transactional
                        ? MessageQueueTransactionType.Single
                        : MessageQueueTransactionType.None);
                    Pull(_stage.In);
                },
                onUpstreamFinish: () =>
                {
                    _promise.SetResult(Done.Instance);
                },
                onUpstreamFailure: ex =>
                {
                    Log.Error("Upstream failure: {0}", ex.Message);
                    _promise.SetException(ex);
                });
        }

        public override void PreStart()
        {
            try
            {
                // This requests one element at the Sink startup
                Pull(_stage.In);
            }
            catch (Exception ex)
            {
                _promise.TrySetException(ex);
                FailStage(ex);
            }
        }

        public override void PostStop()
        {
            _promise.TrySetException(new ApplicationException("Stage stopped unexpectedly"));
            base.PostStop();
        }
    }
}
