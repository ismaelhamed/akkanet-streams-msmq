namespace System.Threading.Tasks
{
    internal static class TaskExtensions
    {
        /// <summary>
        /// Creates a new <see cref="Task"/> by applying a function to the successful result of this task. 
        /// If this task is completed with an exception then the new task will also contain this exception.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult">The type of the returned Task</typeparam>
        /// <param name="source"></param>
        /// <param name="selector">The function which will be applied to the successful result of this Task</param>
        /// <param name="taskContinuationOptions">Options for when the continuation is scheduled and how it behaves.</param>
        /// <returns>A Task which will be completed with the result of the application of the function</returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static Task<TResult> Map<TSource, TResult>(
            this Task<TSource> source,
            Func<TSource, TResult> selector,
            TaskContinuationOptions taskContinuationOptions = TaskContinuationOptions.None)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            return source.ContinueWith(t => selector(t.Result), taskContinuationOptions);
        }
    }
}