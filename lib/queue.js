const { default: PQueue } = require('p-queue');

class AutoRetryQueue {
  constructor(concurrency, maxRetry) {
    this.queue = new PQueue({ concurrency });
    this.maxRetry = maxRetry;
  }

  enqueue(task, onRetryFailed) {
    task.retryCount = 0;
    const wrappedTask = async (queue, maxRetry) => {
      try {
        await task();
      } catch (e) {
        if (task.retryCount >= maxRetry) {
          onRetryFailed(e);
        } else {
          task.retryCount++;
          queue.add(() => wrappedTask(queue, maxRetry));
        }
      }
    };
    this.queue.add(() => wrappedTask(this.queue, this.maxRetry));
  }
}

module.exports = AutoRetryQueue
