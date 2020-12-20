async function withRetry(task, onFailed, retry = 3, latestError = null) {
  if (retry < 0) {
    onFailed(latestError);
    return;
  }
  try {
    await task();
  } catch (e) {
    await withRetry(task, onFailed, retry - 1, e);
  }
}

module.exports = { withRetry };
