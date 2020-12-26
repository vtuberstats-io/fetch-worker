const { getThumbnailOfHighestResolutionVersion } = require('./utils');

async function fetchYoutubeVideoInfo(youtubeApi, videoId) {
  const response = await youtubeApi.videos.list({
    id: videoId,
    part: 'snippet,status,statistics'
  });
  if (!response?.data?.items) {
    throw new Error(`invalid youtube response: ${JSON.stringify(response)}`);
  }

  const info = response.data.items[0];
  return {
    title: info.snippet.title,
    description: info.snippet.description,
    publishedAt: new Date(info.snippet.publishedAt).toISOString(),
    thumbnailUrl: getThumbnailOfHighestResolutionVersion(info.snippet.thumbnails).url,
    tags: info.snippet.tags,
    viewCount: Number(info.statistics.viewCount),
    likeCount: Number(info.statistics.likeCount),
    dislikeCount: Number(info.statistics.dislikeCount)
  };
}

module.exports = { fetchYoutubeVideoInfo };
