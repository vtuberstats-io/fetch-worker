function getThumbnailOfHighestResolutionVersion(thumbnails) {
  const { maxres, standard, high, medium, default: default_ } = thumbnails;
  return maxres || standard || high || medium || default_;
}

async function fetchYoutubeChannelInfo(youtubeApi, channelId) {
  const response = await youtubeApi.channels.list({
    id: channelId,
    part: 'snippet,statistics,brandingSettings'
  });
  if (!response?.data?.items) {
    throw new Error(`invalid youtube response: ${JSON.stringify(response)}`);
  }
  const info = response.data.items[0];
  return {
    title: info.snippet.title,
    description: info.snippet.description,
    publishedAt: new Date(info.snippet.publishedAt).toISOString(),
    thumbnailUrl: getThumbnailOfHighestResolutionVersion(info.snippet.thumbnails),
    bannerUrl: info.brandingSettings.image.bannerExternalUrl,
    viewCount: Number(info.statistics.viewCount),
    subscriberCount: Number(info.statistics.subscriberCount),
    videoCount: Number(info.statistics.videoCount)
  };
}

module.exports = { fetchYoutubeChannelInfo };
