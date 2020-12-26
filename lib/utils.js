function getThumbnailOfHighestResolutionVersion(thumbnails) {
  const { maxres, standard, high, medium, default: default_ } = thumbnails;
  return maxres || standard || high || medium || default_;
}

module.exports = { getThumbnailOfHighestResolutionVersion };
