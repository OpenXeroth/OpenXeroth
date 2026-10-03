// The public endpoint only reads shared state. Visitors never consume YouTube API quota.
export const CHANNEL_ID = 'UCWh93l9snW90iP2ybPHikAg';
export const CHANNEL_URL = 'https://www.youtube.com/@djumacam/streams';
export const LIVE_KEY = 'djuma-live-v1';
const VIDEO_ID = /^[A-Za-z0-9_-]{11}$/;
const MAX_AGE = 2 * 60 * 60 * 1000;

export function publicLiveState(value, now = Date.now()) {
  const checked = Date.parse(value?.checked_at);
  const fresh = Number.isFinite(checked) && checked <= now && now - checked < MAX_AGE;
  const id = fresh && value?.available === true && VIDEO_ID.test(value?.video_id) ? value.video_id : null;
  return {video_id: id, available: Boolean(id), checked_at: fresh ? value.checked_at : null, channel_url: CHANNEL_URL};
}

async function youtube(env, endpoint, params) {
  const url = new URL('https://www.googleapis.com/youtube/v3/' + endpoint);
  url.search = new URLSearchParams(params);
  const response = await fetch(url.href, {
    headers: {'X-Goog-Api-Key': env.YOUTUBE_API_KEY, Accept: 'application/json'},
    redirect: 'manual', signal: AbortSignal.timeout(15000)
  });
  if (!response.ok) throw new Error('YouTube live lookup failed');
  const data = await response.json();
  if (!Array.isArray(data.items)) throw new Error('Unexpected live lookup response');
  return data.items;
}

async function verifyVideos(env, ids) {
  if (!ids.length) return null;
  const videos = await youtube(env, 'videos', {part: 'snippet,status,liveStreamingDetails', id: ids.join(','), fields: 'items(id,snippet(channelId,liveBroadcastContent),status/embeddable,liveStreamingDetails(actualStartTime,actualEndTime))'});
  return videos.find(video => ids.includes(video.id) && video.snippet?.channelId === CHANNEL_ID && video.snippet?.liveBroadcastContent === 'live' && video.status?.embeddable === true && video.liveStreamingDetails?.actualStartTime && !video.liveStreamingDetails?.actualEndTime)?.id || null;
}

export async function refreshLiveStream(env) {
  if (!env.YOUTUBE_API_KEY || !env.LIVE_STATE) throw new Error('Live lookup is not configured');
  const previous = await env.LIVE_STATE.get(LIVE_KEY, 'json');
  // Long-running broadcasts can disappear from search while still live. Recheck
  // the known video directly before asking search to discover a replacement.
  let id = VIDEO_ID.test(previous?.video_id) ? await verifyVideos(env, [previous.video_id]) : null;
  if (!id) {
    const items = await youtube(env, 'search', {part: 'snippet', channelId: CHANNEL_ID, eventType: 'live', type: 'video', videoEmbeddable: 'true', maxResults: '5'});
    const ids = items.filter(item => item.id?.kind === 'youtube#video' && VIDEO_ID.test(item.id?.videoId) && item.snippet?.channelId === CHANNEL_ID && item.snippet?.liveBroadcastContent === 'live').map(item => item.id.videoId);
    id = await verifyVideos(env, ids);
  }
  const state = {video_id: id, available: Boolean(id), checked_at: new Date().toISOString()};
  await env.LIVE_STATE.put(LIVE_KEY, JSON.stringify(state));
  return state;
}

export async function liveStream(env) {
  let state = null;
  try { state = await env.LIVE_STATE?.get(LIVE_KEY, 'json'); } catch { /* Show the channel fallback if storage is unavailable. */ }
  return Response.json(publicLiveState(state), {headers: {'Cache-Control': 'public, max-age=60', 'X-Content-Type-Options': 'nosniff'}});
}
