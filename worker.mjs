// A fixed-station, read-only public interface. No camera or JHB API is contacted.
export const STATION = 'djuma-cam-b1610b';
const BASE = 'https://api.birdnetcloud.com/api/v1/';
const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
const pending = new Map();
const pick = (o, keys) => Object.fromEntries(keys.filter(k => o?.[k] !== undefined).map(k => [k, o[k]]));
const json = (data, status = 200, ttl = 60) => Response.json(data, {status, headers:{'Cache-Control':status === 200 ? `public, max-age=${ttl}` : 'no-store','X-Content-Type-Options':'nosniff'}});
export function mediaURL(value, id, kind) {
  try {
    const u = new URL(value);
    const ext = kind === 'audio' ? 'wav' : 'jpg';
    return u.protocol === 'https:' && u.hostname === 'storage.googleapis.com' && !u.username && !u.password && u.pathname === `/hosana-birdnet-media/birdnet/${id}-${kind}.${ext}` ? u.href : null;
  } catch { return null; }
}
export function cleanDetection(d, publicMedia = false) {
  const out = pick(d, ['id','timestamp','kind','confidence','occurrence','range_occurrence','passed_range_filter','location_filtered','created_at']);
  out.species = pick(d.species, ['id','common_name','scientific_name','ebird_code']);
  out.media_public = publicMedia;
  for (const kind of ['audio','spectrogram']) out[`${kind}_available`] = Boolean(publicMedia && d[`${kind}_available`] && mediaURL(d[`${kind}_url`],d.id,kind));
  return out;
}
export function route(url) {
  const path = url.pathname.replace(/\/$/,'');
  const root = `/stations/${STATION}`;
  let resource = path.slice('/api/birds/'.length);
  if (!path.startsWith('/api/birds/')) throw new Error('Unknown endpoint');
  if (['station','species','stats','detections'].includes(resource)) {
    const q = new URLSearchParams();
    if (resource === 'species') {
      const period = url.searchParams.get('period') || 'all';
      if (!['today','7d','30d','all'].includes(period)) throw new Error('Invalid period');
      q.set('period',period);
    }
    if (resource === 'detections') {
      const page = url.searchParams.get('page') || '1';
      if (!/^\d{1,4}$/.test(page) || Number(page) < 1 || Number(page) > 2000) throw new Error('Invalid page');
      const confidence = url.searchParams.get('min_confidence') || '0';
      if (!['0','0.7','0.9'].includes(confidence)) throw new Error('Invalid confidence');
      const species = url.searchParams.get('species');
      if (species && !/^[A-Za-z][A-Za-z .'-]{0,79}$/.test(species)) throw new Error('Invalid species');
      q.set('page',String(Number(page)));q.set('per_page','50');q.set('sort','time_desc');q.set('include_filtered','true');q.set('min_confidence',confidence);
      if (species) q.set('species',species);
    }
    const suffix = q.size ? `?${q}` : '';
    return {kind:resource,upstream:`${root}${resource === 'station' ? '' : '/'+resource}${suffix}`,key:resource+suffix};
  }
  const m = resource.match(/^call\/([^/]+)(?:\/(audio|spectrogram))?$/);
  if (!m || !UUID.test(m[1])) throw new Error('Invalid detection');
  return {kind:m[2] || 'call',id:m[1].toLowerCase(),upstream:`/detections/${m[1].toLowerCase()}`,key:`call/${m[1].toLowerCase()}?access=${/^[a-f0-9]{64}$/.test(url.searchParams.get('access')||'')?url.searchParams.get('access'):''}`};
}
export async function callAccess(id, secret) {
  if (!secret) return '';
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret), {name:'HMAC',hash:'SHA-256'}, false, ['sign']);
  const bytes = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(id));
  return [...new Uint8Array(bytes)].map(b=>b.toString(16).padStart(2,'0')).join('');
}
export async function hasCallAccess(id, token, secret) {
  if (!secret || !/^[a-f0-9]{64}$/.test(token || '')) return false;
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret), {name:'HMAC',hash:'SHA-256'}, false, ['verify']);
  const bytes = Uint8Array.from(token.match(/../g), h=>parseInt(h,16));
  return crypto.subtle.verify('HMAC', key, bytes, new TextEncoder().encode(id));
}
async function upstream(path) {
  const r = await fetch(BASE+path.replace(/^\//,''),{headers:{Accept:'application/json'},signal:AbortSignal.timeout(12000),redirect:'manual'});
  if (!r.ok) throw new Error(`upstream-${r.status}`);
  const data = await r.json();
  if (data.success === false || !data.data) throw new Error('upstream-format');
  return data;
}
async function cleanPayload(p, spec, media, secret) {
  let data;
  if (spec.kind === 'detections') data = p.data.map(d=>cleanDetection(d,media));
  else if (spec.kind === 'call') data = cleanDetection(p.data,media);
  else if (spec.kind === 'station') data = pick(p.data,['name','timezone','species_count','detection_count','first_detection','last_detection']);
  else if (spec.kind === 'stats') data = pick(p.data,['total_detections','total_species','detections_today','species_today','busiest_hour','hourly_activity','daily_activity','timezone']);
  else data = p.data.map(d=>({...pick(d,['kind','detection_count','max_confidence','avg_confidence','first_heard','last_heard']),species:pick(d.species,['common_name','scientific_name','id','ebird_code'])}));
  if (spec.kind === 'detections' && secret) await Promise.all(data.map(async d=>{d.access=await callAccess(d.id,secret);}));
  return {data,meta:{...pick(p.meta,['total','page','per_page','total_pages','generated_at','has_more']),fetched_at:new Date().toISOString(),source:'BirdNET-Cloud',station:STATION,media_public:media}};
}
export async function handle(request, env, ctx, cache) {
  if (!['GET','HEAD'].includes(request.method)) return json({error:'Read-only endpoint'},405);
  const url = new URL(request.url);
  if (!url.pathname.startsWith('/api/')) return env.ASSETS.fetch(request);
  let spec;try{spec=route(url);}catch{return json({error:'Unknown endpoint or invalid parameters'},400);}
  const media = env.PUBLIC_BIRD_MEDIA === 'true';
  const key = new Request(`${url.origin}/api-cache-v2/${media}/${spec.key}`);
  if (spec.kind === 'audio' || spec.kind === 'spectrogram') {
    if (!media) return json({error:'This recording is not published here.'},404);
    try {
      const p = await upstream(spec.upstream);
      const target = mediaURL(p.data[spec.kind+'_url'],spec.id,spec.kind);
      if (!target || !p.data[spec.kind+'_available']) return json({error:'Recording unavailable'},404);
      const range = request.headers.get('Range');
      if (range && !/^bytes=\d+-\d*$/.test(range)) return json({error:'Invalid range'},400);
      const r = await fetch(target,{headers:range?{Range:range}:{},redirect:'manual',signal:AbortSignal.timeout(15000)});
      if (!r.ok) return json({error:'Recording unavailable'},404);
      const type=r.headers.get('Content-Type')||'';
      if (!(spec.kind==='audio'?/^(audio\/|application\/octet-stream)/:/^image\/(jpeg|png)/).test(type)) return json({error:'Unexpected media type'},502);
      const headers=new Headers({'Content-Type':spec.kind==='audio'?'audio/wav':type,'Cache-Control':'public, max-age=300','X-Content-Type-Options':'nosniff'});
      for(const k of ['Content-Length','Content-Range','Accept-Ranges']) if(r.headers.has(k)) headers.set(k,r.headers.get(k));
      return new Response(request.method==='HEAD'?null:r.body,{status:r.status,headers});
    }catch{return json({error:'Recording temporarily unavailable'},503);}
  }
  const hit=await cache.match(key);if(hit)return hit;
  if (!pending.has(key.url)) pending.set(key.url,(async()=>{
    const p=await upstream(spec.upstream);
    // Current station media has a bucket/object namespace unique to Djuma.
    // Never expose an arbitrary station's detection through this endpoint.
    if(spec.kind==='call' && !await hasCallAccess(spec.id,url.searchParams.get('access'),env.PUBLIC_CALL_KEY) && !mediaURL(p.data.audio_url,spec.id,'audio') && !mediaURL(p.data.spectrogram_url,spec.id,'spectrogram')) throw new Error('wrong-station');
    return cleanPayload(p,spec,media,env.PUBLIC_CALL_KEY);
  })().finally(()=>pending.delete(key.url)));
  try{
    const result=await pending.get(key.url);
    const r=json(result);ctx.waitUntil(cache.put(key,r.clone()));return r;
  }catch(e){console.warn('BirdNET request failed', e.message === 'wrong-station' ? 'station-check' : e.name);return json({error:e.message==='wrong-station'?'This call is not available in the Djuma public feed.':'BirdNET-Cloud is temporarily unavailable. Please try again shortly.'},e.message==='wrong-station'?404:503);}
}
export default {fetch(request,env,ctx){return handle(request,env,ctx,caches.default);}};
