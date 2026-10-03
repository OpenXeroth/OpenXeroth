import {enrichBirds,summarizeDay} from './bird-context.mjs';
// A fixed-station, read-only public interface. No camera or JHB API is contacted.
export const STATION = 'djuma-cam-b1610b';
const BASE = 'https://api.birdnetcloud.com/api/v1/';
const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
const pending = new Map();
let storageToken = null;
let storageTokenRequest = null;
const pick = (o, keys) => Object.fromEntries(keys.filter(k => o?.[k] !== undefined).map(k => [k, o[k]]));
const json = (data, status = 200, ttl = 60) => Response.json(data, {status, headers:{'Cache-Control':status === 200 ? `public, max-age=${ttl}` : 'no-store','X-Content-Type-Options':'nosniff'}});
export function mediaURL(value, id, kind) {
  try {
    const u = new URL(value);
    const ext = kind === 'audio' ? 'wav' : 'jpg';
    return u.origin === 'https://storage.googleapis.com' && !u.username && !u.password && u.pathname === `/hosana-birdnet-media/birdnet/${id}-${kind}.${ext}` ? u.href : null;
  } catch { return null; }
}
export function legacyMediaURL(value,id,kind) {
  try {
    const u=new URL(value);
    return u.origin==='https://api.birdnetcloud.com' && !u.username && !u.password && u.pathname===`/api/v1/detections/${id}/media/${kind}` ? u.href : null;
  } catch {return null;}
}
const encode64 = bytes => btoa(String.fromCharCode(...bytes)).replace(/=/g,'').replace(/\+/g,'-').replace(/\//g,'_');
export async function gcsAccessToken(env) {
  if (!env.GCS_SERVICE_ACCOUNT) throw new Error('media-store-not-configured');
  const service=JSON.parse(env.GCS_SERVICE_ACCOUNT);
  if (!service.client_email || !service.private_key) throw new Error('media-store-not-configured');
  if (storageToken?.email===service.client_email && storageToken.expires>Date.now()+60000) return storageToken.value;
  if (!storageTokenRequest) storageTokenRequest=(async()=>{
    const now=Math.floor(Date.now()/1000),encoder=new TextEncoder();
    const pem=service.private_key.replace(/-----BEGIN PRIVATE KEY-----|-----END PRIVATE KEY-----|\s/g,'');
    const bytes=Uint8Array.from(atob(pem),c=>c.charCodeAt(0));
    const key=await crypto.subtle.importKey('pkcs8',bytes,{name:'RSASSA-PKCS1-v1_5',hash:'SHA-256'},false,['sign']);
    const payload=encode64(encoder.encode(JSON.stringify({alg:'RS256',typ:'JWT'})))+'.'+encode64(encoder.encode(JSON.stringify({iss:service.client_email,scope:'https://www.googleapis.com/auth/devstorage.read_only',aud:'https://oauth2.googleapis.com/token',iat:now,exp:now+3600})));
    const signature=await crypto.subtle.sign('RSASSA-PKCS1-v1_5',key,encoder.encode(payload));
    const r=await fetch('https://oauth2.googleapis.com/token',{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:new URLSearchParams({grant_type:'urn:ietf:params:oauth:grant-type:jwt-bearer',assertion:payload+'.'+encode64(new Uint8Array(signature))}),redirect:'manual',signal:AbortSignal.timeout(10000)});
    if(!r.ok) throw new Error('media-store-authentication');
    const result=await r.json();
    if(!result.access_token)throw new Error('media-store-authentication');
    storageToken={email:service.client_email,value:result.access_token,expires:Date.now()+Math.min(Number(result.expires_in)||3600,3600)*1000};
    return storageToken.value;
  })().finally(()=>{storageTokenRequest=null;});
  return storageTokenRequest;
}
export function cleanPhoto(d) {
  try {
    const u=new URL(d.photo_url || d.photo?.thumbnail_url);
    if(u.protocol!=='https:' || !['upload.wikimedia.org','thumb.wikimedia.org'].includes(u.hostname) || u.username || u.password || u.port) return {};
    const parts=u.pathname.split('/').filter(Boolean);
    if(parts[0]!=='wikipedia' || parts[1]!=='commons') return {};
    const file=parts[2]==='thumb'?parts[5]:parts[parts.length-1];
    if(!file) return {};
    u.search='';u.hash='';
    return {photo_url:u.href,photo:{page_url:`https://commons.wikimedia.org/wiki/File:${encodeURIComponent(decodeURIComponent(file))}`,photographer:String(d.photo?.photographer||'').slice(0,250),license:String(d.photo?.license||'').slice(0,100)}};
  } catch {return {};}
}
export function cleanStation(d) {
  return {...pick(d,['name','slug','timezone','kinds','latitude','longitude','location_label','source_type','species_count','detection_count','first_detection','last_detection']),filter_config:pick(d.filter_config,['min_confidence','range_occurrence_threshold','range_model_enabled']),media_retention:pick(d.media_retention,['configured','effective_cap_per_species_per_day'])};
}
export function hourHistogram(rows) {
  const counts=Array(24).fill(0);
  for(const d of rows){const date=new Date(d.timestamp);if(!Number.isNaN(date.getTime()))counts[(date.getUTCHours()+2)%24]++;}
  return {counts,sample_size:rows.length,timezone:'Africa/Johannesburg',latest:rows[0]?.timestamp||null,earliest:rows[rows.length-1]?.timestamp||null};
}
export function cleanDetection(d, publicMedia = false) {
  const out = pick(d, ['id','timestamp','kind','confidence','occurrence','range_occurrence','passed_range_filter','location_filtered','created_at']);
  out.species = pick(d.species, ['id','common_name','scientific_name','ebird_code']);
  Object.assign(out,cleanPhoto(d));
  out.media_public = publicMedia;
  for (const kind of ['audio','spectrogram']) out[`${kind}_available`] = Boolean(publicMedia && d[`${kind}_available`] && (mediaURL(d[`${kind}_url`],d.id,kind) || legacyMediaURL(d[`${kind}_url`],d.id,kind)));
  return out;
}
export function route(url) {
  const path = url.pathname.replace(/\/$/,'');
  const root = `/stations/${STATION}`;
  let resource = path.slice('/api/birds/'.length);
  if (!path.startsWith('/api/birds/')) throw new Error('Unknown endpoint');
  if (['station','species','stats','detections','histogram','daily'].includes(resource)) {
    const q = new URLSearchParams();
    if(resource==='daily') {
      const date=url.searchParams.get('date')||new Date(Date.now()+7200000).toISOString().slice(0,10);
      const parsed=new Date(date+'T00:00:00+02:00'),minimum=url.searchParams.get('min_confidence')||'0.85';
      if(!/^20\d{2}-\d{2}-\d{2}$/.test(date)||Number.isNaN(parsed.getTime())||new Date(parsed.getTime()+7200000).toISOString().slice(0,10)!==date||!['0','0.7','0.85','0.95'].includes(minimum))throw new Error('Invalid day');
      const today=new Date(Date.now()+7200000).toISOString().slice(0,10);
      if(date>today||date<'2026-08-01')throw new Error('Invalid day');
      q.set('from',parsed.toISOString());q.set('to',new Date(parsed.getTime()+86400000).toISOString());q.set('min_confidence',minimum);q.set('include_filtered','true');q.set('sort','time_asc');q.set('per_page','1000');q.set('kind','bird');
      return {kind:'daily',date,minimum:Number(minimum),upstream:`${root}/detections?${q}`,key:`daily/${date}/${minimum}`};
    }
    if (resource === 'species') {
      const period = url.searchParams.get('period') || 'all';
      if (!['today','7d','30d','all'].includes(period)) throw new Error('Invalid period');
      q.set('period',period);
    }
    if (resource === 'detections' || resource === 'histogram') {
      const page = url.searchParams.get('page') || '1';
      if (!/^\d{1,5}$/.test(page) || Number(page) < 1 || Number(page) > 10000) throw new Error('Invalid page');
      const confidence = url.searchParams.get('min_confidence') || '0';
      if (!['0','0.7','0.85','0.9','0.95'].includes(confidence)) throw new Error('Invalid confidence');
      const species = url.searchParams.get('species');
      if (species && !/^[A-Za-z][A-Za-z .'-]{0,79}$/.test(species)) throw new Error('Invalid species');
      const size=url.searchParams.get('per_page') || '50';
      if(!['25','50','100','200'].includes(size)) throw new Error('Invalid page size');
      if(resource==='histogram' && !species) throw new Error('Species required');
      q.set('page',resource==='histogram'?'1':String(Number(page)));q.set('per_page',resource==='histogram'?'1000':size);q.set('sort','time_desc');q.set('include_filtered','true');q.set('min_confidence',confidence);
      if (species) q.set('species',species);
    }
    const suffix = q.size ? `?${q}` : '';
    return {kind:resource,upstream:`${root}${resource === 'station' ? '' : '/'+(resource==='histogram'?'detections':resource)}${suffix}`,key:resource+suffix};
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
async function cleanPayload(p, spec, media, secret, cache, ctx) {
  let data;
  if (spec.kind === 'detections') data = p.data.map(d=>cleanDetection(d,media));
  else if (spec.kind === 'call') data = cleanDetection(p.data,media);
  else if (spec.kind === 'station') data = cleanStation(p.data);
  else if (spec.kind === 'histogram') data = hourHistogram(p.data);
  else if (spec.kind === 'stats') data = pick(p.data,['total_detections','total_species','detections_today','species_today','busiest_hour','hourly_activity','daily_activity','timezone']);
  else data = p.data.map(d=>({...cleanPhoto(d),...pick(d,['kind','detection_count','max_confidence','avg_confidence','first_heard','last_heard']),species:pick(d.species,['common_name','scientific_name','id','ebird_code'])}));
  if (spec.kind === 'detections' && secret) await Promise.all(data.map(async d=>{d.access=await callAccess(d.id,secret);}));
  if(['detections','call','species'].includes(spec.kind))await enrichBirds(data,cache,ctx);
  return {data,meta:{...pick(p.meta,['total','page','per_page','total_pages','generated_at','has_more']),fetched_at:new Date().toISOString(),source:'BirdNET-Cloud',station:STATION,media_public:media}};
}
export async function handle(request, env, ctx, cache) {
  if (!['GET','HEAD'].includes(request.method)) return json({error:'Read-only endpoint'},405);
  const url = new URL(request.url);
  if (!url.pathname.startsWith('/api/')) return env.ASSETS.fetch(request);
  if(url.pathname==='/api/naturecam/live')return liveStream(cache,ctx);
  let spec;try{spec=route(url);}catch{return json({error:'Unknown endpoint or invalid parameters'},400);}
  const media = env.PUBLIC_BIRD_MEDIA === 'true';
  const key = new Request(`${url.origin}/api-cache-v9/${media}/${spec.key}`);
  if (spec.kind === 'audio' || spec.kind === 'spectrogram') {
    if (!media) return json({error:'This recording is not published here.'},404);
    try {
      const mediaKey = new Request(`${url.origin}/api-media-v1/${spec.id}/${spec.kind}`);
      const cacheable = request.method === 'GET' && !request.headers.has('Range');
      if (cacheable && cache.match) {const hit = await cache.match(mediaKey); if (hit) return hit;}
      const p = await upstream(spec.upstream);
      const own = mediaURL(p.data[spec.kind+'_url'],spec.id,spec.kind);
      const legacy = legacyMediaURL(p.data[spec.kind+'_url'],spec.id,spec.kind);
      if (!own && !await hasCallAccess(spec.id,url.searchParams.get('access'),env.PUBLIC_CALL_KEY)) return json({error:'This recording is not in the verified Djuma feed.'},404);
      const target = own || legacy;
      if (!target || !p.data[spec.kind+'_available']) return json({error:'Recording unavailable'},404);
      const range = request.headers.get('Range');
      if (range && !/^bytes=\d+-\d*$/.test(range)) return json({error:'Invalid range'},400);
      const requestHeaders=range?{Range:range}:{};
      if(own && !new URL(target).searchParams.has('X-Goog-Signature')) requestHeaders.Authorization=`Bearer ${await gcsAccessToken(env)}`;
      const r = await fetch(target,{headers:requestHeaders,redirect:'manual',signal:AbortSignal.timeout(15000)});
      if(r.status===401 || r.status===403) return json({error:'The recording store is temporarily unavailable.'},503);
      if (!r.ok) return json({error:'Recording unavailable'},404);
      const type=r.headers.get('Content-Type')||'';
      if (!(spec.kind==='audio'?/^(audio\/|application\/octet-stream)/:/^image\/(jpeg|png)/).test(type)) return json({error:'Unexpected media type'},502);
      const headers=new Headers({'Content-Type':type.startsWith('application/octet-stream')?'audio/wav':type,'Cache-Control':'public, max-age=300','X-Content-Type-Options':'nosniff'});
      for(const k of ['Content-Length','Content-Range','Accept-Ranges']) if(r.headers.has(k)) headers.set(k,r.headers.get(k));
      const response = new Response(request.method==='HEAD'?null:r.body,{status:r.status,headers});
      if (cacheable && r.status === 200 && cache.put) ctx.waitUntil(cache.put(mediaKey,response.clone()));
      return response;
    }catch{return json({error:'The recording store is temporarily unavailable. Please try again later.'},503);}
  }
  const hit=await cache.match(key);if(hit)return hit;
  if (!pending.has(key.url)) pending.set(key.url,(async()=>{
    if(spec.kind==='daily') {
      const rows=[];let complete=false,total=null;
      for(let page=1;page<=12;page++){
        const p=await upstream(spec.upstream+'&page='+page);total=p.meta?.total??total;rows.push(...p.data);
        if(p.data.length<1000||(typeof total==='number'&&rows.length>=total)){complete=true;break;}
      }
      const data=summarizeDay(rows,spec.date,spec.minimum);
      const allGroups=[...data.groups,...data.other_sounds];
      const examples=allGroups.map(g=>cleanDetection(g.representative,media));
      await enrichBirds(examples,cache,ctx);
      await Promise.all(examples.map(async d=>{if(env.PUBLIC_CALL_KEY)d.access=await callAccess(d.id,env.PUBLIC_CALL_KEY);}));
      allGroups.forEach((g,i)=>{g.representative=examples[i];g.species=examples[i].species;});
      return {data,meta:{complete,upstream_total:total,minimum:spec.minimum,fetched_at:new Date().toISOString(),station:STATION,source:'BirdNET-Cloud'}};
    }
    const p=await upstream(spec.upstream);
    // Current station media has a bucket/object namespace unique to Djuma.
    // Never expose an arbitrary station's detection through this endpoint.
    if(spec.kind==='call' && !await hasCallAccess(spec.id,url.searchParams.get('access'),env.PUBLIC_CALL_KEY) && !mediaURL(p.data.audio_url,spec.id,'audio') && !mediaURL(p.data.spectrogram_url,spec.id,'spectrogram')) throw new Error('wrong-station');
    return cleanPayload(p,spec,media,env.PUBLIC_CALL_KEY,cache,ctx);
  })().finally(()=>pending.delete(key.url)));
  try{
    const result=await pending.get(key.url);
    const r=json(result,200,['histogram','daily'].includes(spec.kind)?300:60);ctx.waitUntil(cache.put(key,r.clone()));return r;
  }catch(e){console.warn('BirdNET request failed', e.message === 'wrong-station' ? 'station-check' : e.name);return json({error:e.message==='wrong-station'?'This call is not available in the Djuma public feed.':'BirdNET-Cloud is temporarily unavailable. Please try again shortly.'},e.message==='wrong-station'?404:503);}
}
export function extractLiveVideo(html) {
  const id=html.match(/<link rel="canonical" href="https:\/\/www.youtube.com\/watch\?v=([\w-]{11})"/i)?.[1];
  return id&&html.includes('"channelId":"UCWh93l9snW90iP2ybPHikAg"')&&html.includes('"isLiveNow":true')?id:null;
}
async function liveStream(cache,ctx) {
  const key=new Request('https://open.xeroth.ai/live-video-v1');
  const hit=await cache.match(key);if(hit)return hit;
  try{
    const r=await fetch('https://www.youtube.com/@djumacam/live',{headers:{'User-Agent':'Mozilla/5.0'},signal:AbortSignal.timeout(10000)});
    const id=r.ok?extractLiveVideo(await r.text()):null;
    const result=json({video_id:id,available:Boolean(id),channel_url:'https://www.youtube.com/@djumacam/streams'},200,300);
    ctx.waitUntil(cache.put(key,result.clone()));return result;
  }catch{return json({available:false,video_id:null,channel_url:'https://www.youtube.com/@djumacam/streams'},200,60);}
}
export default {fetch(request,env,ctx){return handle(request,env,ctx,caches.default);}};
