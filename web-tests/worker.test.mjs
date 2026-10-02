import test from 'node:test';
import assert from 'node:assert/strict';
import {route,cleanDetection,mediaURL,handle} from '../worker.mjs';
const id='11111111-2222-4333-8444-555555555555';
const clip={id,timestamp:'2026-10-02T06:00:00Z',species:{common_name:'Test Bird',scientific_name:'Testus birdus',private_field:'no'},confidence:0.9,audio_url:`https://storage.googleapis.com/hosana-birdnet-media/birdnet/${id}-audio.wav?X-Goog-Signature=private`,audio_available:true,secret:'no'};
test('only fixed station resources and bounded parameters can be requested',()=>{
 const r=route(new URL('https://open.xeroth.ai/api/birds/detections?species=Fiery-necked%20Nightjar&page=2'));
 assert.match(r.upstream,/stations\/djuma-cam-b1610b\/detections/);assert.match(r.upstream,/include_filtered=true/);assert.match(r.upstream,/species=Fiery-necked\+Nightjar/);
 for(const p of ['detections?page=-1','detections?page=10001','detections?species=https://evil.test','species?period=forever','call/../../secret','unknown'])assert.throws(()=>route(new URL('https://open.xeroth.ai/api/birds/'+p)));
});
test('private media URLs and extra fields never escape the public metadata response',()=>{
 const d=cleanDetection(clip,false);assert.equal(d.audio_available,false);assert.equal(d.media_public,false);assert.equal(d.audio_url,undefined);assert.equal(d.secret,undefined);assert.equal(d.species.private_field,undefined);assert.ok(!JSON.stringify(d).includes('Signature'));
 assert.equal(cleanDetection(clip,true).audio_available,true);
});
test('media must belong to the exact Djuma bucket, detection and type',()=>{
 assert.ok(mediaURL(clip.audio_url,id,'audio'));
 for(const u of ['https://evil.test/x.wav',clip.audio_url.replace('hosana-birdnet-media','other'),clip.audio_url.replace(id,'other'),clip.audio_url.replace('https:','http:')])assert.equal(mediaURL(u,id,'audio'),null);
 assert.equal(mediaURL(clip.audio_url,id,'spectrogram'),null);
});
test('metadata is cached, sanitized, and no login is needed',async()=>{
 const old=globalThis.fetch;let hits=0;globalThis.fetch=async()=>{hits++;return Response.json({data:[clip],meta:{total:1}});};
 const map=new Map(),cache={match:async k=>map.get(k.url)?.clone(),put:async(k,r)=>map.set(k.url,r)};const tasks=[],ctx={waitUntil:p=>tasks.push(p)};
 try{const request=new Request('https://open.xeroth.ai/api/birds/detections');const first=await handle(request,{},ctx,cache);await Promise.all(tasks);const second=await handle(request,{},ctx,cache);assert.equal(first.status,200);assert.equal(second.status,200);assert.equal(hits,1);assert.ok(!(await first.text()).includes('Signature'));}finally{globalThis.fetch=old;}
});
test('media is unavailable by default; mutations are forbidden',async()=>{
 const cache={match:async()=>null},ctx={waitUntil(){}};
 assert.equal((await handle(new Request(`https://open.xeroth.ai/api/birds/call/${id}/audio`),{},ctx,cache)).status,404);
 assert.equal((await handle(new Request('https://open.xeroth.ai/api/birds/detections',{method:'POST'}),{},ctx,cache)).status,405);
});
test('upstream failures return a useful error, not credentials or exception dumps',async()=>{
 const old=globalThis.fetch;globalThis.fetch=async()=>{throw new Error('private upstream URL');};
 try{const r=await handle(new Request('https://open.xeroth.ai/api/birds/stats'),{},{waitUntil(){}},{match:async()=>null});assert.equal(r.status,503);assert.ok(!(await r.text()).includes('private upstream'));}finally{globalThis.fetch=old;}
});
test('another station detection is rejected',async()=>{
 const old=globalThis.fetch;globalThis.fetch=async()=>Response.json({data:{...clip,audio_url:'https://elsewhere.test/audio'}});
 try{const r=await handle(new Request(`https://open.xeroth.ai/api/birds/call/${id}`),{},{waitUntil(){}},{match:async()=>null});assert.equal(r.status,404);}finally{globalThis.fetch=old;}
});
test('historical call links verify station membership without publishing storage URLs',async()=>{
 const {callAccess,hasCallAccess}=await import('../worker.mjs');
 const key='synthetic-test-key',token=await callAccess(id,key);
 assert.equal(await hasCallAccess(id,token,key),true);
 assert.equal(await hasCallAccess(id,token+'0',key),false);
 assert.equal(await hasCallAccess(id,token,'wrong-key'),false);
 assert.equal(await hasCallAccess('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',token,key),false);
 const old=globalThis.fetch;globalThis.fetch=async()=>Response.json({data:{...clip,audio_url:null,audio_available:false}});
 try{const r=await handle(new Request(`https://open.xeroth.ai/api/birds/call/${id}?access=${token}`),{PUBLIC_CALL_KEY:key},{waitUntil(){}},{match:async()=>null,put:async()=>{}});assert.equal(r.status,200);assert.equal((await r.json()).data.audio_available,false);}finally{globalThis.fetch=old;}
});
test('enabled audio streams through the site, preserves byte ranges and hides storage credentials',async()=>{
 const old=globalThis.fetch,requests=[];
 globalThis.fetch=async(url,options)=>{
  requests.push({url,options});
  if(requests.length===1)return Response.json({data:clip});
  return new Response('RIFF',{status:206,headers:{'Content-Type':'audio/wav','Content-Length':'4','Content-Range':'bytes 0-3/100','Accept-Ranges':'bytes'}});
 };
 try{
  const r=await handle(new Request(`https://open.xeroth.ai/api/birds/call/${id}/audio`,{headers:{Range:'bytes=0-3'}}),{PUBLIC_BIRD_MEDIA:'true'},{waitUntil(){}},{});
  assert.equal(r.status,206);assert.equal(await r.text(),'RIFF');assert.equal(r.headers.get('Content-Range'),'bytes 0-3/100');assert.equal(requests[1].options.headers.Range,'bytes=0-3');assert.equal(requests[1].options.redirect,'manual');assert.ok(!JSON.stringify([...r.headers]).includes('Signature'));
 }finally{globalThis.fetch=old;}
});
test('enabled spectrogram is served as an image and upstream redirects are not followed',async()=>{
 const old=globalThis.fetch;let redirected=false;
 const d={...clip,spectrogram_available:true,spectrogram_url:`https://storage.googleapis.com/hosana-birdnet-media/birdnet/${id}-spectrogram.jpg?X-Goog-Signature=synthetic`};
 globalThis.fetch=async url=>url.includes('api.birdnetcloud.com')?Response.json({data:d}):redirected?new Response(null,{status:302,headers:{Location:'https://elsewhere.test'}}):new Response('image',{headers:{'Content-Type':'image/jpeg'}});
 try{
  const req=new Request(`https://open.xeroth.ai/api/birds/call/${id}/spectrogram`),env={PUBLIC_BIRD_MEDIA:'true'};
  const r=await handle(req,env,{waitUntil(){}},{});assert.equal(r.status,200);assert.equal(r.headers.get('Content-Type'),'image/jpeg');assert.equal(await r.text(),'image');
  redirected=true;assert.equal((await handle(req,env,{waitUntil(){}},{})).status,404);
 }finally{globalThis.fetch=old;}
});
test('Wikimedia images preserve source attribution without accepting arbitrary image hosts',async()=>{
 const {cleanPhoto}=await import('../worker.mjs');
 const value=cleanPhoto({photo_url:'https://thumb.wikimedia.org/wikipedia/commons/thumb/a/ab/Test%20bird.jpg/330px-Test%20bird.jpg?utm_source=wiki',photo:{photographer:'A Photographer',license:'CC BY-SA 4.0',page_url:'https://evil.test'}});
 assert.equal(value.photo_url,'https://thumb.wikimedia.org/wikipedia/commons/thumb/a/ab/Test%20bird.jpg/330px-Test%20bird.jpg');
 assert.equal(value.photo.page_url,'https://commons.wikimedia.org/wiki/File:Test%20bird.jpg');assert.equal(value.photo.photographer,'A Photographer');assert.equal(value.photo.license,'CC BY-SA 4.0');
 for(const url of ['https://evil.test/bird.jpg','http://upload.wikimedia.org/wikipedia/commons/a/b.jpg','https://upload.wikimedia.org:444/wikipedia/commons/a/b.jpg'])assert.deepEqual(cleanPhoto({photo_url:url}),{});
});
test('station information is complete but excludes private configuration',async()=>{
 const {cleanStation}=await import('../worker.mjs');
 const s=cleanStation({name:'Djuma',latitude:-24.71,longitude:31.54,source_type:'rtsp',source_url:'private',filter_config:{min_confidence:.7,range_occurrence_threshold:.015,range_model_enabled:true,secret:'private'},media_retention:{configured:'platform_default',effective_cap_per_species_per_day:0,private:'no'}});
 assert.equal(s.filter_config.min_confidence,.7);assert.equal(s.media_retention.effective_cap_per_species_per_day,0);assert.equal(s.latitude,-24.71);assert.ok(!JSON.stringify(s).includes('private'));
});
test('page sizes, confidence floors and bounded species histograms match the original controls',async()=>{
 for(const size of [25,50,100,200])assert.match(route(new URL(`https://open.xeroth.ai/api/birds/detections?per_page=${size}&min_confidence=0.85`)).upstream,new RegExp(`per_page=${size}`));
 assert.throws(()=>route(new URL('https://open.xeroth.ai/api/birds/detections?per_page=100000')));
 assert.throws(()=>route(new URL('https://open.xeroth.ai/api/birds/histogram')));
 const h=route(new URL('https://open.xeroth.ai/api/birds/histogram?species=Test%20Bird'));assert.match(h.upstream,/per_page=1000/);assert.match(h.upstream,/\/detections\?/);
 const {hourHistogram}=await import('../worker.mjs');const counts=hourHistogram([{timestamp:'2026-10-02T22:30:00Z'},{timestamp:'2026-10-02T06:00:00Z'}]);assert.equal(counts.counts[0],1);assert.equal(counts.counts[8],1);assert.equal(counts.sample_size,2);
});
test('private GCS uses a verified read-only assertion and keeps credentials server-side',async()=>{
 const {gcsAccessToken}=await import('../worker.mjs');
 const pair=await crypto.subtle.generateKey({name:'RSASSA-PKCS1-v1_5',modulusLength:2048,publicExponent:new Uint8Array([1,0,1]),hash:'SHA-256'},true,['sign','verify']);
 const der=await crypto.subtle.exportKey('pkcs8',pair.privateKey);
 const env={PUBLIC_BIRD_MEDIA:'true',GCS_SERVICE_ACCOUNT:JSON.stringify({client_email:'synthetic-test@example.invalid',private_key:'-----BEGIN PRIVATE KEY-----\n'+Buffer.from(der).toString('base64')+'\n-----END PRIVATE KEY-----'})};
 const old=globalThis.fetch;let exchanges=0,reads=0;
 globalThis.fetch=async(url,options)=>{
  if(url==='https://oauth2.googleapis.com/token'){
   exchanges++;assert.equal(options.redirect,'manual');
   const form=new URLSearchParams(options.body),[header,claims,signature]=form.get('assertion').split('.');
   const c=JSON.parse(Buffer.from(claims,'base64url'));assert.equal(c.scope,'https://www.googleapis.com/auth/devstorage.read_only');assert.equal(c.aud,url);assert.equal(c.iss,'synthetic-test@example.invalid');assert.equal(c.exp-c.iat,3600);
   assert.equal(await crypto.subtle.verify('RSASSA-PKCS1-v1_5',pair.publicKey,Buffer.from(signature,'base64url'),new TextEncoder().encode(header+'.'+claims)),true);
   return Response.json({access_token:'synthetic-storage-token',expires_in:3600});
  }
  if(url.startsWith('https://api.birdnetcloud.com/')){assert.equal(options.headers.Authorization,undefined);return Response.json({data:{...clip,audio_url:clip.audio_url.split('?')[0]}});}
  reads++;assert.equal(url,clip.audio_url.split('?')[0]);assert.equal(options.headers.Authorization,'Bearer synthetic-storage-token');assert.equal(options.redirect,'manual');return new Response('RIFF',{headers:{'Content-Type':'audio/wav'}});
 };
 try{
  const tokens=await Promise.all([gcsAccessToken(env),gcsAccessToken(env)]);assert.equal(tokens[0],tokens[1]);assert.equal(exchanges,1);
  const r=await handle(new Request(`https://open.xeroth.ai/api/birds/call/${id}/audio`),env,{waitUntil(){}},{});
  assert.equal(r.status,200);assert.equal(await r.text(),'RIFF');assert.equal(reads,1);assert.equal(exchanges,1);assert.ok(!JSON.stringify([...r.headers]).includes('synthetic-storage-token'));
 }finally{globalThis.fetch=old;}
});
test('unconfigured private storage is reported as unavailable, not as a missing recording',async()=>{
 const old=globalThis.fetch;let reads=0;globalThis.fetch=async()=>{reads++;return Response.json({data:{...clip,audio_url:clip.audio_url.split('?')[0]}});};
 try{const r=await handle(new Request(`https://open.xeroth.ai/api/birds/call/${id}/audio`),{PUBLIC_BIRD_MEDIA:'true'},{waitUntil(){}},{});assert.equal(r.status,503);assert.equal(reads,1);assert.ok(!(await r.text()).includes('private_key'));}finally{globalThis.fetch=old;}
});
test('historical BirdNET media requires a Djuma-issued link and never receives GCS credentials',async()=>{
 const {callAccess,legacyMediaURL}=await import('../worker.mjs'),key='synthetic-history-key',access=await callAccess(id,key);
 const url=`https://api.birdnetcloud.com/api/v1/detections/${id}/media/audio`;
 assert.equal(legacyMediaURL(url,id,'audio'),url);assert.equal(legacyMediaURL(url.replace('api.birdnetcloud.com','evil.test'),id,'audio'),null);assert.equal(legacyMediaURL(url,id,'spectrogram'),null);assert.equal(mediaURL(clip.audio_url.replace('storage.googleapis.com','storage.googleapis.com:444'),id,'audio'),null);
 const old=globalThis.fetch;let reads=0;globalThis.fetch=async(u,options)=>{
  if(u===url){reads++;assert.equal(options.headers.Authorization,undefined);return new Response('wave',{headers:{'Content-Type':'audio/wav'}});}
  return Response.json({data:{...clip,audio_url:url}});
 };
 try{
  const env={PUBLIC_BIRD_MEDIA:'true',PUBLIC_CALL_KEY:key},ctx={waitUntil(){}},request=`https://open.xeroth.ai/api/birds/call/${id}/audio`;
  assert.equal((await handle(new Request(request),env,ctx,{})).status,404);assert.equal(reads,0);
  const r=await handle(new Request(request+'?access='+access),env,ctx,{});assert.equal(r.status,200);assert.equal(await r.text(),'wave');assert.equal(reads,1);
 }finally{globalThis.fetch=old;}
});
