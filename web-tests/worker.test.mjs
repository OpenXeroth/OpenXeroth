import test from 'node:test';
import assert from 'node:assert/strict';
import {route,cleanDetection,mediaURL,handle} from '../worker.mjs';
const id='11111111-2222-4333-8444-555555555555';
const clip={id,timestamp:'2026-10-02T06:00:00Z',species:{common_name:'Test Bird',scientific_name:'Testus birdus',private_field:'no'},confidence:0.9,audio_url:`https://storage.googleapis.com/hosana-birdnet-media/birdnet/${id}-audio.wav?X-Goog-Signature=private`,audio_available:true,secret:'no'};
test('only fixed station resources and bounded parameters can be requested',()=>{
 const r=route(new URL('https://open.xeroth.ai/api/birds/detections?species=Fiery-necked%20Nightjar&page=2'));
 assert.match(r.upstream,/stations\/djuma-cam-b1610b\/detections/);assert.match(r.upstream,/include_filtered=true/);assert.match(r.upstream,/species=Fiery-necked\+Nightjar/);
 for(const p of ['detections?page=-1','detections?page=2001','detections?species=https://evil.test','species?period=forever','call/../../secret','unknown'])assert.throws(()=>route(new URL('https://open.xeroth.ai/api/birds/'+p)));
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
