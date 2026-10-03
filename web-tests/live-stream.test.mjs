import test from 'node:test';
import assert from 'node:assert/strict';
import {CHANNEL_ID,CHANNEL_URL,LIVE_KEY,publicLiveState,refreshLiveStream,liveStream} from '../live-stream.mjs';
import worker,{handle} from '../worker.mjs';
const video_id='iUdDKf9aDUU';
const verified={id:video_id,snippet:{channelId:CHANNEL_ID,liveBroadcastContent:'live'},status:{embeddable:true},liveStreamingDetails:{actualStartTime:'2026-07-31T17:11:00Z'}};
const item={id:{kind:'youtube#video',videoId:video_id},snippet:{channelId:CHANNEL_ID,liveBroadcastContent:'live'}};

test('scheduled lookup is fixed to embeddable live Djuma videos; its secret stays server-side',async()=>{
 const old=globalThis.fetch,writes=[];
 globalThis.fetch=async(value,options)=>{
  const url=new URL(value);assert.equal(url.origin,'https://www.googleapis.com');
  if(url.pathname.endsWith('/videos'))return Response.json({items:[verified]});
  assert.equal(url.pathname,'/youtube/v3/search');
  for(const [key,value] of Object.entries({channelId:CHANNEL_ID,eventType:'live',type:'video',videoEmbeddable:'true',maxResults:'5'}))assert.equal(url.searchParams.get(key),value);
  assert.equal(url.searchParams.has('key'),false);assert.equal(options.headers['X-Goog-Api-Key'],'synthetic-secret');assert.equal(options.redirect,'manual');
  return Response.json({items:[{...item,snippet:{...item.snippet,channelId:'other'}},{...item,snippet:{...item.snippet,liveBroadcastContent:'upcoming'}},item]});
 };
 try{const result=await refreshLiveStream({YOUTUBE_API_KEY:'synthetic-secret',LIVE_STATE:{get:async()=>null,put:async(k,v)=>writes.push([k,JSON.parse(v)])}});
 assert.equal(result.video_id,video_id);assert.equal(writes[0][0],LIVE_KEY);assert.deepEqual(writes[0][1],result);assert.ok(!JSON.stringify(result).includes('secret'));
 }finally{globalThis.fetch=old;}
});
test('offline results clear the video; failed lookups leave the previous state untouched',async()=>{
 const old=globalThis.fetch,writes=[],env={YOUTUBE_API_KEY:'test',LIVE_STATE:{get:async()=>null,put:async(k,v)=>writes.push(JSON.parse(v))}};
 try{
  globalThis.fetch=async()=>Response.json({items:[]});assert.equal((await refreshLiveStream(env)).available,false);assert.equal(writes[0].video_id,null);
  globalThis.fetch=async()=>new Response('private error',{status:403});await assert.rejects(refreshLiveStream(env),/YouTube live lookup failed/);assert.equal(writes.length,1);
  globalThis.fetch=async()=>Response.json({wrong:'format'});await assert.rejects(refreshLiveStream(env),/Unexpected live lookup/);assert.equal(writes.length,1);
 }finally{globalThis.fetch=old;}
});
test('stale, invalid and future state cannot advertise a live stream',()=>{
 const now=Date.now(),state={video_id,available:true,checked_at:new Date(now-60000).toISOString(),secret:'private'};
 assert.equal(publicLiveState(state,now).video_id,video_id);assert.equal(publicLiveState(state,now).secret,undefined);
 for(const change of [{checked_at:new Date(now-7200000).toISOString()},{checked_at:new Date(now+60000).toISOString()},{checked_at:'invalid'},{video_id:'https://evil.test'},{available:false}])assert.equal(publicLiveState({...state,...change},now).available,false);
 assert.equal(publicLiveState(null).channel_url,CHANNEL_URL);
});
test('public visitors only read shared state, even when missing or storage fails',async()=>{
 const old=globalThis.fetch;globalThis.fetch=async()=>{throw new Error('A visitor must never call YouTube');};
 try{
  const state={video_id,available:true,checked_at:new Date().toISOString()};let reads=0;
  const env={LIVE_STATE:{get:async(key,type)=>{reads++;assert.equal(key,LIVE_KEY);assert.equal(type,'json');return state;}}};
  for(let i=0;i<3;i++){const response=await handle(new Request('https://open.xeroth.ai/api/naturecam/live?channelId=other'),env,{},{});assert.equal((await response.json()).video_id,video_id);}
  assert.equal(reads,3);assert.equal((await (await liveStream({})).json()).available,false);
  assert.equal((await (await liveStream({LIVE_STATE:{get:async()=>{throw new Error('private');}}})).json()).available,false);
 }finally{globalThis.fetch=old;}
});
test('scheduled handler waits for the shared-state update',async()=>{
 const old=globalThis.fetch;globalThis.fetch=async url=>Response.json({items:url.includes('/videos?')?[verified]:[item]});
 try{let written=false;const tasks=[];worker.scheduled({}, {YOUTUBE_API_KEY:'synthetic',LIVE_STATE:{get:async()=>null,put:async()=>{written=true;}}},{waitUntil:p=>tasks.push(p)});await Promise.all(tasks);assert.equal(written,true);assert.equal(tasks.length,1);}finally{globalThis.fetch=old;}
});

test('long-running streams are verified directly; ended or non-embeddable videos are never advertised',async()=>{
 const old=globalThis.fetch,paths=[],writes=[];
 const env={YOUTUBE_API_KEY:'synthetic',LIVE_STATE:{get:async()=>({video_id}),put:async(k,v)=>writes.push(JSON.parse(v))}};
 try{
  globalThis.fetch=async url=>{paths.push(new URL(url).pathname);return Response.json({items:[verified]});};
  assert.equal((await refreshLiveStream(env)).video_id,video_id);assert.deepEqual(paths,['/youtube/v3/videos']);
  for(const changed of [{...verified,liveStreamingDetails:{...verified.liveStreamingDetails,actualEndTime:'2026-10-03T12:00:00Z'}},{...verified,status:{embeddable:false}},{...verified,snippet:{...verified.snippet,channelId:'other'}}]){
   globalThis.fetch=async url=>Response.json({items:url.includes('/videos?')?[changed]:[]});
   assert.equal((await refreshLiveStream(env)).available,false);
  }
 }finally{globalThis.fetch=old;}
});
