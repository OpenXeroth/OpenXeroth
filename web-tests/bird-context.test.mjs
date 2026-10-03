import test from 'node:test';
import assert from 'node:assert/strict';
import {rangeAssessment,summarizeDay,creditFromMetadata,enrichBirds} from '../bird-context.mjs';
import {route,handle} from '../worker.mjs';
const bird={common_name:'Fiery-necked Nightjar',scientific_name:'Caprimulgus pectoralis'};
test('regional review flags explicit range conflicts, taxonomy differences and new unknown names',()=>{
 assert.equal(rangeAssessment(bird).status,'listed');
 assert.equal(rangeAssessment({scientific_name:'Alopochen aegyptiaca'}).status,'listed');
 assert.equal(rangeAssessment({scientific_name:'Upupa epops'}).status,'taxonomy_review');
 for(const name of ['Pternistis hildebrandti','Myadestes townsendi','Pardalotus rubricatus'])assert.equal(rangeAssessment({scientific_name:name}).status,'outside_range');
 assert.equal(rangeAssessment({scientific_name:'Unknown bird'}).status,'not_listed');
 assert.match(rangeAssessment({scientific_name:'Prinia subflava'}).note,/not proof/);
});
test('daily grouping counts each event once, applies SAST day boundaries and matches the confidence filter',()=>{
 const row=(id,timestamp,confidence=.9)=>({id,timestamp,confidence,species:bird});
 const rows=[row('a','2026-10-02T22:00:00Z'),row('b','2026-10-03T21:59:59Z'),row('a','2026-10-02T22:00:00Z'),row('c','2026-10-02T21:59:59Z'),row('d','2026-10-03T22:00:00Z'),row('e','2026-10-03T09:00:00Z',.7)];
 const d=summarizeDay(rows,'2026-10-03',.85);assert.equal(d.total_detections,2);assert.equal(d.total_species,1);assert.equal(d.groups[0].count,2);assert.equal(d.counts[0],1);assert.equal(d.counts[23],1);assert.equal(d.counts.reduce((a,b)=>a+b),d.total_detections);
});
test('daily aggregation is server-side, bounded and uses a fixed interval at midnight SAST',()=>{
 const r=route(new URL('https://open.xeroth.ai/api/birds/daily?date=2026-09-30&min_confidence=0.85'));
 const u=new URL('https://api.birdnetcloud.com'+r.upstream);assert.equal(u.searchParams.get('from'),'2026-09-29T22:00:00.000Z');assert.equal(u.searchParams.get('to'),'2026-09-30T22:00:00.000Z');assert.equal(u.searchParams.get('include_filtered'),'true');
 for(const query of ['date=2026-02-30','date=2099-01-01','date=2026-09-30&min_confidence=evil'])assert.throws(()=>route(new URL('https://open.xeroth.ai/api/birds/daily?'+query)));
});
test('Wikimedia credit comes from author metadata and only permits recognised licence destinations',()=>{
 const c=creditFromMetadata({extmetadata:{Artist:{value:'<a href="https://evil.test">An Author</a> &amp; Co'},LicenseShortName:{value:'CC BY-SA 4.0'},LicenseUrl:{value:'https://creativecommons.org/licenses/by-sa/4.0/'}}});
 assert.equal(c.photographer,'An Author & Co');assert.equal(c.verified,true);assert.match(c.license_url,/creativecommons.org/);
 assert.equal(creditFromMetadata({extmetadata:{Artist:{value:'Author'}}}),null);
});
test('missing attribution withholds the image while retaining a source link',async()=>{
 const old=globalThis.fetch;globalThis.fetch=async()=>new Response('unavailable',{status:503});
 try{const row={species:bird,photo_url:'https://upload.wikimedia.org/test.jpg',photo:{page_url:'https://commons.wikimedia.org/wiki/File:Bird.jpg'}};await enrichBirds([row],{match:async()=>null},{waitUntil(){}});assert.equal(row.photo_url,undefined);assert.equal(row.photo.verified,false);assert.ok(row.photo.page_url);}finally{globalThis.fetch=old;}
});
test('daily endpoint paginates before counting and sanitizes representative clips',async()=>{
 const old=globalThis.fetch;let hits=0;
 const item={id:'11111111-2222-4333-8444-555555555555',timestamp:'2026-09-30T06:00:00Z',confidence:.9,species:{...bird,private_field:'private'},secret:'private'};
 globalThis.fetch=async()=>{hits++;return Response.json({data:hits===1?Array.from({length:1000},(_,i)=>({...item,id:String(i)})):[{...item,id:'1000'}],meta:{total:1001}});};
 try{const r=await handle(new Request('https://open.xeroth.ai/api/birds/daily?date=2026-09-30'),{},{waitUntil(){}},{match:async()=>null,put:async()=>{}});const p=await r.json();assert.equal(hits,2);assert.equal(p.data.total_detections,1001);assert.equal(p.data.groups.length,1);assert.equal(p.meta.complete,true);assert.equal(p.data.groups[0].representative.secret,undefined);assert.equal(p.data.groups[0].species.private_field,undefined);}finally{globalThis.fetch=old;}
});
test('non-species sound classes remain accessible without inflating bird diversity or bird counts',()=>{
 const d=summarizeDay([{id:'bird',timestamp:'2026-10-03T08:00:00Z',confidence:.9,species:bird},{id:'engine',timestamp:'2026-10-03T08:00:01Z',confidence:.95,species:{common_name:'Engine',scientific_name:'Engine'}}],'2026-10-03',.85);
 assert.equal(d.total_species,1);assert.equal(d.total_detections,1);assert.equal(d.other_sounds[0].count,1);assert.equal(d.counts.reduce((a,b)=>a+b),1);
});
test('repeated photographs share one bounded metadata request and one cache lookup',async()=>{
 const old=globalThis.fetch;let fetched=0,lookups=0,puts=0;
 globalThis.fetch=async(url,options)=>{fetched++;assert.equal(options.redirect,'manual');return Response.json({query:{pages:{1:{title:'File:Bird.jpg',imageinfo:[{extmetadata:{Artist:{value:'An Author'},LicenseShortName:{value:'CC BY 4.0'},LicenseUrl:{value:'https://creativecommons.org/licenses/by/4.0/'}}}]}}}});};
 try{const rows=Array.from({length:200},()=>({species:bird,photo_url:'https://upload.wikimedia.org/bird.jpg',photo:{page_url:'https://commons.wikimedia.org/wiki/File:Bird.jpg'}}));await enrichBirds(rows,{match:async()=>{lookups++;return null;},put:async()=>{puts++;}},{waitUntil(){}});assert.equal(fetched,1);assert.equal(lookups,1);assert.equal(puts,1);assert.ok(rows.every(r=>r.photo.verified));}finally{globalThis.fetch=old;}
});
