import test from 'node:test';
import assert from 'node:assert/strict';
import {rangeAssessment,summarizeDay,creditFromMetadata,enrichBirds,addDay,pickHighlight,HISTORY_KEY} from '../bird-context.mjs';
import {route,handle,updateHistory} from '../worker.mjs';
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
test('the eBird range verdict separates wrong-continent suggestions from local birds missing from the 2019 checklist',()=>{
 const osprey={scientific_name:'Pandion haliaetus',ebird_code:'osprey'},grosbeak={scientific_name:'Coccothraustes vespertinus',ebird_code:'evegro'};
 assert.equal(rangeAssessment(osprey,{flagged:false,occurrence:.1354}).status,'regional');
 assert.equal(rangeAssessment(osprey,{flagged:false,occurrence:.1354}).tier,'regional');
 assert.match(rangeAssessment(osprey,{flagged:false,occurrence:.1354}).note,/13\.5%/);
 assert.equal(rangeAssessment(grosbeak,{flagged:true,occurrence:.0006}).tier,'misidentification');
 assert.match(rangeAssessment(grosbeak,{flagged:true,occurrence:.0006}).note,/under 0\.1%/);
 assert.equal(rangeAssessment(bird,{flagged:true}).status,'listed_flagged');
 assert.equal(rangeAssessment({scientific_name:'Pternistis hildebrandti'},{flagged:false}).tier,'misidentification');
 assert.equal(rangeAssessment(bird,{flagged:false}).status,'listed');
});
test('daily summary groups by the range verdict and counts distinct species per hour',()=>{
 const row=(id,species,passed,t='2026-10-03T08:00:00Z')=>({id,timestamp:t,confidence:.9,species,passed_range_filter:passed,range_occurrence:passed?.5:.0001});
 const d=summarizeDay([row('a',bird,true),row('b',bird,true,'2026-10-03T08:30:00Z'),row('c',{common_name:'Evening Grosbeak',scientific_name:'Coccothraustes vespertinus'},false),row('d',{common_name:'Osprey',scientific_name:'Pandion haliaetus'},true,'2026-10-03T09:00:00Z')],'2026-10-03',.85);
 assert.deepEqual(d.groups.map(g=>g.range_review.tier).sort(),['checklist','misidentification','regional']);
 assert.equal(d.review_species,1);assert.equal(d.species_counts[10],2);assert.equal(d.species_counts[11],1);assert.equal(d.counts[10],3);
});
test('station history counts each recorded day once and the highlight favours the rarest checklist species',()=>{
 let h=addDay(null,'2026-08-26',['caprimulgus pectoralis','vanellus coronatus']);
 h=addDay(h,'2026-08-27',[]);h=addDay(h,'2026-08-28',['caprimulgus pectoralis']);h=addDay(h,'2026-08-28',['caprimulgus pectoralis']);
 assert.equal(h.recorded_days,2);assert.equal(h.through,'2026-08-28');assert.equal(h.days['caprimulgus pectoralis'],2);assert.equal(h.days['vanellus coronatus'],1);
 const g=(name,sci,max)=>({species:{common_name:name,scientific_name:sci},max_confidence:max,range_review:{status:'listed'},representative:{audio_available:true,photo:{verified:true}}});
 const groups=[g('Fiery-necked Nightjar','Caprimulgus pectoralis',.99),g('Crowned Lapwing','Vanellus coronatus',.8)];
 assert.deepEqual(pickHighlight(groups,h),{key:'Vanellus coronatus',reason:'rarest',days:1,recorded_days:2,since:'2026-08-26',through:'2026-08-28'});
 assert.deepEqual(pickHighlight(groups,null),{key:'Caprimulgus pectoralis',reason:'score'});
 assert.equal(pickHighlight([{...groups[0],range_review:{status:'regional'}}],h),null);
});
test('the scheduled history update adds complete past days and never the current day',async()=>{
 const old=globalThis.fetch,store=new Map(),asked=[];
 globalThis.fetch=async url=>{const u=new URL(url);asked.push(u.searchParams.get('from'));return Response.json({data:[{id:'x',timestamp:u.searchParams.get('from'),confidence:.9,species:bird}],meta:{total:1}});};
 const env={LIVE_STATE:{get:async k=>store.has(k)?JSON.parse(store.get(k)):null,put:async(k,v)=>{store.set(k,v);}}};
 try{
  store.set(HISTORY_KEY,JSON.stringify({since:'2026-08-26',through:'2026-10-09',recorded_days:5,days:{}}));
  const h=await updateHistory(env,Date.parse('2026-10-12T10:00:00Z'));
  assert.equal(h.through,'2026-10-11');assert.equal(asked.length,2);assert.equal(h.recorded_days,7);assert.equal(h.days['caprimulgus pectoralis'],2);
 }finally{globalThis.fetch=old;}
});
