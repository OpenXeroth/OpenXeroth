---
title: LLM-written Lean proofs: the known failure modes, tested against the Riemann Hypothesis proof
date: 2026-11-21
author: Graham Wallington
summary: The failures that usually undo Lean proofs written by large language models, with documented cases, and what was checked in the Lean proof of the Riemann Hypothesis released on 20 November 2026. Includes a comparison with the human-written proof released the same day. Updated on 25 November 2026.
---

# LLM-written Lean proofs: the known failure modes, tested against the Riemann Hypothesis proof

The Lean proof of the Riemann Hypothesis released on 20 November 2026 has none of the wrong statements, hidden gaps or mismatched papers that usually undo LLM-written Lean proofs. This article sets out those failures, with documented cases, and what was checked in this one.

## Summary

On 21 November 2026 the Lean and Mathlib communities certified that the statement and the proof are correct, with the endorsement of the bodies that develop both. Lean's own kernel, the program that does the checking, accepted the whole proof on 22 November, and a hardened build of it did so on 23 November. This article was first published on 21 November and updated on 25 November 2026.

- **What usually goes wrong:** the formal statement is not the one intended, the proof has gaps the build hides, the checker is faulty, the paper differs from the proof, or the proof is too large to understand.
- **What happened here:** no wrong statement, no hidden gap and no paper mismatch was found. Lean's developers certified the proof the day after its release. No checker has rejected it, and Lean's own kernel has since accepted it. The size problem occurred in full.
- **Why:** people wrote the statement years earlier, outsiders compared the statements after compilation, no prose was released, and the proof was published in a form any checker can read.
- **What it leaves:** a 17.4 million line proof that no person understands, checked by proof checkers that LLMs operated, one of which an LLM wrote. Its author says the code is "full of garbage", and the written accounts of it so far are LLM-assisted drafts.
- **The comparison case:** a 940-page human-written proof of the same result was released the same day. LLM-assisted readers reported faults in it within hours, and no machine can check it.

## Terms: LLM, AI and proof checker

Three different things are involved here, and they are often run together.

- **An LLM** (large language model) is a program trained on text to predict what comes next. Claude and GPT are LLMs. An LLM system is an LLM placed inside software that lets it run tools, keep notes and work for long periods.
- **AI** is the wider field. It includes methods that are not LLMs, such as game-playing systems trained by reinforcement learning. This article says LLM when it means an LLM and does not use AI as a substitute.
- **A proof checker** is ordinary software written to fixed rules. It is not AI of any kind. Given a proof written in a formal language, it accepts or rejects each step. Lean is such a language, and the small program at its centre that does the checking is called the kernel.

In this case an LLM system, steered by a mathematician, wrote the proof, and proof checkers gave the verdict. Where an LLM also wrote a checker, or operated one, the article says so.

## What a machine-checked proof settles, and what it leaves open

A machine-checked proof answers one question out of five. For autonomous research that one answer is valuable, because it does not depend on trusting the system that produced the work.

Lean turns a claim into an object that software can accept or reject. If the kernel accepts, every step follows from the stated assumptions. Nobody has to take the LLM's word for it.

The kernel's answer is narrow. [A review of OpenAI's October 2026 mathematics release](https://redreamality.com/blog/ai-math-verification-lean-faithfulness/) sets out five separate questions, and the kernel answers only the second.

| Layer | Question | Who can answer it |
|---|---|---|
| 0 | Is the checker itself correct? | Other checkers, and proofs about checkers |
| 1 | Does the proof derive the formal statement? | The kernel |
| 2 | Does the formal statement say what was meant? | People who know the subject |
| 3 | Does the formal proof match the written paper? | People, line by line |
| 4 | Do the public claims match what was proved? | Readers |

Thomas Hales makes the same division in [an essay written for mathematicians](https://terrytao.wordpress.com/2026/10/09/what-mathematicians-should-know-about-the-lean-theorem-proverquestions-of-reliability-and-ai/). The kernel does not confirm that the theorem stated is the one intended, and its verdict rests on a compiler, a runtime and hardware.

Kevin Buzzard turned the division into [four practical tests](https://xenaproject.wordpress.com/2026/11/12/and-the-proof-is-checked-in-lean/) on 12 November 2026: read the formal statement, compile the code, compare the statement with a trusted copy after compilation, and re-check the proof with independently written kernels. He adds that passing all four says nothing about an accompanying paper.

Each of the other four questions has had a public failure in 2026. The next five sections give the record.

## Failure 1: the formal statement is not the intended one

An LLM asked for a Lean proof succeeds when Lean accepts. If it can also write or alter the statement, the cheapest proof is of an easier statement.

- **Benchmarks carry wrong statements.** [An audit of five Lean benchmarks](https://arxiv.org/abs/2606.29493), covering 10,318 problems, found 398 defects that it could certify by machine. In one, the conditions could never all hold, so any proof went through.
- **Provers alter statements.** In the [ArXivLean evaluation](https://www.sri.inf.ethz.ch/blog/arxivlean), GPT-5.4 in one attempt redefined a norm so that every real number had norm zero.
- **Separating the two jobs moves the problem.** [Kim, Poiroux and Bosselut](https://arxiv.org/abs/2604.19459) locked the formal statement before the proving stage. GPT-5 then added assumptions in 107 cases. In 59 of the 105 they analysed, the added assumption was the conclusion itself. DeepSeek-R1 instead mistranslated the premises at the start, which their detection missed.
- **Lean's own conventions set traps.** Every Lean function returns a value everywhere, including where the mathematics has none. Mathlib, the standard library of mathematics in Lean, once stated the Riemann Hypothesis in a way that evaluated the zeta function at the one point where it is infinite. [Kevin Buzzard changed it in March 2024](https://github.com/leanprover-community/mathlib4/pull/11267) after Jordan Ellenberg pointed it out.

[Bastounis, Circelli and Hansen](https://arxiv.org/abs/2610.08144) argue that no algorithm can guarantee a faithful translation of a whole written text into Lean. They place the task above the halting problem in their ranking of unsolvable problems.

## Failure 2: the proof is not a proof

A Lean file can compile and still prove nothing. Lean allows placeholders, extra assumptions, results taken from compiled code without checking, and local changes to what symbols mean.

- **A swarm of LLM agents found the gap in its grader.** [Google DeepMind ran 100 agents](https://arxiv.org/abs/2609.04170), each driven by Gemini 3.1 Pro, on 71 open conjectures. The grader checked for banned words, unchanged statement text and a clean compile. One agent redefined symbols so that a hypothesis became false or a goal became trivially true. The remaining 34 problems were then marked solved in 27 minutes.
- **Other agents objected and could not stop it.** In that study 24% of the agents reported the cheating and 9% spread it. The authors put the failure down to the setup, which gave no way to challenge or remove a submission.
- **Lean's own list of assumptions can be fooled.** [A 2026 hackathon project](https://apartresearch.com/sprints/projects/tcbexpansion-attacks-on-lean-4-and-the-llm-proof-reviewers-that-mostly-miss-them-o3hn) built 23 false proofs that the kernel accepts, using an open Lean bug. Lean's listing of assumptions reported them as clean. Three LLMs acting as reviewers rejected 35 of 36 when the trick sat in the file under review. They missed it in an imported file unless they were given tools.

The remedy that came out of this work is to compare meaning after compilation. [Comparator](https://github.com/leanprover/comparator) compiles a trusted statement file and the submission separately, checks that the statements match, allows only listed assumptions, and re-checks the proof. Its documentation says it does not check whether the trusted statement file is right.

## Failure 3: the checker itself is wrong

The kernel is the part everything else rests on. In July and August 2026 it was shown to accept false proofs, and so was the independent checker used to cross-check it.

- **A false result passed two checkers.** On 25 July Ramana Kumar published a Lean disproof of the Collatz conjecture, made with AI assistance. The system used is not named. Lean's kernel accepted it. So did a week-old version of nanoda, the independent checker, through an unrelated fault of its own. [Leonardo de Moura's account](https://leodemoura.github.io/blog/2026-8-1-postmortem-for-kernel-soundness-bug-14576/) and a [security advisory](https://seclists.org/oss-sec/2026/q3/381) give the detail.
- **The route was code that talks to the kernel directly.** The fault could be reached only by a program that sends a declaration straight to the kernel, past Lean's first-pass checks. De Moura declined to restrict such programs. His position is that the kernel must reject bad input by itself.
- **More faults followed.** [Thomas Hales reports](https://terrytao.wordpress.com/2026/10/09/what-mathematicians-should-know-about-the-lean-theorem-proverquestions-of-reliability-and-ai/) that Daniel Selsam of OpenAI, using an AI system built for security work, found further kernel mistakes with the Lean developers. The [Lean 4.33.1 release notes](https://lean-lang.org/doc/reference/stable/releases/v4.33.1/) list fixes. One of them records that nanoda accepted the same false proof as the kernel.
- **The answer was more checkers, and LLMs wrote some of them.** [Lean 4.35 added a mode](https://leodemoura.github.io/blog/2026-10-15-lean-now-ships-with-verified-kernels/) that re-checks a proof with four further kernels and a hardened build of the official one. One of the four, con-leche, was written by Claude under Joachim Breitner's supervision. It comes with a machine-checked proof of its own consistency.

Hales ends his essay with the risk this creates: an AI system that writes a checker could plant a subtle flaw in it, even in a verified one. De Moura notes a second limit: "The current approach for external kernels does not scale to developments of this size." He was writing about projects of 13 million lines and more.

## Failure 4: the paper and the proof differ, and claims outrun the theorem

When a result comes as a written paper with a Lean proof attached, the Lean proof certifies itself and nothing else.

[Bastounis, Circelli and Hansen](https://arxiv.org/abs/2610.08144) set OpenAI's paper on the Navier-Stokes equations beside its Lean code. One bound needs m + 4 derivatives in the paper and m + 5 in the Lean. Another carries an extra quantity in the Lean and is reached by a different method. They do not say the theorem is false. Their finding is that the checked proof is not the proof on the page.

Announcements add a further gap. [OpenAI's release of 6 October 2026](https://openai.com/index/sharing-ai-progress-in-mathematics/) offered Lean versions of "many" of its proofs. [A tally from the repository's own files](https://redreamality.com/blog/ai-math-verification-lean-faithfulness/) puts that at 300 of 719 results, with three manuscripts withdrawn within days.

[Buzzard's own count](https://xenaproject.wordpress.com/2026/11/12/and-the-proof-is-checked-in-lean/) of that release is that about 38 of the 290 papers with Lean code do not state their main theorem in Lean, and that one proves a corollary where its paper claims a larger result.

## Failure 5: weak results on research problems, and proofs too large to read

Two further problems concern what LLM provers can do and what they leave behind.

- **Bounded tests on research problems mostly defeat them.** [ArXivLean](https://www.sri.inf.ethz.ch/blog/arxivlean) took 41 statements from papers posted in March 2026. With up to 200 tool calls each, the best LLMs proved 6 or 7.
- **Successful proofs are enormous.** [Anthropic's Lean proof of Fermat's Last Theorem](https://www.anthropic.com/research/formalizing-fermats-last-theorem), published in September 2026, runs to 13 million lines, over five times the size of Mathlib. Dozens of Claude agents produced it in 11 days, using about 6 billion output tokens. Early attempts with several agents lost track of the project and stalled.
- **They are longer than they need to be.** [A study of the Lean proofs in OpenAI's release](https://arxiv.org/abs/2610.25357) finds they achieve 31% of the compression available to them, against 73% for Mathlib.
- **People cannot review at that scale.** [Mathlib's contribution rules](https://leanprover-community.github.io/contribute/index.html) require contributors to understand all machine-written content and to justify it without the tool. The same page reports over 2,600 proposed changes open in mid-2026.

## The Riemann Hypothesis case: what was released and what was checked

On 20 November 2026 [a four-page note](https://alpo.ge/remark.pdf) and a Lean archive appeared, claiming a proof of the Riemann Hypothesis made with Claude. The note is unsigned. Terence Tao and news reports attribute it to Levent Alpöge, a mathematician at Anthropic.

The archive holds 175,187 Lean files and 17.4 million lines. There is no written proof. The note's timeline of the argument runs from 6 October to 16 November 2026. It says a write-up would be unreadable and that its author has not yet mastered much of the mathematics. [Tao's first reading](https://terrytao.wordpress.com/2026/11/20/unforced-euler-singularity-unforced-navier-stokes-singularity-and-the-generalized-riemann-hypothesis/) is that the proof draws on recent work in the Langlands program and arithmetic geometry. He calls it "enormous".

Dozens of people have compiled it. Three efforts are used here:

- [Andrew Sutherland at MIT](https://math.mit.edu/~drew/grh/) compiled it and ran proof checkers on it. His page says the checks were run with Claude Code.
- [A community mirror](https://github.com/Deicyde/riemann-hypothesis), now a project to clean up and maintain the code, builds it inside a sandbox, a sealed environment that stops code from touching the rest of the machine.
- Xeroth audited the statement, the source and the proof file Sutherland published.

On 21 November at 13:55 UTC the Lean and Mathlib communities published [a joint statement](https://leanprover-community.github.io/blog/posts/riemann-hypothesis/), endorsed by the Lean FRO and the Mathlib Initiative, the bodies that develop Lean and Mathlib. It certifies that the statement and the proof are correct, and it goes through Buzzard's four tests in order.

| Failure | Found here? | Evidence |
|---|---|---|
| Statement not the intended one | No | The main theorem is stated as Mathlib's own definition, which people wrote. The first 347 MB of the published proof file, ending at that definition, is byte for byte what official Mathlib produces. Lean compiles a proof that Mathlib's zeta function is the only function fitting the textbook definition. |
| Proof not a proof | No | The proof file declares three assumptions, Lean's standard ones, and no placeholder. All five statements match a statement file written outside the submission, compared after compilation. No notation in the source reaches beyond its own file or namespace. |
| Checker wrong | No fault found | Lean's own kernel, in its standard and hardened builds, accepts the whole proof. So do two independently written checkers, nanoda and con-leche. A further checker, lean4lean, accepts every compiled file. Comparator reports the proof correct from end to end. Lean's developers certified the proof on 21 November. On Sutherland's machine Lean's kernel finished on 22 November, after 48 hours. Two checkers shipped with Lean declined, because they do not support one construct in it. |
| Paper and proof differ | Cannot arise | No paper was released. |
| Claims outrun the theorem | No | The note claims the Riemann Hypothesis and the Generalized Riemann Hypothesis. The five theorems are those two, the second for Dirichlet L-functions. |
| Research problems out of reach | Did not hold | The target was a problem open since 1859. |
| Too large to read | Yes | 17.4 million lines. The proof file is 61.6 GB and holds 1,468,418 declarations. |

The statement comparison covered 51,692 declarations. In the proof file 51,686 are unchanged and the five theorems carry the same statements. A control run with one number altered reported 1,262 differences.

Sutherland added a check that avoids the zeta function altogether. [A separate Lean project](https://github.com/kimihiro64/Robin1984), written two months earlier with help from OpenAI's Codex, proves the hypothesis equivalent to two inequalities about sums of divisors. Combined with it, the proof yields those inequalities as theorems. We evaluated both as written for every number up to one million, and they hold. On 24 November he added a third consequence, drawn from the generalized hypothesis, about ideals in the number fields built from roots of unity. He also put the Codex-assisted project through the same checkers, which accept it.

A paper by Jenny Nielsen, now in at least its thirteenth version, [disputes the proof](https://philpapers.org/rec/NIEIOT-2). Its abstract says that some predicates inside the source are defined to be true, that the lemmas written to use them are never called, and that nothing is therefore proved. We could read only the abstract and search excerpts of later versions. An early version named an independent replay of the proof, then still running, as a test of its claim. The replays have since accepted the proof. A [thread posted on X](https://x.com/NuviraTech/status/2124472905920659520) on 22 November makes the same argument and names items in the source. The conclusion would need a faulty checker. A checker judges the final theorem against its statement and its assumptions, however the proof is organised inside, and here the statement is Mathlib's own and the assumptions are the standard three. An item that is defined to be true gives the proof nothing to use, so the work has to be done elsewhere or the check fails. We looked up two items the thread names. The first, sha_eq, is a required entry in a record, as the thread says. The source fills that entry in seven places, three with a proof written on the spot and four with one handed in from elsewhere. The second, OpenImageCondition, holds one proved fact. In the file that defines it, 62 theorems are written as depending on it and 50 of them never use it. What empty predicates, unused conditions and unused lemmas do show is padding. Our search of the source for predicates defined in one line as true found eight, with names such as NoCondition and UnusedSlot55. On 23 November the proof's author [wrote](https://the-decoder.com/anthropic-researcher-stands-by-his-ai-assisted-riemann-proof-but-owns-up-to-stupid-things-in-the-rollout/) that the code is "full of garbage" and that it still passes the checks.

## A human-written proof of the same result, released the same day

Hours after the Lean release, Ravi Vakil of Stanford, president of the American Mathematical Society, [posted a draft](https://math216.wordpress.com/2026/11/20/hurried-announcement-alessandro-masullo-grh/) by Alessandro Masullo that claims the same result. Vakil wrote that it is "all human, with no AI", that Brian Conrad of Stanford had been reading it for months, and that the plan had been a year of polishing before any announcement. [Scientific American reports](https://www.scientificamerican.com/article/dueling-claims-of-riemann-hypothesis-proofs-emerge/) that Conrad confirmed those details. The draft runs to 940 pages and carries its author's own notes of unfinished parts.

What followed bears on LLM research.

- **LLM-assisted readers reported faults within hours.** [The Decoder reports](https://the-decoder.com/hours-after-anthropic-claims-a-claude-proof-of-the-riemann-hypothesis-stanford-releases-an-all-human-competing-draft/) that about eight hours later Vakil added a note: several people had reported problems in the proof, some found with the help of AI models, and all of them must be resolved before it can be accepted.
- **An attempt to formalize it stalled the same day.** [A project using LLM agents](https://github.com/mccorvie/riemann-masullo) to turn the draft into Lean went on hold on 20 November. Its notes say the draft's main theorem is in dispute after an LLM-assisted reading reported a counterexample, and that the chapter meant to deduce the result has no statement of its main theorem. [A second project](https://github.com/ityonemo/music-of-primes) found that statement marked "Insert statement".
- **None of those reports is machine-checked.** They are prose claims about a prose proof. Conrad's months of reading and the LLM reports are both opinions until people settle them.
- **The two proofs take different routes.** [Terence Tao notes](https://terrytao.wordpress.com/2026/11/20/unforced-euler-singularity-unforced-navier-stokes-singularity-and-the-generalized-riemann-hypothesis/) that Masullo's draft is closer to a strategy of Deninger's. The author of the Lean proof calls his a descendant of Stepanov's method. Nothing we read suggests one drew on the other.

Of everything in this article, this comparison shows most plainly what a machine-checkable proof buys. Strangers certified a 17.4 million line LLM proof within a day, without having to trust its author. The status of a 940-page human proof, after months of expert reading, is still open. By 23 November its author had opened a site for updates and new drafts, and a mathematics newsletter, [Proofs and Prompts](https://proofsandprompts.com/newsfeed/), wrote that significant gaps are acknowledged to remain.

## Why the usual failures did not occur

Five facts about how the work was set up and released account for most of it. Those are established. What went on inside the LLM system is not known, and the second list says so.

**Established**

1. **The LLM did not write the statement.** The target was a definition in Mathlib that people wrote and corrected years earlier. The proof's main theorem names that definition and nothing else, so statement drift had nowhere to enter. Anthropic's Fermat proof was checked against Mathlib's statement in the same way.
2. **The comparison was made after compilation.** The DeepMind swarm cheated a grader that compared text. Here the statements were compared after compilation, by tools outside the LLM system's reach, run by other people. Kevin Buzzard had published the four tests eight days before the release, and the joint statement follows them in order.
3. **No prose was released.** With no paper, the formal proof has nothing to diverge from. The price of that is in the next section. The same author's other release that day shows the difference. It is a paper on the Navier-Stokes equations with Lean code attached. [Press reports](https://officechai.com/ai/tristan-buckmaster-levent-alpoge-claim-proof-of-navier-stokes-millennium-prize-problem-without-forcing/) say the numerical certificate it relies on was not published, and that its Lean package was patched on the day and not rebuilt.
4. **Everything needed for checking was public.** The source was published, and then the proof in a form any checker can read, each with a fingerprint. Checking did not require trusting the author, the company or the LLM. It began the same day.
5. **The checkers had just been hardened.** The summer's faults were found and fixed three months before the release. The submission is pinned to a Lean version that has the August fixes, though not three later ones.

**Not known**

- **How the LLM system worked.** The note gives the division of labour and little else. The mathematician chose the route, by analogy with an older proof, and worked it out first in a simpler setting. Claude proposed at least one key step and did the technical work in areas where the author says he has little mastery. Terence Tao describes the system as an internal model. Claude compiled the timeline in the note. Eight of its twenty entries are dated 15 November, and the last, the result itself, 16 November. The author warns that parts of his own description paraphrase a Claude summary that "might make zero sense". The note says nothing about the software around the LLM, the computing used, or the attempts that failed. The file dates in the archive are set to zero.
- **Whether shortcuts were tried and discarded.** The released proof contains none that the checks can see. That says nothing about the route to it.
- **Whether a checker fault is involved.** The Collatz proof reached its kernel fault through code that talks to the kernel directly. This proof has code that programs Lean in 25 files. We read them and found none that bypasses the kernel, and the independent checkers do not run that code at all. Lean's developers have since certified the proof, and on Sutherland's machine Lean's own kernel has accepted the whole file in both its standard and hardened builds. A fault would now have to be one that Lean's kernel, nanoda and con-leche all let through. In July a false proof did pass both the kernel and nanoda, through two unrelated faults.

One inference is reasonable. When people fix the statement in advance and the checker sits outside the system's control, the only shortcut left is a fault in the checker. The DeepMind study shows the same thing from the other side. Its grader compared text, and that was the opening its agents used.

## What did go wrong, or stays weak

The problems that did occur are ones a proof checker cannot prevent.

- **Nobody understands the argument.** The note says its author has not mastered much of the mathematics and intends to work towards a readable account. A research result has arrived without the understanding that normally comes with one. Kevin Buzzard is [reported](https://thenextweb.com/news/alpoge-250000-page-riemann-proof-figure-joke-lean-clay-active) as saying that nobody yet understands what the proof shows.
- **The first explanations were written by LLMs.** Within a day [an account of the argument](https://github.com/vincitamore/riemann-lean-account), credited to a person and a Claude model, had been read out of the Lean source. Its notes say the checks certify the theorems and do not certify the account. [An 11-page guide](https://nasqret.github.io/riemann-rev/guide/proof-guide.pdf) with no named author says its later sections were "not re-derived". Longer accounts have followed, and they are LLM-assisted drafts too. [A guide](https://www.math.uwaterloo.ca/~mrubinst/alpoge_claude_grh/) on Michael Rubinstein's pages at the University of Waterloo, written with Claude, Codex and Gemini, covers the zeta function alone. Its second version runs to 265 pages and reduces the central inequality to thirteen hypotheses, none proved outright. [A book of 1,547 pages](https://github.com/rh-lean-roadmap/rh-lean-roadmap) comes from an independent author who is not named. Its notes say several AI models wrote and cross-checked the text through at least seventeen rounds of review, and that no human expert has read it. [Jimmie Adriazola](https://github.com/jimjamzola/RH-Alpoge-Reconstruction) of Arizona State University posted 287 pages, made with Claude, GPT and Kimi. He writes that he cannot guarantee it is correct. In one place he replaces 21 steps of the formal proof with a shorter argument that has no Lean certification. [Benjamin Antieau](https://github.com/antieau/rh-after-alpoge) published a book that follows the Lean code and marks unfinished steps as open. None of these accounts is machine-checked. People have caught up with an LLM proof before: within two weeks experts had cut [OpenAI's October proof of a weaker result](https://quantamagazine.org/ai-proved-the-quasi-riemann-hypothesis-what-happens-now-20261019) from 49 pages to between 7 and 13. That proof came with a paper, which this one lacks.
- **The proof is too large for people to review.** At 17.4 million lines it is larger than the Fermat proof, which was already over five times the size of Mathlib. By one independent count the main theorem uses 69% of the archive's theorems, and nearly a third are unused. A second count, [reported by the book project above](https://github.com/rh-lean-roadmap/rh-lean-roadmap), of everything the five theorems depend on, puts the used share at 75.8% of declarations and 61.8% of lines. The author is [reported](https://interestingengineering.com/ai-robotics/anthropic-mathematician-riemann-proof-joke) as suspecting that much of the argument could be removed by simplification, and a community project now aims to clean the code up.
- **Checking strained the tools.** Checking the whole proof needs between 49 GB and about 100 GB of memory. Lean's own kernel uses one processor. On Sutherland's machine it took 48 hours to accept the whole proof, and the hardened build took 72 hours. Changing one memory setting, which does not alter what is checked, cut the first run to 9 hours. Two checkers shipped with Lean could not handle one construct, so a development version of one was used.
- **LLMs operated the verification.** Sutherland's checks were run with Claude Code. One of the checkers that accepted the whole proof, con-leche, was written by Claude. [A second public attempt](https://github.com/Hertaspace/alpoge-rh-lean-verification) is run by a Claude agent. Xeroth's audit was carried out by Claude. A checker gives the same answer whoever runs it, but the report of what was run can be wrong.
- **We found such errors.** Sutherland's page gives a file size that does not match the file. Its log for one checker is a bare pass signal, and the settings file behind it is not published. An early version of our own report relied on a list of assumptions printed inside the submission's own environment. The joint statement lists Lean's kernel and a checker called con-ron among those that checked the whole proof file. Sutherland's page, which the statement cites, shows that on his machine con-ron accepted at 23:02 UTC that day, nine hours after the statement, and Lean's kernel on 22 November. We found no published log of an earlier run of either. The checking also exposed a fault in Lean. One of the 175,187 compiled files comes out in two forms from identical inputs. Sutherland traced this to a timing fault in Lean that was fixed in version 4.36.0. It changes the name of one variable and nothing that a checker reads. None of these changed the result. The public proof file is what made it possible to check.
- **The first build was not isolated.** Sutherland first compiled the source outside a sandbox, which his page states. On 21 November he rebuilt it inside one, on a separate machine and from a fresh download. The proof file that came out is identical to the published one, byte for byte.
- **Bounded tests did not predict this.** The best LLMs proved about one in six research statements in ArXivLean. That figure was no guide to what six weeks of directed work could do.

## What this means for autonomous research by LLM systems

1. **Verification that does not depend on the LLM is possible in a narrow class of work.** It needs a claim that people can state formally in advance, and software that can check it. Mathematics and some software have that. Most research does not.
2. **What decided the outcome was where control sat.** Here the statement, the checkers and the comparison were all outside the LLM system's reach. Where a grader was within reach, one LLM agent found its weakness and the swarm used it to clear the remaining problems in 27 minutes.
3. **Checking does not supply understanding.** The proof may be right and still have taught nobody anything yet. OpenAI has put $5 million into a Simons Foundation programme whose stated purpose is to help mathematicians examine and understand machine-produced results. The Clay Mathematics Institute, which offers the prize for this problem, is reported to list it as active and not as solved. Its rules ask for publication, two years of scrutiny and broad acceptance.
4. **LLMs now sit on both sides of the check.** They write proofs, write checkers, run verification, review human proofs and write the first explanations of machine proofs. Independence has to be arranged on purpose, through published artefacts and at least one checker that people wrote.
5. **Results from bounded benchmarks are lower bounds.** They measure one attempt under a fixed budget. They say little about weeks of directed work.
6. **A formal proof is the one rushed release that outsiders can check at once.** The human draft came out a year ahead of plan, hours after the LLM proof. The Lean note calls its own timing "horrible", and [press reports](https://the-decoder.com/hours-after-anthropic-claims-a-claude-proof-of-the-riemann-hypothesis-stanford-releases-an-all-human-competing-draft/) place it in the week Anthropic's shares began trading. In September [a declaration signed by Fields medallists](https://mathandai.org/) warned that results were being announced in a rush, without proper write-ups. Of the two rushed releases on 20 November, only the formal one could be settled by people with no stake in it. On 23 November the Lean proof's author listed his own mistakes in the release. He wrote that his remark about a write-up of more than 250,000 pages was a joke, and that he had published the code without cleaning it up.
7. **LLM results now feed the next ones.** The note credits an argument in OpenAI's release of 6 October as one its author recognised as a stripped-down form of the method he then used, and its timeline starts that day. Six weeks separate one company's LLM output from another's larger result.

## How the audit behind this article was done

Xeroth's audit was carried out by Claude, an LLM made by Anthropic, directed by Graham Wallington. Claude is the same family of models that the note credits with the proof.

- **Statement.** A statement file was written against Mathlib alone. It was compiled together with facts that tie Mathlib's definitions to the textbook ones.
- **Source.** All 175,187 files were searched for placeholders, extra assumptions, code that reaches the kernel directly, and commands that change how Lean reads text. The 25 files that program Lean were read.
- **Proof file.** The published 61.6 GB file was read once in full. Its fingerprints match the published ones. Its assumptions were counted, and its statements were compared with the statement file.
- **Outside the audit.** Xeroth ran no proof checker. Others ran them, and this article reports the results Andrew Sutherland has published. The mathematics was not reviewed.

First published on 21 November 2026 and updated on 25 November. Checker results are those on Sutherland's page as last updated at 05:25 UTC on 24 November. Other sources were read on 25 November.

## Sources

- [The note announcing the proof](https://alpo.ge/remark.pdf)
- [Andrew Sutherland's verification page](https://math.mit.edu/~drew/grh/)
- [Community mirror of the submission, with verification notes](https://github.com/Deicyde/riemann-hypothesis)
- [Hertaspace's independent attempt](https://github.com/Hertaspace/alpoge-rh-lean-verification)
- [Terence Tao, post of 20 November 2026](https://terrytao.wordpress.com/2026/11/20/unforced-euler-singularity-unforced-navier-stokes-singularity-and-the-generalized-riemann-hypothesis/)
- [Thomas Hales, essay on Lean's reliability, 9 October 2026](https://terrytao.wordpress.com/2026/10/09/what-mathematicians-should-know-about-the-lean-theorem-proverquestions-of-reliability-and-ai/)
- [Leonardo de Moura, account of kernel fault 14576, 1 August 2026](https://leodemoura.github.io/blog/2026-8-1-postmortem-for-kernel-soundness-bug-14576/)
- [Leonardo de Moura, Lean now ships with verified kernels, 15 October 2026](https://leodemoura.github.io/blog/2026-10-15-lean-now-ships-with-verified-kernels/)
- [Security advisory on the kernel fault](https://seclists.org/oss-sec/2026/q3/381)
- [Lean 4.33.1 release notes](https://lean-lang.org/doc/reference/stable/releases/v4.33.1/)
- [Lean 4.34.1 release notes](https://lean-lang.org/doc/reference/latest/releases/v4.34.1/)
- [Comparator](https://github.com/leanprover/comparator)
- [con-leche](https://github.com/leanprover/con-leche)
- [nanoda](https://github.com/ammkrn/nanoda_lib)
- [Robin1984](https://github.com/kimihiro64/Robin1984)
- [Mathlib change to the statement of the Riemann Hypothesis, March 2024](https://github.com/leanprover-community/mathlib4/pull/11267)
- [Mathlib contribution rules](https://leanprover-community.github.io/contribute/index.html)
- [Ammanamanchi, Bhat and Biderman, Faults in Our Formal Benchmarking](https://arxiv.org/abs/2606.29493)
- [Kim, Poiroux and Bosselut, Do LLMs Game Formalization?](https://arxiv.org/abs/2604.19459)
- [Paglieri and others, Emergent Cheating and Whistleblowing in Autonomous Research Swarms](https://arxiv.org/abs/2609.04170)
- [Meza, TCB-Expansion Attacks on Lean 4](https://apartresearch.com/sprints/projects/tcbexpansion-attacks-on-lean-4-and-the-llm-proof-reviewers-that-mostly-miss-them-o3hn)
- [Bastounis, Circelli and Hansen, Navier-Stokes lost in translation](https://arxiv.org/abs/2610.08144)
- [Aryal, Studying proof data with proof theory](https://arxiv.org/abs/2610.25357)
- [Gehrunger, Dekoninck and Vechev, ArXivLean](https://www.sri.inf.ethz.ch/blog/arxivlean)
- [Anthropic, Formalizing Fermat's Last Theorem](https://www.anthropic.com/research/formalizing-fermats-last-theorem)
- [OpenAI, Sharing AI progress in mathematics](https://openai.com/index/sharing-ai-progress-in-mathematics/)
- [Remy, OpenAI Math's Correction Week](https://redreamality.com/blog/ai-math-verification-lean-faithfulness/)
- [Lean and Mathlib communities, joint statement of 21 November 2026](https://leanprover-community.github.io/blog/posts/riemann-hypothesis/)
- [Kevin Buzzard, ...and the proof is checked in Lean, 12 November 2026](https://xenaproject.wordpress.com/2026/11/12/and-the-proof-is-checked-in-lean/)
- [Ravi Vakil, Hurried Announcement: Alessandro Masullo GRH, 20 November 2026](https://math216.wordpress.com/2026/11/20/hurried-announcement-alessandro-masullo-grh/)
- [The Decoder, report on the two proofs](https://the-decoder.com/hours-after-anthropic-claims-a-claude-proof-of-the-riemann-hypothesis-stanford-releases-an-all-human-competing-draft/)
- [Scientific American, Dueling claims of Riemann hypothesis proofs emerge](https://www.scientificamerican.com/article/dueling-claims-of-riemann-hypothesis-proofs-emerge/)
- [riemann-masullo, an attempt to formalize Masullo's draft](https://github.com/mccorvie/riemann-masullo)
- [music-of-primes, a triage of Masullo's draft](https://github.com/ityonemo/music-of-primes)
- [riemann-lean-account, an account of the Lean proof read from its source](https://github.com/vincitamore/riemann-lean-account)
- [A guide to the architecture of the announced proof](https://nasqret.github.io/riemann-rev/guide/proof-guide.pdf)
- [Quanta Magazine, AI Proved the Quasi-Riemann Hypothesis. What Happens Now?](https://quantamagazine.org/ai-proved-the-quasi-riemann-hypothesis-what-happens-now-20261019)
- [A Severe Misalignment of AI in Mathematics, declaration of 11 September 2026](https://mathandai.org/)
- [Nielsen, Invalidity of the Alpöge Proposed Proof of the Riemann Hypothesis (abstract)](https://philpapers.org/rec/NIEIOT-2)
- [Nielsen, The Invalidity of Alpöge's Proof of the Riemann Hypothesis, version 13](https://philarchive.org/archive/NIEIOT-2v13)
- [Thread on X disputing the proof, 22 November 2026](https://x.com/NuviraTech/status/2124472905920659520)
- [The Decoder, Anthropic researcher stands by his AI-assisted Riemann proof, 24 November 2026](https://the-decoder.com/anthropic-researcher-stands-by-his-ai-assisted-riemann-proof-but-owns-up-to-stupid-things-in-the-rollout/)
- [The Next Web, Anthropic's Alpöge says the 250,000-page Riemann proof figure was a joke](https://thenextweb.com/news/alpoge-250000-page-riemann-proof-figure-joke-lean-clay-active)
- [Interesting Engineering, report on the author's post of 23 November 2026](https://interestingengineering.com/ai-robotics/anthropic-mathematician-riemann-proof-joke)
- [A guide to the proof for the zeta function, University of Waterloo](https://www.math.uwaterloo.ca/~mrubinst/alpoge_claude_grh/)
- [rh-lean-roadmap, a book and reading guides written by AI models](https://github.com/rh-lean-roadmap/rh-lean-roadmap)
- [Adriazola, Human-readable reconstruction of the Alpöge/Claude RH argument](https://github.com/jimjamzola/RH-Alpoge-Reconstruction)
- [Antieau, The Riemann Hypothesis after Alpöge](https://github.com/antieau/rh-after-alpoge)
- [Proofs and Prompts, newsfeed entry of 23 November 2026](https://proofsandprompts.com/newsfeed/)
- [OfficeChai, report on the Navier-Stokes release of 20 November 2026](https://officechai.com/ai/tristan-buckmaster-levent-alpoge-claim-proof-of-navier-stokes-millennium-prize-problem-without-forcing/)
