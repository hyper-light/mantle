# 09 — Cells, partition routing and S3/DynamoDB internals: primary-source research

Research note for mantle's cells (STATUS.md, planned item 5). It covers:

- what a "cell" is;
- how requests reach the right cell and the right partition;
- how load is balanced;
- how data moves safely between nodes and between cells;
- how cells scale up and down.

It compares AWS's published designs (S3, DynamoDB, EBS/Physalia, Aurora) with Tectonic and with the peer-reviewed partition-management literature: Slicer, Centrifuge, Shard Manager, Akkio, Windows Azure Storage, CockroachDB, Spanner and Dynamo.

Compiled 2026-09-28. This is research input, not a decision record. §9 proposes decisions for `docs/design/`.

---

## How to read this document

**Citation tags.**

- Papers are cited as `[KEY §section, p. N]`, where `p.` is the printed proceedings page. When a PDF has no printed page numbers, the tag says "PDF p. N".
- Web sources (AWS documentation, blogs, whitepapers, the Builders' Library, patents) are cited as `[KEY, "page or heading title"]` or by printed PDF page.
- Keys are defined once, in the Sources section below.
- Earlier notes are cited as:
  - "note 01 §x": Tectonic, f4, Haystack, Ambry, ZippyDB;
  - "note 04 §x": erasure coding, copysets, placement;
  - "note 05 §x": S3 API semantics;
  - "note 06 §x": consensus and range-partitioned metadata;
  - "note 07 §x": focal's consensus stack.

**Quotes.** Quotes are verbatim from each source's text layer, or from the fetched page text for web sources:

- ligatures are normalized;
- words hyphenated across line breaks are rejoined;
- bracketed reference numbers are dropped;
- "..." marks an elision;
- "[sic]" marks an error in the original.

**Evidence labels.**

- *(no label)*: stated in the cited **peer-reviewed** source and checked against its full text.
- **NON-PEER-REVIEWED**: AWS documentation, the Amazon Builders' Library, AWS whitepapers, AWS and Amazon blogs (AWS News Blog, All Things Distributed, AWS Architecture Blog), conference keynotes and talks, patents, and GitHub READMEs or source code. AWS documentation is a primary source for *documented behavior*, but it is not peer-reviewed. Every such fact or block is labeled.
- **DERIVED**: arithmetic or an interpretation made by this note from stated facts. The source does not state it.
- **UNVERIFIED**: not found in any primary source we consulted. Do not rely on it without new evidence.
- **INFERENCE / Recommendation**: design reasoning for mantle, citing the facts it rests on.

**Method.**

1. PDFs were downloaded from the publishers (usenix.org, ACM, sigops.org) or from the authors, converted with `pdftotext`, and read in full. Page numbers were checked page by page.
2. Web pages were fetched on 2026-09-28.
3. Every quoted fragment of four or more words was then checked by machine against the saved source texts, with both sides reduced to lowercase letters and digits. Each fragment that did not match was reviewed by hand:
   - most were page footers, dropped reference numbers or text-layer glyph substitutions inside the quote;
   - the rest were misquotes, which were corrected.
4. No secondary summaries (blogs about papers, third-party lecture notes or slide decks) were used as evidence.

## Sources

Keys are defined once here and used in the section that cites them. The 'Peer-reviewed' column is the evidence label that applies to every fact drawn from that source.

### Sources for §1, §7.8, §7.9, §8.4.1 and §9

| Key | Full citation | Peer-reviewed | URL / where obtained |
|---|---|---|---|
| **PHY** | Marc Brooker, Tao Chen, Fan Ping (Amazon Web Services). "Millions of Tiny Databases." *17th USENIX Symposium on Networked Systems Design and Implementation (NSDI '20)*, Santa Clara, CA, February 25–27, 2020, pp. 463–478. ISBN 978-1-939133-13-7. | Yes | https://www.usenix.org/conference/nsdi20/presentation/brooker (PDF: https://www.usenix.org/system/files/nsdi20-paper-brooker.pdf). Printed page = PDF page + 461. |
| **AKKIO** | Muthukaruppan Annamalai, Kaushik Ravichandran, Harish Srinivas, Igor Zinkovsky, Luning Pan, Tony Savor, David Nagle (Facebook), Michael Stumm (University of Toronto). "Sharding the Shards: Managing Datastore Locality at Scale with Akkio." *13th USENIX Symposium on Operating Systems Design and Implementation (OSDI '18)*, Carlsbad, CA, October 8–10, 2018, pp. 445–460. ISBN 978-1-939133-08-3. Same key as note 01. | Yes | https://www.usenix.org/conference/osdi18/presentation/annamalai (PDF: https://www.usenix.org/system/files/osdi18-annamalai.pdf). Printed page = PDF page + 443. |
| **SPN** | James C. Corbett, Jeffrey Dean, Michael Epstein, Andrew Fikes, Christopher Frost, JJ Furman, Sanjay Ghemawat, Andrey Gubarev, Christopher Heiser, Peter Hochschild, Wilson Hsieh, Sebastian Kanthak, Eugene Kogan, Hongyi Li, Alexander Lloyd, Sergey Melnik, David Mwaura, David Nagle, Sean Quinlan, Rajesh Rao, Lindsay Rolig, Yasushi Saito, Michal Szymaniak, Christopher Taylor, Ruth Wang, Dale Woodford (Google). "Spanner: Google's Globally-Distributed Database." *10th USENIX Symposium on Operating Systems Design and Implementation (OSDI '12)*, Hollywood, CA, October 8–10, 2012, pp. 251–264. | Yes | https://www.usenix.org/conference/osdi12/technical-sessions/presentation/corbett (PDF: https://www.usenix.org/system/files/conference/osdi12/osdi12-final-16.pdf). Printed page = PDF page + 250. |
| **TEC** | Satadru Pan, Theano Stavrinos, Yunqiao Zhang, Atul Sikaria, Pavel Zakharov, Abhinav Sharma, Shiva Shankar P, Mike Shuey, Richard Wareing, Monika Gangapuram, Guanglei Cao, Christian Preseau, Pratap Singh, Kestutis Patiejunas, JR Tipton, Ethan Katz-Bassett, Wyatt Lloyd. "Facebook's Tectonic Filesystem: Efficiency from Exascale." *19th USENIX Conference on File and Storage Technologies (FAST '21)*, February 23–25, 2021, pp. 217–231. ISBN 978-1-939133-20-5. Summarized in note 01; the passages on cluster scope and federation (§1, §2.2, §3.1, §7) were re-checked for this note. | Yes | https://www.usenix.org/conference/fast21/presentation/pan (PDF: https://www.usenix.org/system/files/fast21-pan.pdf). Printed page = PDF page + 215. |
| **AMZN-PAT** | Stanislav Pavlovskii, Jacob Carr (inventors); Amazon Technologies, Inc. (assignee). "Cell-based storage system with failure isolation." US Patent 11,327,859 B1, filed 2018-09-18, granted 2022-05-10; continuation US Patent 11,886,309 B2, filed 2022-05-06, granted 2024-01-30 (same abstract). Quotes are from the description of US 11,327,859 B1. A patent is not evidence of deployment. | **No (NON-PEER-REVIEWED; patent)** | https://patents.google.com/patent/US11327859B1/en ; https://patents.google.com/patent/US11886309B2/en |

### Sources for §2 ShardStore

| Key | Full citation | Peer-reviewed | URL / where obtained |
|---|---|---|---|
| **SS** | James Bornholt, Rajeev Joshi, Vytautas Astrauskas, Brendan Cully, Bernhard Kragl, Seth Markle, Kyle Sauri, Drew Schleit, Grant Slatton, Serdar Tasiran, Jacob Van Geffen, Andrew Warfield. "Using Lightweight Formal Methods to Validate a Key-Value Storage Node in Amazon S3." *Proceedings of the ACM SIGOPS 28th Symposium on Operating Systems Principles (SOSP '21)*, October 26-28, 2021, Virtual Event, Germany. ACM, pp. 836-850. ISBN 978-1-4503-8709-5. DOI 10.1145/3477132.3483540. Authors, venue and page range checked against Crossref. | Yes | https://doi.org/10.1145/3477132.3483540 (text read from the author copy: https://www.cs.utexas.edu/~bornholt/papers/shardstore-sosp21.pdf) |
| **SHUTTLE** | awslabs/shuttle: README, crate documentation in `shuttle/src/lib.rs`, and GitHub and crates.io metadata, observed 2026-09-28. Primary-source code. | **No (NON-PEER-REVIEWED)** | https://github.com/awslabs/shuttle |
| **LOOM** | tokio-rs/loom: README, and GitHub and crates.io metadata, observed 2026-09-28. Primary-source code. | **No (NON-PEER-REVIEWED)** | https://github.com/tokio-rs/loom |
| **MIRI** | rust-lang/miri: README, observed 2026-09-28. | **No (NON-PEER-REVIEWED)** | https://github.com/rust-lang/miri |

**Works SS cites that this note did not read.** Anything said about these works below is SS's description of them, not ours:
- soft updates: Ganger and Patt, OSDI '94 (SS ref. 16);
- WiscKey: Lu et al., FAST '16 (ref. 31);
- PCT: Burckhardt et al., ASPLOS '10 (ref. 5);
- CDSChecker: Norris and Demsky, OOPSLA '13 (ref. 39);
- bounded partial-order reduction: Coons et al., OOPSLA '13 (ref. 10);
- BOB: Pillai et al., OSDI '14 (ref. 42);
- CrashMonkey: Mohan et al., OSDI '18 (ref. 33);
- Vogels, "Diving Deep on S3 Consistency", 2021 blog (ref. 53; NON-PEER-REVIEWED; see §6);
- Narendra, Tasiran, Desai, "Amazon S3 Strong Consistency", 2021 video talk (ref. 36; NON-PEER-REVIEWED; not viewed);
- Ligouri, "Automating safe, hands-off deployments", Amazon Builders' Library, 2020 (ref. 29; NON-PEER-REVIEWED);
- Newcombe et al., CACM 2015 (ref. 38; see §4 of this note).

### Sources for §3–§4 DynamoDB and formal methods

| Key | Full citation | Peer-reviewed | URL / where obtained |
|---|---|---|---|
| **DDB** | Mostafa Elhemali, Niall Gallagher, Nicholas Gordon, Joseph Idziorek, Richard Krog, Colin Lazier, Erben Mo, Akhilesh Mritunjai, Somu Perianayagam, Tim Rath, Swami Sivasubramanian, James Christopher Sorenson III, Sroaj Sosothikul, Doug Terry, Akshat Vig. "Amazon DynamoDB: A Scalable, Predictably Performant, and Fully Managed NoSQL Database Service." *2022 USENIX Annual Technical Conference (USENIX ATC '22)*, July 11-13, 2022, Carlsbad, CA, pp. 1037-1048. ISBN 978-1-939133-29-8. USENIX papers carry no DOI. | Yes | https://www.usenix.org/conference/atc22/presentation/vig (PDF: https://www.usenix.org/system/files/atc22-elhemali.pdf) |
| **DDB-TX** | Joseph Idziorek, Alex Keyes, Colin Lazier, Somu Perianayagam, Prithvi Ramanathan, James Christopher Sorenson III, Doug Terry, Akshat Vig. "Distributed Transactions at Scale in Amazon DynamoDB." *2023 USENIX Annual Technical Conference (USENIX ATC '23)*, July 10-12, 2023, Boston, MA, pp. 705-717. ISBN 978-1-939133-35-9. Supplementary: only three passages are used, on routing, splits and failover. | Yes | https://www.usenix.org/conference/atc23/presentation/idziorek (PDF: https://www.usenix.org/system/files/atc23-idziorek.pdf) |
| **DDB-DOCS** | Amazon DynamoDB Developer Guide, pages "Partitions and data distribution in DynamoDB" and "DynamoDB burst and adaptive capacity", read 2026-09-28. | **No (NON-PEER-REVIEWED)** | https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.Partitions.html ; https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/burst-adaptive-capacity.html |
| **FM** | Chris Newcombe, Tim Rath, Fan Zhang, Bogdan Munteanu, Marc Brooker, Michael Deardeuff. "How Amazon Web Services Uses Formal Methods." *Communications of the ACM* 58(4):66-73, April 2015. DOI 10.1145/2699417. | Yes, as a refereed CACM "contributed article" (see §4.1 for the caveat) | Publisher PDF (8 pp., "DOI:10.1145/ 2699417" printed on p. 66) from Amazon Science: https://cdn.amazon.science/67/f9/92733d574c11ba1a11bd08bfb8ae/how-amazon-web-services-uses-formal-methods.pdf (landing page https://www.amazon.science/publications/how-amazon-web-services-uses-formal-methods). cacm.acm.org and dl.acm.org returned Cloudflare/403 to our fetchers. |
| **FM-TR** | The same six authors. "Use of Formal Methods at Amazon Web Services." Preprint dated 29 September 2014, 12 pp. It is the earlier version of FM. | No: preprint of FM, used only to record wording differences | https://lamport.azurewebsites.net/tla/formal-methods-amazon.pdf |

**Page numbers.**

- DDB: printed page = PDF page + 1035 (PDF p. 1 is the USENIX cover).
- DDB-TX: printed page = PDF page + 703.
- FM: printed page = PDF page + 65.
- FM-TR has no printed page numbers and is cited as "PDF p. N".

### Sources for §5 and §8 AWS cell guidance and shuffle sharding

| Key | Full citation | Peer-reviewed | URL / where obtained |
|---|---|---|---|
| **CELL-WP** | Amazon Web Services. *Reducing the Scope of Impact with Cell-Based Architecture.* AWS Well-Architected whitepaper. Publication date September 20, 2023; the document history lists only the initial publication. Contributor: Robisson Oliveira. Read from the PDF build of 2026-09-28 (58 pp.), cited by printed page. | **No (NON-PEER-REVIEWED)** | https://docs.aws.amazon.com/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/reducing-scope-of-impact-with-cell-based-architecture.html (PDF: https://docs.aws.amazon.com/pdfs/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/reducing-scope-of-impact-with-cell-based-architecture.pdf) |
| **FIB** | Amazon Web Services. *AWS Fault Isolation Boundaries.* AWS Whitepaper. Publication date November 16, 2022; minor revision February 9, 2023. Contributor: Michael Haken. Read from the PDF build of 2026-09-28 (41 pp.). | **No (NON-PEER-REVIEWED)** | https://docs.aws.amazon.com/whitepapers/latest/aws-fault-isolation-boundaries/abstract-and-introduction.html (PDF: https://docs.aws.amazon.com/pdfs/whitepapers/latest/aws-fault-isolation-boundaries/aws-fault-isolation-boundaries.pdf) |
| **BL-SHUFFLE** | Colm MacCárthaigh. "Workload isolation using shuffle-sharding." *Amazon Builders' Library*, © 2019. PDF, 7 pp. | **No (NON-PEER-REVIEWED)** | https://aws.amazon.com/builders-library/workload-isolation-using-shuffle-sharding/ (PDF: https://d1.awsstatic.com/builderslibrary/pdfs/workload-isolation-using-shuffle-sharding.pdf) |
| **BL-STATIC** | Becky Weiss, Mike Furr. "Static stability using Availability Zones." *Amazon Builders' Library*, © 2019. PDF, 10 pp. | **No (NON-PEER-REVIEWED)** | https://aws.amazon.com/builders-library/static-stability-using-availability-zones/ (PDF: https://d1.awsstatic.com/builderslibrary/pdfs/static-stability-using-availability-zones.pdf) |
| **BL-SMALLER** | Joe Magerramov. "Avoiding overload in distributed systems by putting the smaller service in control." *Amazon Builders' Library*, © 2020. PDF, 8 pp. | **No (NON-PEER-REVIEWED)** | https://aws.amazon.com/builders-library/avoiding-overload-in-distributed-systems-by-putting-the-smaller-service-in-control/ (PDF: https://d1.awsstatic.com/builderslibrary/pdfs/Avoiding%20overload%20in%20distributed%20systems%20by%20putting%20the%20smaller%20service%20in%20control-Joe%20Magerramov.pdf) |
| **BL-CONSTANT** | Colm MacCárthaigh. "Reliability, constant work, and a good cup of coffee." *Amazon Builders' Library*, © 2021. PDF, 8 pp. | **No (NON-PEER-REVIEWED)** | https://aws.amazon.com/builders-library/reliability-and-constant-work/ (PDF: https://d1.awsstatic.com/builderslibrary/pdfs/Reliability-constant-work-and-a-good-cup-of-coffee-Colm-MacCarthaigh.pdf) |
| **SS-BLOG** | Colm MacCárthaigh. "Shuffle Sharding: Massive and Magical Fault Isolation." *AWS Architecture Blog*, 14 April 2014 (the post carries an author's correction note). | **No (NON-PEER-REVIEWED)** | https://aws.amazon.com/blogs/architecture/shuffle-sharding-massive-and-magical-fault-isolation/ |
| **INFIMA** | awslabs/route53-infima (Amazon Route 53 Infima): `README.md`, `SimpleSignatureShuffleSharder.java` and `StatefulSearchingShuffleSharder.java` on branch `master`. Apache-2.0. The repository is archived; its last push was 2022-08-05 (GitHub API, observed 2026-09-28). | **No (NON-PEER-REVIEWED; primary source code)** | https://github.com/awslabs/route53-infima |
| **COPYSETS** | Asaf Cidon, Stephen M. Rumble, Ryan Stutsman, Sachin Katti, John Ousterhout, Mendel Rosenblum. "Copysets: Reducing the Frequency of Data Loss in Cloud Storage." *2013 USENIX Annual Technical Conference (USENIX ATC '13)*, pp. 37-48. Covered in depth in note 04 §A6.1; only §7.1 is used here. | Yes | https://www.usenix.org/system/files/conference/atc13/atc13-cidon.pdf |
| **CATCHME** | Quan Jia, Huangxin Wang, Dan Fleck, Fei Li, Angelos Stavrou, Walter Powell. "Catch Me If You Can: A Cloud-Enabled DDoS Defense." *44th Annual IEEE/IFIP International Conference on Dependable Systems and Networks (DSN 2014)*, pp. 264-275. DOI 10.1109/DSN.2014.35. Read from the 12-page author copy, which has no printed page numbers. | Yes | https://mason.gmu.edu/~hwang14/files/DSN14.pdf |
| **MTDDOS** | Huangxin Wang, Quan Jia, Dan Fleck, Walter Powell, Fei Li, Angelos Stavrou. "A moving target DDoS defense mechanism." *Computer Communications* 46 (2014), pp. 10-21. DOI 10.1016/j.comcom.2014.03.009. Read from the 14-page author copy (manuscript pagination). | Yes | http://mason.gmu.edu/~hwang14/files/COMCOM14.pdf |
| **MOTAG** | Quan Jia, Kun Sun, Angelos Stavrou. "MOTAG: Moving Target Defense against Internet Denial of Service Attacks." *22nd International Conference on Computer Communication and Networks (ICCCN 2013)*, pp. 1-9. DOI 10.1109/ICCCN.2013.6614155. Only the bibliographic record (Crossref, Semantic Scholar) was checked; the full text was not obtained. | Yes | https://doi.org/10.1109/ICCCN.2013.6614155 |
| **CYBERSTAR** | Tingting Xu, Bengbeng Xue, Yang Song, Xiaomin Wu, Xiaoxin Peng, Yilong Lyu, Xiaoliang Wang, Chen Tian, Baoliu Ye, Camtu Nguyen, Biao Lyu, Rong Wen, Zhigang Zong, Shunmin Zhu. "CyberStar: Simple, Elastic and Cost-Effective Network Functions Management in Cloud Network at Scale." *2024 USENIX Annual Technical Conference (USENIX ATC '24)*, pp. 227-246 (range read from the PDF footers). | Yes | https://www.usenix.org/conference/atc24/presentation/xu-tingting |
| **SFQ-BLOG** | Marc Brooker. "SFQ: Simple, Stateless, Stochastic Fairness." *Marc's Blog*, 2026-02-25 (date taken from the URL). | **No (NON-PEER-REVIEWED)** | https://brooker.co.za/blog/2026/02/25/sfq.html |

**Page numbers.** PDFs are cited by the page number printed in their footers. SS-BLOG and SFQ-BLOG are web pages and are cited by heading. CATCHME and MTDDOS are author copies without proceedings pagination and are cited by PDF page.

**Named but not reviewed:**

- the re:Invent 2018 talk ARC338;
- the "Physalia: Cell-based Architecture to Provide Higher Availability on Amazon EBS" video;
- MOTAG's full text;
- McKenney's 1990 Stochastic Fairness Queuing paper.

### Sources for §6 S3 internals

| Key | Full citation | Peer-reviewed | URL / where obtained |
|---|---|---|---|
| S3UG-PERF | AWS. "Best practices design patterns: optimizing Amazon S3 performance." *Amazon S3 User Guide*. Accessed 2026-09-28. | No | https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html |
| S3UG-GUIDE | AWS. "Performance guidelines for Amazon S3." *Amazon S3 User Guide*. Accessed 2026-09-28. | No | https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-guidelines.html |
| S3UG-PATTERNS | AWS. "Performance design patterns for Amazon S3." *Amazon S3 User Guide*. Accessed 2026-09-28. | No | https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html |
| S3UG-PREFIX | AWS. "Organizing objects using prefixes." *Amazon S3 User Guide*. Accessed 2026-09-28. | No | https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html |
| S3UG-CONS | AWS. "Amazon S3 data consistency model" (section of "What is Amazon S3?"). *Amazon S3 User Guide*. Accessed 2026-09-28. | No | https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel |
| S3DG-ERR | AWS. "Error responses" (list of error codes) and "Amazon S3 error best practices." *Amazon S3 Developer Guide*. Accessed 2026-09-28. | No | https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorResponses.html ; https://docs.aws.amazon.com/AmazonS3/latest/developerguide/ErrorBestPractices.html |
| S3DG-ROUTE | AWS. "Request routing." *Amazon S3 Developer Guide*. Accessed 2026-09-28. | No | https://docs.aws.amazon.com/AmazonS3/latest/developerguide/UsingRouting.html |
| S3UG-DIRB | AWS. "Working with directory buckets." *Amazon S3 User Guide*. Accessed 2026-09-28. | No | https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-overview.html |
| S3UG-XPERF | AWS. "Optimizing S3 Express One Zone performance." *Amazon S3 User Guide*. Accessed 2026-09-28. | No | https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-performance.html |
| S3UG-XDIFF | AWS. "Differences for directory buckets." *Amazon S3 User Guide*. Accessed 2026-09-28. | No | https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-differences.html |
| S3UG-XEND | AWS. "Regional and Zonal endpoints for directory buckets in an Availability Zone." *Amazon S3 User Guide*. Accessed 2026-09-28. | No | https://docs.aws.amazon.com/AmazonS3/latest/userguide/endpoint-directory-buckets-AZ.html |
| S3-RRPC17 | AWS. "Request Rate and Performance Considerations." *Amazon S3 Developer Guide (API Version 2006-03-01)*. Internet Archive snapshot of 2017-06-01. The page no longer exists. | No | http://web.archive.org/web/20170601151738/http://docs.aws.amazon.com:80/AmazonS3/latest/dev/request-rate-perf-considerations.html |
| S3-WN18 | AWS. "Amazon S3 Announces Increased Request Rate Performance." *AWS What's New*, posted Jul 17, 2018. | No | https://aws.amazon.com/about-aws/whats-new/2018/07/amazon-s3-announces-increased-request-rate-performance/ |
| S3-BLOG12 | Doug Grismore (Director of Storage Operations, AWS), guest post published by Jeff Barr. "Amazon S3 Performance Tips & Tricks + Seattle S3 Hiring Event." *AWS News Blog*, 06 MAR 2012. The post carries the note "Update (September 2013) ... supplanted by a newer document, S3 Request Rate and Performance Considerations", which is S3-RRPC17. | No | https://aws.amazon.com/blogs/aws/amazon-s3-performance-tips-tricks-seattle-hiring-event/ |
| S3-BLOG20 | Jeff Barr. "Amazon S3 Update – Strong Read-After-Write Consistency." *AWS News Blog*, 01 DEC 2020. | No | https://aws.amazon.com/blogs/aws/amazon-s3-update-strong-read-after-write-consistency/ |
| S3-CONSPAGE | AWS. "Amazon S3 Strong Consistency" (product page). Accessed 2026-09-28. | No | https://aws.amazon.com/s3/consistency/ |
| S3-BLOG23X | Jeff Barr. "New Amazon S3 Express One Zone high performance storage class." *AWS News Blog*, 28 NOV 2023 (updated January 24, 2024). | No | https://aws.amazon.com/blogs/aws/new-amazon-s3-express-one-zone-high-performance-storage-class/ |
| S3-BLOG26 | Sébastien Stormacq. "Twenty years of Amazon S3 and building what's next." *AWS News Blog*, 13 MAR 2026. | No | https://aws.amazon.com/blogs/aws/twenty-years-of-amazon-s3-and-building-whats-next/ |
| REPOST-5XX | AWS Knowledge Center. "How do I troubleshoot an HTTP 500 or 503 error from Amazon S3?" *AWS re:Post*. Published 2018-09-19, modified 2025-08-15 (page metadata). The older URL `.../s3-503-within-request-rate-prefix` now 308-redirects here. | No | https://repost.aws/knowledge-center/http-5xx-errors-s3 |
| VOGELS21 | Werner Vogels. "Diving Deep on S3 Consistency." *All Things Distributed*, April 20, 2021. | No | https://www.allthingsdistributed.com/2021/04/s3-strong-consistency.html |
| PARIS86 | Jehan-François Pâris. "Voting with Witnesses: A Consistency Scheme for Replicated Files." *Proc. 6th International Conference on Distributed Computing Systems (ICDCS)*, IEEE, 1986, pp. 606–612. Read from the author-hosted retypeset copy (9 PDF pages), which is the copy VOGELS21 links to. Cited by section, because the proceedings pagination cannot be mapped onto that copy. | **Yes** | http://www2.cs.uh.edu/~paris/MYPAPERS/Icdcs86.pdf |
| PCASES | The P language project (AWS). "Case Studies." Accessed 2026-09-28. | No | https://p-org.github.io/P/casestudies/ |
| FAST23-KN | Andy Warfield (Amazon). "Building and Operating a Pretty Big Storage System (My Adventures in Amazon S3)." Keynote address, *21st USENIX Conference on File and Storage Technologies (FAST '23)*, Santa Clara, CA, Tuesday, February 21, 2023, 9:15–10:15 am. Only the abstract is text. A video exists; no transcript was reviewed. | No (keynote) | https://www.usenix.org/conference/fast23/presentation/warfield |
| WARFIELD23 | Andy Warfield (guest post on Werner Vogels's blog). "Building and operating a pretty big storage system called S3." *All Things Distributed*, July 27, 2023. Vogels's preface says it is "based on the Keynote address he gave at USENIX FAST '23". | No | https://www.allthingsdistributed.com/2023/07/building-and-operating-a-pretty-big-storage-system.html |
| STG314-23 | Amy Therrien (Director, S3 Engineering), Seth Markle (Senior Principal Engineer, Amazon S3). "Dive deep on Amazon S3" (STG314). *AWS re:Invent 2023*. Slide deck PDF, 76 pages. | No | https://d1.awsstatic.com/events/Summits/reinvent2023/STG314_Dive-deep-on-Amazon-S3.pdf |
| STG203-22 | Oleg Lvovitch (Principal Engineer, Amazon S3), Sally Guo (Software Development Engineer, Amazon S3). "Deep dive on Amazon S3" (STG203). *AWS re:Invent 2022*. Slide deck PDF, 82 pages. | No | https://d1.awsstatic.com/events/Summits/reinvent2022/STG203_Deep-dive-on-Amazon-S3.pdf |

All §6 sources except PARIS86 are **NON-PEER-REVIEWED**: AWS documentation, AWS blogs, a keynote, and re:Invent slide decks. They are primary sources for what AWS *says*, not peer-reviewed descriptions of how S3 is built. Slide numbers are PDF page numbers.

**Not reviewed.** Named here so nobody cites them from this note:

- Marc Brooker and Ankush Desai, "Systems Correctness Practices at Amazon Web Services," *CACM*, 2025, DOI 10.1145/3729175. Both ACM hosts returned HTTP 403. Only the one-sentence summary quoted by PCASES was seen.
- The re:Invent 2024 STG302 deck. The guessed URL returned 403.
- The Pragmatic Engineer interview with Mai-Lan Tomsen Bukovec that S3-BLOG26 draws on. It is third-party.

### Sources for §7.1–§7.3 Slicer, Centrifuge, Shard Manager

| Key | Full citation | Peer-reviewed | URL / where obtained |
|---|---|---|---|
| **SLI** | Atul Adya, Daniel Myers, Jon Howell, Jeremy Elson, Colin Meek, Vishesh Khemani, Stefan Fulger, Pan Gu, Lakshminath Bhuvanagiri, Jason Hunter, Roberto Peon, Larry Kai, Alexander Shraer, Arif Merchant (Google), Kfir Lev-Ari (Technion). "Slicer: Auto-Sharding for Datacenter Applications." *12th USENIX Symposium on Operating Systems Design and Implementation (OSDI '16)*, Nov 2-4, 2016, Savannah, GA, pp. 739-753. ISBN 978-1-931971-33-1. | Yes | https://www.usenix.org/conference/osdi16/technical-sessions/presentation/adya (PDF: https://www.usenix.org/system/files/conference/osdi16/osdi16-adya.pdf) |
| **CEN** | Atul Adya (Google; work done at Microsoft), John Dunagan, Alec Wolman (Microsoft Research). "Centrifuge: Integrated Lease Management and Partitioning for Cloud Services." *7th USENIX Symposium on Networked Systems Design and Implementation (NSDI '10)*, April 2010, San Jose, CA. 16 pages. The USENIX PDF numbers its pages 1-16 and prints no proceedings page range. | Yes | https://www.usenix.org/conference/nsdi10-0/centrifuge-integrated-lease-management-and-partitioning-cloud-services (PDF: https://www.usenix.org/legacy/event/nsdi10/tech/full_papers/adya.pdf) |
| **SM** | Sangmin Lee, Zhenhua Guo, Omer Sunercan, Jun Ying, Thawan Kooburat, Suryadeep Biswal, Jun Chen, Kun Huang, Yatpang Cheung, Yiding Zhou, Kaushik Veeraraghavan, Biren Damani, Pol Mauri Ruiz, Vikas Mehta, Chunqiang Tang (Facebook Inc.). "Shard Manager: A Generic Shard Management Framework for Geo-distributed Applications." *Proceedings of the ACM SIGOPS 28th Symposium on Operating Systems Principles (SOSP '21)*, Oct 26-29, 2021, Virtual Event, Germany, pp. 553-569. DOI 10.1145/3477132.3483546. ISBN 978-1-4503-8709-5. (Author list and page range checked against Crossref.) | Yes | https://doi.org/10.1145/3477132.3483546. The text was read from the ACM open-access PDF as captured by the Internet Archive on 2025-04-17 (https://web.archive.org/web/20250417011159/https://dl.acm.org/doi/pdf/10.1145/3477132.3483546). On 2026-09-28 the direct ACM link and the research.facebook.com PDF both refused non-browser downloads. |

**Page numbers.** SLI and SM page numbers are the printed proceedings pages in the PDF footers. For SLI, PDF page 2 is p. 739, because PDF page 1 is the USENIX cover. For SM, PDF page 1 is p. 553. CEN page numbers are the footer numbers of the USENIX PDF, 1-16.

### Sources for §7.4–§7.7 WAS, Aurora, CockroachDB, Dynamo

| Key | Full citation | Peer-reviewed | URL / where obtained |
|---|---|---|---|
| **WAS** | Brad Calder, Ju Wang, Aaron Ogus, Niranjan Nilakantan, Arild Skjolsvold, Sam McKelvie, Yikang Xu, Shashwat Srivastav, Jiesheng Wu, Huseyin Simitci, Jaidev Haridas, Chakravarthy Uddaraju, Hemal Khatri, Andrew Edwards, Vaman Bedekar, Shane Mainali, Rafay Abbasi, Arpit Agarwal, Mian Fahim ul Haq, Muhammad Ikram ul Haq, Deepali Bhardwaj, Sowmya Dayanand, Anitha Adusumilli, Marvin McNett, Sriram Sankaran, Kavitha Manivannan, Leonidas Rigas. "Windows Azure Storage: A Highly Available Cloud Storage Service with Strong Consistency." *Proc. 23rd ACM Symposium on Operating Systems Principles (SOSP '11)*, Cascais, Portugal, Oct 23-26, 2011, pp. 143-157. DOI 10.1145/2043556.2043571. | Yes | https://doi.org/10.1145/2043556.2043571 (PDF read: https://sigops.org/s/conferences/sosp/2011/current/2011-Cascais/printable/11-calder.pdf) |
| **AUR18** | Alexandre Verbitski, Anurag Gupta, Debanjan Saha, James Corey, Kamal Gupta, Murali Brahmadesam, Raman Mittal, Sailesh Krishnamurthy, Sandor Maurice, Tengiz Kharatishvilli, Xiaofeng Bao. "Amazon Aurora: On Avoiding Distributed Consensus for I/Os, Commits, and Membership Changes." *Proc. 2018 International Conference on Management of Data (SIGMOD '18)*, Houston, TX, June 10-15, 2018, pp. 789-796. DOI 10.1145/3183713.3196937. | Yes | https://doi.org/10.1145/3183713.3196937 (PDF read: course-hosted copy https://pages.cs.wisc.edu/~yxy/cs839-s20/papers/aurora-sigmod-18.pdf; its footers 789-796 match Crossref) |
| **AUR17** | Alexandre Verbitski, Anurag Gupta, Debanjan Saha, Murali Brahmadesam, Kamal Gupta, Raman Mittal, Sailesh Krishnamurthy, Sandor Maurice, Tengiz Kharatishvili, Xiaofeng Bao. "Amazon Aurora: Design Considerations for High Throughput Cloud-Native Relational Databases." *Proc. 2017 ACM International Conference on Management of Data (SIGMOD '17)*, pp. 1041-1052. DOI 10.1145/3035918.3056101. Used only for segment, protection-group and control-plane facts. | Yes | https://doi.org/10.1145/3035918.3056101 (PDF read: https://homepages.cwi.nl/~boncz/lsde/papers/aurora.pdf; footers 1041-1052) |
| **CRDB** | Rebecca Taft, Irfan Sharif, Andrei Matei, Nathan VanBenschoten, Jordan Lewis, Tobias Grieger, Kai Niemi, Andy Woods, Anne Birzin, Raphael Poss, Paul Bardea, Amruta Ranade, Ben Darnell, Bram Gruneir, Justin Jaffray, Lucy Zhang, Peter Mattis. "CockroachDB: The Resilient Geo-Distributed SQL Database." *Proc. 2020 ACM SIGMOD International Conference on Management of Data (SIGMOD '20)*, pp. 1493-1509. DOI 10.1145/3318464.3386134. | Yes | https://www.cockroachlabs.com/pdf/cockroachdb-the-resilient-geo-distributed-sql-database-sigmod-2020.pdf |
| **DYN** | Giuseppe DeCandia, Deniz Hastorun, Madan Jampani, Gunavardhan Kakulapati, Avinash Lakshman, Alex Pilchin, Swaminathan Sivasubramanian, Peter Vosshall, Werner Vogels. "Dynamo: Amazon's Highly Available Key-value Store." *Proc. 21st ACM Symposium on Operating Systems Principles (SOSP '07)*, Stevenson, WA, Oct 14-17, 2007, pp. 205-220. DOI 10.1145/1294261.1294281. | Yes | https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf |
| **CRDB-DOCS-REPL** | Cockroach Labs. "Replication Layer", CockroachDB architecture documentation, "stable" channel (the page marks features "New in v26.3"). Retrieved 2026-09-28. Cited by section heading. | **No (NON-PEER-REVIEWED)** | https://docs.cockroachlabs.com/docs/stable/architecture/replication-layer |
| **CRDB-DOCS-LBS** | Cockroach Labs. "Load-Based Splitting", CockroachDB documentation, "stable" channel. Retrieved 2026-09-28. Cited by section heading. | **No (NON-PEER-REVIEWED)** | https://docs.cockroachlabs.com/docs/stable/load-based-splitting |
| **CRDB-MERGE-TN** | Nikhil Benesch. "Range merges", tech note in the cockroachdb/cockroach repository, `docs/tech-notes/range-merges.md` ("Last update: April 10, 2019"). Retrieved 2026-09-28. Cited by section heading. | **No (NON-PEER-REVIEWED)** | https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/range-merges.md |

**Page numbers.** WAS, AUR17, AUR18 and CRDB pages are the printed footer numbers, checked page by page and against the Crossref page ranges. The DYN PDF carries two stamped numbers on every page (205-220 and 195-210). This note uses 205-220, which matches the ACM/Crossref record.

---

## 0. Decision-relevant summary

1. **Two different units are called "cell".**
   - Physalia's cell is one consensus group per partition key, with 7 nodes [PHY §2.1-§2.2, pp. 465-466]. The authors "wanted millions" of them [PHY Abstract, p. 463].
   - AWS's cell is a complete copy of a workload with a capped size, behind a thin router (NON-PEER-REVIEWED) [CELL-WP pp. 5-9].
   - mantle's metadata *range* is the first kind. STATUS.md item 5's *cell*, which owns key ranges, is the second (§9.1).
2. **Cells cost what Tectonic set out to avoid.**
   - Tectonic replaced federated clusters because federation brings "the operational complexity of bin-packing datasets" and "resource-heavy data copying" to move data between instances. It names WAS as an example [TEC §7, p. 228].
   - WAS runs cells ("stamps") anyway [WAS §3.2-§3.4, §5.6, §8]:
     - 10-20 racks and up to 30 PB each;
     - a 70% utilization target;
     - accounts of at most 100 TB;
     - migration by asynchronous replication, then a clean failover and a DNS switch.
   - **Proposal:** make cells large, build the cell map and cross-cell moves from day one, and get most blast-radius control inside the cell (§9.2).
3. **A cell's size is bounded by its control plane.**
   - WAS caps a stamp at what its stream manager can hold in memory (50 M extents, 100 K streams, 32 GB) and derives its partition watermark from that [WAS §4.1, §5.5].
   - Shard Manager splits its own control plane into mini-SMs of up to about 50 K servers and 1.3 M shards [SM §6.1, §8.1].
   - AWS: "Small enough to test at full scale" (NON-PEER-REVIEWED) [CELL-WP p. 34] (§9.3).
4. **The router is thin and statically stable.**
   - **AWS (NON-PEER-REVIEWED)** [CELL-WP pp. 16, 23-28]:
     - the router holds the key-to-cell map in memory, contains no business logic, and keeps routing while the control plane is down;
     - mapping is a full table plus an override table, or a hash into tens of thousands of logical slots with an explicit slot-to-cell table.
   - **WAS:** the account name in DNS resolves to the stamp's VIP. The Location Service publishes it only after the stamp has accepted the account [WAS §2, §3.2].
   - **S3 (NON-PEER-REVIEWED):** DNS to any front end, and a TemporaryRedirect on a misroute that clients must not cache (§6.4).
5. **Stale routing may cost liveness, never correctness.**
   - Physalia model-checked that stale discovery-cache entries cannot cause split brain [PHY §4.3].
   - DynamoDB's storage nodes are authoritative. They answer a stale route with newer membership or an error [DDB §6.6].
   - Akkio, Centrifuge and TiKV also reject at the owner (§7.8.3, §7.2.3, note 06 §A4.4). WAS does not describe how its front-ends detect a stale partition map (§7.7.5).
6. **Keep caches constant-work.**
   - DynamoDB's whole-table routing cache had a 99.75% hit rate, which made metadata load bimodal and risked cascading failure. The fix refreshes on every hit, so load is constant [DDB §6.6].
   - AWS publishes full, fixed-size configuration in a loop for the same reason (NON-PEER-REVIEWED) [BL-CONSTANT pp. 2-7] (§3.5, §5.4).
7. **S3's general-purpose index is an ordered, range-partitioned keymap that splits for heat** (NON-PEER-REVIEWED) [S3-RRPC17; S3-BLOG12; S3UG-PERF; STG314-23] (§6.2-§6.3):
   - keys are stored in UTF-8 binary order;
   - partitions split on sustained load or on key count, sometimes into several children at once;
   - each partitioned prefix supports at least 3,500 write and 5,500 read requests per second;
   - S3 returns 503 SlowDown while it scales.

   Directory buckets instead use a hierarchical index with unsorted LIST [S3UG-DIRB] (§6.5). Together with WAS's peer-reviewed range-partitioned blob index [WAS §5.1, §8], this is new evidence for option B of note 01 §6.2 (§9.11).
8. **S3's strong consistency kept its caches and added an in-memory "witness"** that sees every write and acts as a read barrier (NON-PEER-REVIEWED) [VOGELS21; S3-BLOG20; PCASES] (§6.6).
   - It was verified with proofs, model checking and P, and has "no global dependencies".
   - In mantle, a range's leaseholder already sees every write (§6.8 item 4).
9. **Admission control belongs to the tenant, not the partition.**
   - DynamoDB's per-partition capacity caused throughput dilution on split and throttling of hot partitions. It moved to global admission control, which vends tokens to the routers, and kept per-partition caps as defense in depth [DDB §4-§4.2].
   - It splits for consumption at an observed key, and does not split for single-key or sequential patterns [DDB §4.4] (§3.6-§3.8).
10. **Splits, merges and moves inside a cell have exact published procedures.**
    - WAS [WAS §5.5] (§7.4.7):
      - it splits and merges by rewriting pointers to immutable extents;
      - it splits in place, updates the partition map, and only then moves a child;
      - a stamp sees about 75 splits and merges and 200 load balances per day.
    - CockroachDB merges (NON-PEER-REVIEWED) [CRDB-MERGE-TN] (§7.6.3) require:
      - aligned replica sets;
      - a frozen right-hand range;
      - unanimous acknowledgement;
      - a generation counter against ABA races.
11. **Moving data-plane replicas needs no consensus when there is one writer.**
    - Aurora replaces a segment by overlapping quorum sets under membership epochs. It fences old writers with a volume epoch "rather than waiting for a lease to expire" [AUR18 §2.4, §4.1].
    - mantle's single-writer blocks fit this model (§7.7.4 item 12).
    - **DERIVED:** WAS's rule of sealing at the shortest replica's length is unsafe under 2-of-3 acknowledgement (§7.7.4 item 13).
12. **Keep the control plane off the request path, and plan for planned events.**
    - Slicer routes on cached assignments even with its whole service down. 99.98% of 260 billion task selections succeeded, and 0.004% of requests were misrouted [SLI §4.3, §5.1.1].
    - Shard Manager servers read their assignments from ZooKeeper at startup [SM §3.2].
    - Planned container stops outnumber failures about 1000 to 1. With graceful primary handoff, request success stayed near 100%; without any graceful handling it fell below 90% [SM §1.1, §8.2].
    - Centrifuge is the counterexample: when its manager is down, all leases expire [SLI §6] (§7.1-§7.3).
13. **Balance with an explicit assignment and a churn budget.**
    - Slicer replaced load-aware consistent hashing after 18 months, when the hottest task ran 50% above the mean. Its greedy weighted-move algorithm moves at most 9% of the keyspace per round, and its constants were not "measured ... rigorously" [SLI §4.4.1, §4.4.4, §5.1.2].
    - Shard Manager uses a local-search solver with move budgets, and separate emergency and periodic modes [SM §5.1-§5.3] (§7.1.6, §7.3.5).
14. **For moves between cells, the steps are published but the fencing is not.**
    - AWS (NON-PEER-REVIEWED): clone, flip, redirect, forget. The details are "system-dependent" [CELL-WP p. 41].
    - WAS: asynchronous replication, then a "clean failover" [WAS §5.6].
    - Spanner: copy in the background, then a transaction for the "nominal amount" [SPN §2.2].
    - Akkio: fence with ACLs, plus locks and sequence-numbered movers [AKKIO §4.6.3-§4.6.5].
    - §9.6 proposes a fenced protocol for mantle that meets STATUS item 5's "no lost write and no stale read". It must be model-checked first.
15. **Shuffle sharding has no peer-reviewed analysis, and its math is elementary.**
    - The probability of full coverage is 1/C(n,k), but partially degraded tenants are common: 46% in AWS's own 8-worker example. Shard size must be retries + 1, and isolation is weak below about 16 workers (DERIVED) (§8.2-§8.3).
    - The 2014 AWS blog's "56" and "1/1680" are ordered counts; the correct figures are 28 and 1/70.
    - An Amazon patent applies overlap-limited sets to quorum replicas (NON-PEER-REVIEWED) [AMZN-PAT]. With q-of-k quorums, an overlap of at most k − q keeps every other set above quorum (§8.4.1).
16. **Model-check the protocols that move data.**
    - AWS's model checking found a data-loss bug in DynamoDB replication whose shortest trace was 35 steps, a bug in a data-migration feature, and in S3's background redistribution a bug followed by a bug in its first fix [FM pp. 69-72].
    - ShardStore's lightweight methods blocked 16 issues, 8 of them in reclamation or extent reset [SS Fig. 5] (§4, §2).

---

## 1. Physalia: "Millions of Tiny Databases" (Brooker, Chen, Ping, NSDI '20)

Physalia is the configuration store behind Amazon EBS replication. It is the peer-reviewed origin of AWS's "cell per partition" idea: one small consensus group per partition key, placed near the clients that need it. The paper is 16 pages (pp. 463–478) and was read in full from its text layer.

### 1.1 The problem it solves [PHY Abstract, §1, §1.1, pp. 463–464]

- **EBS replication needs a configuration master.** EBS replicates volumes with "a chain replication scheme (similar to the one described by van Renesse, et al)". In normal operation "replicated data flows through the chain from client, to primary, to replica, with no need for coordination". When failures occur, the scheme "requires the services of a configuration master, which ensures that updates to the order and membership of the replication group occur atomically, are well ordered, and follow the rules needed to ensure durability" [PHY §1, pp. 463–464].
- **Why this workload is unusual.** "In normal operation it handles little traffic ... However, when large-scale failures (such as power failures or network partitions) happen, a large number of servers can go offline at once, requiring the master to do a burst of work." The work "is latency critical, because volume IO is blocked until it is complete". It "requires strong consistency, because any eventual consistency would make the replication protocol incorrect". It "is also most critical at the most challenging time: during large-scale failures" [PHY §1, p. 464].
- **The outage that motivated it.** On 21 April 2011 "an incorrectly executed network configuration change triggered a condition which caused 13% of the EBS volumes in a single Availability Zone (AZ) to become unavailable." At that time "replication configuration was stored in the EBS control plane, sharing a database with API traffic". The quoted postmortem describes re-mirroring negotiations whose retries caused "a brown out of the EBS control plane" that "affected EBS APIs across the Region". "This failure vector was the inspiration behind Physalia's design goal of limiting the blast radius of failures, including overload, software bugs, and infrastructure failures" [PHY §1.1, p. 464].
- **Blast radius as a stated AWS goal.** "Not only do we work to make outages rare and short, we work to reduce the number of resources and customers that they affect ..., an approach we call blast radius reduction" [PHY §1, p. 463]. The cited source for this is a re:Invent 2018 talk (Vosshall, ref. [55]), which is not peer-reviewed.
- **The core observation.** "we do not require all keys to be available to all clients. In fact, each key needs to be available at only three points in the network: the AWS EC2 instance that is the client of the volume, the primary copy, and the replica copy" [PHY §1.2, p. 464]. Physalia's claimed contribution is that "infrastructure aware placement and careful system design can significantly reduce the effect of network partitions, infrastructure failures, and even software bugs" [PHY §1.2, p. 464].
- **Scale goal.** It should be "highly scalable, able to support an entire EBS availability zone in a single installation" [PHY §2, p. 465].

### 1.2 Nodes, cells and the colony: the cell-per-partition design [PHY §2.1, p. 465; Figs. 2–3]

- **Definitions.** "each Physalia installation is a colony, made up of many cells. The cells live in the same environment: a mesh of nodes, with each node running on a single server. Each cell manages the data of a single partition key, and is implemented using a distributed state machine, distributed across seven nodes. Cells do not coordinate with other cells, but each node can participate in many cells" [PHY §2.1, p. 465].
- **Cells are the blast-radius tool.** "The division of a colony into a large number of cells is our main tool for reducing radius in Physalia. Each node is only used by a small subset of cells, and each cell is only used by a small subset of clients" [PHY §2.1, p. 465].
- **One partition key per volume.** "Each EBS volume is assigned a unique partition key at creation time, and all operations for that volume occur within that partition key" [PHY §2.3, p. 466]. **DERIVED:** in the EBS deployment a cell is therefore the configuration of one volume. That is where "millions" of cells comes from: the paper's title and "instead of one database, build millions" [PHY §3.3, p. 470]. The paper gives no exact cell count.
- **Cells are fully independent.** The authors considered avoiding a separate control plane and repair workflow "by following the example of elastic replication or Scatter". They rejected this because "the additional complexity, and additional communication and dependencies between shards, were at odds with our focus on blast radius. We chose to keep our cells completely independent, and implement the control plane as a seperate [sic] system" [PHY §2.1, p. 465].

### 1.3 Cell size: why seven replicas [PHY §2.2, p. 466; Fig. 4]

"In the EBS installation of Physalia, the cell performs Paxos over seven nodes" [PHY §2.2, p. 466]. The paper gives four reasons:

- **Durability.** "Durability improves exponentially with larger cell size. Seven replicas means that each piece of data is durable to at least four disks, offering durability around 5000x higher than the 2-replication used for the volume data."
- **Tail latency.** "Cell size has little impact on mean latency, but larger cells tend to have lower high percentiles because they better reject the effects of slow nodes, such as those experiencing GC pauses."
- **Availability depends on the failure mode.** "smaller cells offer lower availability in the face of small numbers of uncorrelated node failures, but better availability when the proportion of node failure exceeds 50%. While such high failure rates are rare, they do happen in practice, and a key design concern for Physalia." The caption of Fig. 4 reads: "The size of cells is a trade-off between tolerance to large correlated failures and tolerance to random failures."
- **Cost.** "Larger cells consume more resources, both because Paxos requires O(cellsize) communication, but also because a larger cell needs to keep more copies of the data." The authors call this a minor concern for EBS because of the "relatively small transaction rate, and very small data".

### 1.4 Placement: near the clients, across failure domains [PHY §2.1, p. 465; §3.1–§3.2, pp. 468–469; §5.2, p. 473]

- **Two competing goals.** When a cell is created, the control plane uses "its knowledge of the power and network topology of the datacenter (discovered from AWS's datacenter automation systems) to choose a set of nodes for the cell". The choice balances two goals. "Nodes should be placed close to the clients (where close is measured in logical distance through the network and power topology) to ensure that failures far away from their clients do not cause the cell to fail. They must also be placed with sufficient diversity to ensure that small-scale failures do not cause the cell to fail" [PHY §2.1, p. 465].
- **What is optimized.** The control plane "optimizes the conditional probability P(Av|Ai)": the availability of the volume given that the client instance is available. Full co-location with the instance is ruled out because volumes must survive instance failure and be re-attachable [PHY §3.2, p. 469].
- **Worked example** [PHY §3.2, p. 469]. The datacenter has three network levels (servers, racks, rows) and three power domains. The client, primary and replica are on three racks in the same row. Physalia then places "all nodes for the cell ... within the row (there's no point being available if the row is down), but spread across at least three racks to ensure that the loss of one rack doesn't impact availability. It will also ensure that the nodes are in three different power domains, with no majority in any single domain."
- **Placement is continuous.** "EBS volumes move by replication, and their clients move by customers detaching their volumes from one instance and attaching them to another. The Physalia control plane continuously responds to these changes in state, moving nodes to ensure that placement constraints continue to be met" [PHY §3.2, p. 469].
- **Why cells beat a monolith** [PHY §3.1, pp. 468–469]:
  - A monolith "needs to make a trade-off between being localized to a small part of the network (and so risking being partitioned away from clients), or being spread over the network (and so risking suffering an internal partition making some part of it unavailable)".
  - "The monolith also increases blast radius: a single bad software deployment could cause a complete failure."
  - The authors concede the monolith's advantage: "No need for the discovery cache, most of the control plane, cell creation, placement, etc." and "Our experience has shown that simplicity improves availability".
- **Placement algorithm.** Global optimization "is not feasible", because the problem is "a non-convex optimization problem across huge numbers of variables" and because it "needs to be done online". In simulation, a rough heuristic, "a sort of bubble sort which swaps nodes between two cells at random if doing so would improve locality", with "20 candidates per cell", gave "significantly (up to 4x) reduced probability of losing availability" compared with a single-point database, under agg-layer partitions (Fig. 11, cell sizes 5 and 9) [PHY §5.2, p. 473].
- **Anti-correlated cell mixes.** "The control plane tries to ensure that each node contains a different mix of cells, which reduces the probability of correlated failure due to load or poison pill transitions. In other words, if a poisonous transition crashes the node software on each node in the cell, only that cell should be lost" [PHY §2.2, p. 466]. Deploying to "large numbers of nodes well-distributed across the datacenter" gives the control plane "more placement options" [PHY §2.2, p. 466].

### 1.5 Consensus, data model and API [PHY §2.2–§2.3, pp. 465–467]

- **Paxos.** Cells use Paxos "to create an ordered log of updates, with batching and pipelining". "Batch sizes and pipeline depths are kept small, to keep per-item work well bounded and ensure short time-to-recovery". The implementation is custom, written in Java, "which keeps all required state both in memory and persisted to disk". Because of the locality requirement, "extra care in implementation and testing were required to ensure that Paxos is implemented safely, even across dirty reboots" [PHY §2.2, pp. 465–466].
- **Optimistic proposals.** "All transactions given to the proposer are proposed, and at the time they are to be applied ... they are committed or ignored depending on whether the write conditions pass." The advantage is progress under the OCC pattern. The cost is "significant additional work during contention" [PHY §2.2, p. 466].
- **Data model.** Within each partition key, "a transactional store with a typed key-value schema, supporting strict serializable reads, writes and conditional writes over any combination of keys", plus "simple in-place operations like atomic increments". "The API can address only one partition key at a time" (Fig. 5) [PHY §2.3, p. 466]. The API is "inspired by the Amazon DynamoDB API", extended with "a compound read-and-conditional-write operation" [PHY §2.3, p. 467].
- **Determinism by construction** [PHY §2.3, p. 467]:
  - Supported field types are byte arrays, arbitrary-precision integers and booleans.
  - "Floating-point data types and limited-precision integers are not supported due to difficulties in ensuring that nodes will produce identical results when using different software versions and hardware."
  - The authors "chose not to offer a richer API (like SQL) for a similar reason".
- **Two consistency modes** [PHY §2.3, p. 467]:
  - In the consistent mode, reads and writes are "both linearizable and serializable". Most clients use this mode.
  - The eventually consistent mode is read-only. It offers "a consistent prefix" and "monotonic reads ... within a single client session", and is used for monitoring and for the discovery cache.
- **Leases** [PHY §2.3, p. 467]:
  - The API offers "first-class leases (lightweight time-bounded locks)", designed "to tolerate arbitrary clock skew and short pauses".
  - They "will give incorrect results if long-term clock rates are too different". In the implementation this means "the fastest node clock is advancing at more than three times the rate of the slowest clock".
  - "leases are only used where they are not critical for data safety or integrity."
- **Batching.** All keys and conditions are provided in the input transaction, so the proposer can batch safely. When a batch is rejected, it "can remove the offending change from the batch and re-submit" [PHY §2.3, p. 467].

### 1.6 Poison pills and the other safety mechanisms [PHY §2.2–§2.3, §2.6, §3.3–§3.5, §5.1]

- **Definition.** "A poison pill is a transaction which passes validation and is accepted into the log, but cannot be applied without causing an error. Pipelining requires that transactions are validated before the state they will execute on is fully known, meaning that even simple operations like numerical division could be impossible to apply." Poison pills "are typically caused by under-specification in the transaction logic ("what does dividing by zero do?", "what does it mean to decrement an unsigned zero?"), and are fixed by fully specifying these behaviors" [PHY §3.3, p. 470].
- **The expressiveness trade-off.** "Increasing API expressiveness ... increases the probability that the system will be able to accept a transition that cannot be applied (a poison pill)" [PHY §2.3, p. 467].
- **Correlated work is the general hazard.** Every replica processes the same updates in the same order, so replicas tend to "trigger the same bugs in each copy at the same time". Correlated load also fills memory and disks "at the same rate" [PHY §3.3, p. 469].
- **Deployments and quorums.** Because a replicated state machine tolerates failure of fewer than half its hosts, "failure may not be evident until new code is deployed to half of all hosts". Positive validation "reduce[s] but do[es] not eliminate this risk" [PHY §3.3, pp. 469–470].
- **Mechanisms, as listed across the paper:**
  1. **Different cell mixes per node,** so a poisonous transition kills one cell, not its neighbours [PHY §2.2, p. 466].
  2. **Colors** [PHY §3.4, p. 470]:
     - "Each cell is assigned a color, and each cell is constructed only of nodes of the same color."
     - "When software deployments and other operations are performed, they proceed color-by-color." Monitoring looks "for anomalies in single colors".
     - "Nodes of different colors don't communicate with each other, making it significantly less likely that a poison pill or overload could spread across colors."
     - The control plane spreads colors evenly so that "color choice minimally constrains how close a cell can be to its clients".
  3. **A restricted, deterministic API** (§1.5 above) [PHY §2.3, p. 467].
  4. **No partial understanding.** After a deployment-tool bug rolled a few nodes back to old code, those nodes silently skipped "a conditional they didn't understand" and state diverged. The lesson: "Postel's famous robustness principle ... does not apply to distributed state machines: they should not accept transactions they only partially understand and allow the consensus protocol to treat them as temporarily failed." Testing also had to "cover more than adjacent versions, and include strong mechanisms for testing rollback cases" [PHY §5.1, p. 472].
  5. **Control-plane restraint.** The corrupted cells looked empty, so "The control plane dutifully took action, deleting the cells." Physalia then added "rate limiting logic (don't move faster than the expected rate of change), and a big red button (allowing operators to safely and temporarily stop the control plane from taking action)". "control planes should exploit their central position in a systems architecture to offer additional safety" [PHY §5.1, p. 472].
  6. **Load shedding** [PHY §3.5, p. 470]:
     - "cells reject load once their pipelines are full". This is chosen because it "decreases attempted throughput with increased latency (and is therefore stable in the control theory sense)".
     - "Clients are expected to exponentially back off, apply jitter, and eventually retry".
     - "As the number of clients in the Physalia system is bounded, this places an absolute upper limit on load, at the cost of latency during overload."
  7. **Message integrity.** The system model assumes messages "can be arbitrarily lost, replayed, re-ordered, and modified after transmission". Every message carries "a cryptographic HMAC", and messages that fail authentication "are simply discarded". The model "extends the "benign faults" assumptions of Paxos slightly, but stops short of Byzantine fault tolerance": the authors chose simpler protocols plus "cryptographic integrity and authentication checks" [PHY §2.6, p. 468].

### 1.7 How the control plane creates, repairs and moves cells [PHY §2.1, p. 465; §2.4, pp. 467–468; §3.2, p. 469]

- **The three workflows.** "The cell creation and repair workflows respond to requests to create new cells (by placing them on under-full nodes), handling cells that contain failed nodes (by replacing these nodes), and moving cells closer to their clients as clients move (by incrementally replacing nodes with closer ones)" [PHY §2.1, p. 465].
- **Reconfiguration inside the cell.** It follows Lampson: per-cell configuration is stored "in the distributed state machine" and updated by "passing a transition with the existing jury". Because of pipelining, "configuration changes accepted at log position i must not take effect logically until position i + α, where α is the maximum allowed pipeline length". α is "typically 3", and Physalia "simply waits for natural traffic to cause reconfiguration to take effect (rather than stuffing no-ops into the log)". The authors call this "a very sharp edge in Paxos, which doesn't exist in either Raft or Viewstamped Replication" [PHY §2.4, p. 467; Fig. 6].
- **Moving by iterative reconfiguration** [PHY §2.4, p. 467; Fig. 7]:
  - "reconfiguration happens frequently. The colony-level control plane actively moves Physalia cells to be close to their clients ... by replacing far-away nodes with close nodes."
  - "The system prefers safety over speed, moving a single node at a time (and waiting for that node to catch up) to minimize the impact on durability."
  - Small cell data allows "movement to complete within a minute", typically.
- **Teaching (state transfer)** happens outside the consensus protocol, in three modes [PHY §2.4, pp. 467–468]:
  1. **Bulk:** a snapshot from any existing node, for new nodes.
  2. **Log-based:** ships a log segment, for re-joining nodes. It is "triggered rather frequently in production, due to nodes temporarily falling behind during Java garbage collection pauses", and is chosen when the missing segment is "significantly smaller than the size of the entire dataset".
  3. **"Whack-a-mole":** for persistent log holes. The learner proposes a no-op into the vacant position. This "is always safe in Paxos, but can affect liveness, so learners apply substantial jitter".
- **ABA hazard.** "Multiple reconfigurations also introduce an ABA problem when cells move off, and then back onto, a node." This was checked with TLA+ (§1.9) [PHY §4.3, p. 471].

### 1.8 Routing requests to the right cell: the discovery cache [PHY §2.5, p. 468; §4.3, p. 471]

- **Mechanism.** "Clients find cells using a distributed discovery cache", an "eventually-consistent cache which allow[s] clients to discover which nodes contain a given cell (and hence a given partition key). Each cell periodically pushes updates to the cache identifying which partition key they hold and their node members" [PHY §2.5, p. 468].
- **Safety split.** "Incorrect information in the cache affects the liveness, but never the correctness, of the system" [PHY §2.5, p. 468].
- **Three availability techniques** [PHY §2.5, p. 468]:
  1. **Client-side caching.** "it is always safe for a client to cache past discovery cache results, allowing them to refresh lazily and continue to use old values for an unbounded period on failure".
  2. **Forwarding pointers.** "Physalia nodes keep long-term (but not indefinite) forwarding pointers when cells move from node to node. Forwarding pointers include pointers to all the nodes in a cell, making it highly likely that a client will succeed in pointer chasing to the current owner provided that it can get to at least one of the past owners."
  3. **Replication.** "because the discovery cache is small, we can economically keep many copies of it".
- **Why stale routes are safe (TLA+-checked)** [PHY §4.3, p. 471]:
  - The property: "a client acting on stale information couldn't cause a split brain by allowing a group of old nodes to form a quorum."
  - The informal argument: reconfiguration makes "f > N/2 of the pre-reconfiguration nodes aware of a configuration change". So "at most N/2 nodes may have been deposed from the jury without being aware of the change", and they cannot form a quorum. (The fraction is flattened to "N2" in the text layer.)
  - The argument "becomes successively more complex as multiple reconfigurations are passed, especially during a single window of α". TLA+ and TLC were used to gain confidence.
- **DERIVED.** Physalia has no range directory and no central router on the request path. Routing is `partition key → cell membership` held in an eventually consistent, cell-populated cache. Correctness rests on the consensus group rejecting requests addressed to a superseded configuration.

### 1.9 Testing and formal methods [PHY §4, pp. 470–471]

- **SimWorld** [PHY §4.1, pp. 470–471]. Code is built "against abstract physical layers (such as networks, clocks, and disks)". In tests those layers are replaced with in-memory implementations with "rich fault-injection APIs", "control down to the packet level", disk-IO faults, corruption, and "each actor ... it's own view of time arbitrarily controlled by the test". "A typical test which tests correctness under packet loss can be implemented in less than 10 lines of Java code, and executes in less than 100ms"; the team wrote "hundreds of such tests".
- **Other methods** [PHY §4.2, p. 471]:
  - Automatically generated tests running "the Paxos implementation through every combination of packet loss and reordering that a node can experience", inspired by TLC.
  - Jepsen, to check that "API responses are linearizable under network failure cases".
  - Game days in production-like deployments.
- **TLA+** [PHY §4.3, p. 471]. It was used to write specifications, to model-check "correctness and liveness properties using the TLC model checker", and as documentation. That last use was "perhaps the most useful": "Our code reviews, simworld tests, and design meetings frequently referred back to the TLA+ models."

### 1.10 Evaluation [PHY §5, pp. 471–473]

- **Deployment.** "running in over 60 availability zones" [PHY §5.1, p. 472].
- **Availability gain** [PHY §5.1, p. 472]:
  - Fig. 8 shows first-try availability of the configuration store as seen by the EBS primary. Deploying Physalia "shows a clear (p = 7.7x10^-5) improvement".
  - Failures in the previous system came from infrastructure failures and transient overload.
  - Fig. 9 counts hours per month in which EBS masters exceeded an internal error-rate goal of 0.05%.
- **Throughput and latency** [PHY §5.1, p. 472]. Deployments at AZ scale "routinely serve thousands of requests per second". "Linearizable reads can sometimes be handled by the distinguished proposer". In a typical installation, "reads take less than 10ms at the 99th percentile, and writes typically take less than 50ms" (Fig. 10).
- **Simulation** [PHY §5.2–§5.2.1, p. 473]:
  - The placement heuristic gives up to 4x lower probability of losing availability (§1.4 above).
  - Under agg-router failures, offered load "increases linearly with the count of failed devices, up to maximum of 29%", then drops as volumes disconnect entirely. These results "closely match what we have observed" in production.
- **Not reported:** total cell counts, node counts, per-node cell counts, cell-creation rates, or time-to-repair distributions. **UNVERIFIED** if claimed elsewhere.

### 1.11 Implications for mantle (Physalia)

- **P1. A metadata range is a Physalia cell.** *INFERENCE.*
  - Physalia makes the consensus group the unit of data, isolation and placement, with no cross-cell coordination [PHY §2.1]. mantle's Raft-group-per-range is the same design (note 06 §A4).
  - Keep ranges fully independent. Cross-range work, such as Tectonic's cross-directory move (note 01 §1.6), must be client-orchestrated, never a group-to-group protocol. This matches Physalia's rejection of inter-shard dependencies [PHY §2.1].
- **P2. Replicas per range: 3 by default, and 5 where correlated failure is the concern.** *INFERENCE* from Fig. 4 and §2.2.
  - Physalia's 7 buys tail latency and durability for tiny data at low write rates.
  - mantle's metadata ranges hold far more data and write traffic, and Paxos/Raft cost is O(cell size) [PHY §2.2]. mantle's data is also durable on the chunk layer, not in the metadata group.
  - The replica count is a per-layer policy decision, not a copy of Physalia's 7.
- **P3. Give nodes different mixes of ranges.** *INFERENCE* [PHY §2.2].
  - If two ranges share exactly the same replica set, a poison command that crashes those nodes takes out both.
  - For Raft groups, the placement driver should therefore bound how many nodes any two replica sets share. With 3 replicas and 2-of-3 quorums, an overlap of at most 1 keeps every other range above quorum when one range's nodes all fail (§8.4.1).
  - That bound can coexist with the small family of sets that copysets want for durability (note 01 §1.10; §8.4). Ranges placed on the same set still share its fate.
- **P4. Deterministic, fully specified state-machine commands.** *INFERENCE* [PHY §2.3, §3.3, §5.1]:
  - Every command's outcome is fully specified, including overflow, underflow, missing keys and unknown fields. mantle's no-panic rule (CLAUDE.md §1) already makes errors typed values; the state machine must turn them into *deterministic rejected outcomes*, never divergent behavior.
  - No floating point in replicated state.
  - A replica that meets a command version it does not understand must refuse to apply it, blocking that group, rather than skip it [PHY §5.1].
  - This matters more under focal's fast track. There, "an entry must be a request every member evaluates" (note 07 §7.3), so every replica evaluates commands itself, exactly the setting in which Physalia's poison pills and version skew arose.
- **P5. Deployment colors = rollout waves that are also placement constraints.** *INFERENCE* [PHY §3.4].
  - Assign each node a color. Build each metadata range's replicas from one color. Deploy color by color.
  - This needs at least `replicas × failure-domains` nodes per color. On small clusters and on a laptop there is exactly one color, and the mechanism degenerates to nothing.
- **P6. Stale routing must affect liveness only.** *INFERENCE* [PHY §2.5, §4.3].
  - Clients may cache range locations indefinitely. Nodes keep bounded forwarding pointers after a move. A range replica that is not in the current configuration, or holds a stale descriptor, must reject rather than serve.
  - Specify and model-check the stale-cache and ABA cases (a range moving off and back onto a node) in TLA+ before implementing moves.
- **P7. Overload is shed at the cell, clients back off.** *INFERENCE* [PHY §3.5]. A range whose proposal pipeline is full returns a typed `Busy`, which matches mantle's bounded-queue rule (CLAUDE.md §2). Clients back off exponentially with jitter. The gateway's retry budget bounds total load.
- **P8. The control plane must be rate-limited and stoppable.** *INFERENCE* [PHY §5.1].
  - Destructive placement actions, such as deleting replicas or ranges or garbage-collecting "empty" cells, proceed no faster than the expected rate of change.
  - There is an operator stop switch.
  - The same logic already gives the chunk store its 3-day deletion grace (docs/design/chunk-store.md §8).
- **P9. Moves go one replica at a time, catch-up first.** *INFERENCE* [PHY §2.4]. Mantle uses Raft, not Paxos, so the α-window sharp edge does not apply. Joint consensus is the recommended replacement for single-member changes (note 06 §A4.3).
- **P10. Testing.** SimWorld [PHY §4.1] is the same method as note 06 C.d (deterministic simulation with injectable network, clock and disk). Physalia's exhaustive packet-loss/reordering enumeration [PHY §4.2] is a cheap complement for the Raft core.

### 1.12 UNVERIFIED / not found (Physalia)

- The number of cells, nodes per colony, or cells per node in production: not stated.
- The exact placement constraint set beyond the idealized row/rack/power example [PHY §3.2]: not stated. The paper says real topologies are "significantly more complex".
- How colors are assigned or how many colors exist: not stated.
- The discovery cache's implementation, refresh period, and forwarding-pointer lifetime: not stated beyond "long-term (but not indefinite)".
- The content of the re:Invent 2018 blast-radius talk (ref. [55]): not reviewed. Not peer-reviewed.

---

## 2. ShardStore: S3's per-disk storage node and how AWS validates it (Bornholt et al., SOSP '21)

**Reading notes.** The author copy has no printed page numbers. Crossref gives pp. 836-850, which maps one-to-one onto the copy's 15 pages (PDF p. N = printed p. 835+N), and the tags below use the printed numbers. The PDF text layer replaces some punctuation with other glyphs (closing quotes, en and em dashes, the section sign). Quotes below restore the intended characters and rejoin words hyphenated across lines.

### 2.1 What ShardStore is and where it sits in S3

- **Role.** "At the core of S3 are storage node servers that persist object data on hard disks. These storage nodes are key-value stores that hold shards of object data, replicated by the control plane across multiple nodes for durability." [SS §1, p. 836]
- **No replication inside a node.** "Each storage node stores shards of customer objects, which are replicated across multiple nodes for durability, and so storage nodes need not replicate their stored data internally." [SS §2, p. 837]
- **Keys and values.** "keys are shard identifiers and values are shards of customer object data. Customer requests are mapped to shard identifiers by S3's metadata subsystem" [SS §2.1, p. 837]. SS's citation for the metadata subsystem is Vogels's 2021 blog post (ref. 53), which is NON-PEER-REVIEWED and covered in §6.
- **One store per disk, one router per host.** "ShardStore runs on storage hosts with multiple HDDs. Each disk is an isolated failure domain and runs an independent key-value store. Clients interact with ShardStore through a shared RPC interface that steers requests to target disks based on shard IDs." [SS §2.1, p. 838]
- **Rollout behind an unchanged API:**
  - ShardStore "is being gradually deployed within our current service" and "currently stores hundreds of petabytes of customer data as part of a gradual rollout" [SS §1, p. 836].
  - "ShardStore is API-compatible with our existing storage node software, and so requests can be served by either ShardStore or our existing key-value stores." [SS §2, p. 837]
- **Size and rate of change.**
  - "over 40,000 lines of Rust code" [SS §1, p. 836]. Fig. 6 counts 44,048 implementation lines [SS Fig. 6, p. 847].
  - It is "developed and operated by a team of engineers whose changes are continuously deployed worldwide" [SS §1, p. 836].
- **S3 around it:**
  - "Amazon S3 is designed for eleven nines of data durability, and replicates object data across multiple storage nodes" [SS §2.2, p. 839].
  - S3 is "a complex distributed system with hundreds of microservices, several of which interact with ShardStore storage nodes to replicate customer data and perform control plane operations" [SS §8.4, pp. 847-848].
  - AWS uses "the P language for asynchronous programs ... to validate the correctness of new S3 features such as strong consistency", and "TLA+ to validate the designs of a number of systems" [SS §8.4, p. 848]. The strong-consistency reference is a video talk (ref. 36, NON-PEER-REVIEWED).
- **What Warfield adds [WARFIELD23]. NON-PEER-REVIEWED:**
  - ShardStore was a rewrite of "the bottom-most layer of S3's storage stack – the part that manages the data on each individual disk".
  - The team moved "the implementation to Rust in order to get type safety and structured language support to help identify bugs sooner, and even wrote libraries that extend that type safety to apply to on-disk structures".
  - The model is "about 1% of the size of the real system".
  - The tools run "with every single commit to the software".
  - He gives an HDD random-access budget of "about 120 operations per second".
  - The post gives no ShardStore deployment figures.

**Terminology across systems (DERIVED).** Each row compares like with like.

| ShardStore [SS §2.1] | mantle (docs/design/chunk-store.md) | Tectonic (note 01 §1.3) |
|---|---|---|
| **shard**: the value stored under a key, one shard of an object | **chunk**: one replica or EC shard of a block, keyed `(block id, epoch, chunk index)` (§5) | **chunk** |
| **chunk**: framed on-disk unit; a shard is "one or more chunks" | **data record**: a whole chunk or one appended fragment (§3.1) | chunk stored as a file on XFS |
| **extent**: contiguous, append-only, write pointer, reset | **segment**: 256 MiB, written sequentially, freed and reused under a new incarnation (§2, §8) | none (XFS allocates) |
| **superblock** in extent 0, holding soft write pointers | **superblocks A/B**, holding a checkpoint pointer and a sequence number (§2, §5) | none |
| **LSM-tree index**, itself stored as chunks on extents | **in-memory index**, plus an index log and checkpoints (§3.2, §5) | none |
| **reclamation** | **cleaning** (§8) | none |
| independent key-value store per disk; request plane is put/get/delete, control plane covers migration, repair, listing and bulk operations | one volume per device (§1) | storage node with 36 HDDs and one local XFS instance; API is get/put/append/delete plus list/scan (note 01 §1.3) |

### 2.2 On-disk design [SS §2.1, pp. 837-838; Fig. 1]

- **Index with values kept outside it.** "ShardStore's key-value store comprises a log-structured merge tree (LSM tree) but with shard data stored outside the tree to reduce write amplification, similar to WiscKey." "The LSM tree maps each shard identifier to a list of (pointers to) chunks, each of which is stored within an extent." [p. 837]
- **Extents:**
  - "Extents are contiguous regions of physical storage on a disk; a typical disk has tens of thousands of extents."
  - "ShardStore requires that writes within each extent are sequential, tracked by a write pointer defining the next valid write position, and so data on an extent cannot be immediately overwritten. Each extent has a reset operation to return the write pointer to the beginning of the extent and allow overwrites." [p. 837]
  - Neither extent size nor disk size is given.
- **No single shared log, by choice.** "Rather than centralizing all shard data in a single shared log on disk, ShardStore spreads shard data across extents. This approach gives us flexibility in placing each shard's data on disk to optimize for expected heat and access patterns (e.g., to minimize seek latency). However, the lack of a single log makes crash consistency more complex" [p. 838].
- **Chunk store:**
  - "All persistent data is stored in chunks, including the backing storage for the LSM tree itself."
  - "The chunk store offers PUT(data) → locator and GET(locator) → data interfaces, where locators are opaque chunk identifiers and used as pointers. A single shard comprises one or more chunks depending on its size." [p. 838]
- **Reclamation (garbage collection).** "Reclamation selects an extent and scans it to find all chunks it stores. For each chunk, reclamation performs a reverse lookup in the index (the LSM tree); chunks that are still referenced in the index are evacuated to a new extent and their pointers updated in the index ..., while unreferenced chunks are simply dropped. Once the entire extent has been scanned, its write pointer is reset and it is available for reuse." [p. 838]
- **Why the ordering matters.** "Resetting an extent's write pointer makes all data on that extent unreadable even if not yet physically overwritten (ShardStore forbids reads beyond an extent's write pointer), and so the chunk store must enforce a crash-consistent ordering for chunk evacuations, index updates, and extent resets." [p. 838]
- **Reclaiming the index's own chunks.** Chunks left unused by LSM compaction are reclaimed the same way, "except that the reverse lookup is into the LSM tree's metadata structure (stored on disk in a reserved metadata extent) that records locators of chunks currently in use by the tree." [p. 838]
- **Append-only I/O on both zoned and conventional disks.** "To support both zoned and conventional disks, ShardStore provides its own implementation of the extent append operation in terms of the write system call. It does this by tracking in memory a soft write pointer for each extent, internally translating extent appends to write system calls accordingly, and persisting the soft write pointer for each extent in a superblock flushed on a regular cadence." [p. 838] Fig. 2b labels extent 0 as the superblock [p. 839]. The cadence is not given.
- **Chunk framing.** "Chunk data is framed on disk with a two-byte magic header ... and a random UUID, repeated on both ends to allow validating the chunk's length." [SS §5, pp. 843-844]
  - Checksums appear only in passing: components should "detect and fail operations that involve corruption even in the presence of IO errors (e.g., by validating checksums)" [SS §4.4, p. 843].
  - The checksum algorithm and its granularity are **UNVERIFIED**.

### 2.3 Crash consistency: soft updates expressed as a dependency graph [SS §2.2, pp. 838-839; Fig. 2]

- **Why soft updates.** "A soft updates implementation orchestrates the order in which writes are sent to disk to ensure that any crash state of the disk is consistent. Soft updates avoid the cost of redirecting writes through a write-ahead log and allow flexibility in physical placement of data on disk." [p. 838]
- **Making the orderings declarative.** "Correctly implementing soft updates requires global reasoning about all possible orderings of writebacks to disk. To reduce this complexity, ShardStore's implementation specifies crash-consistent orderings declaratively, using a Dependency type to construct dependency graphs at run time that dictate valid write orderings." [p. 838]
- **The one write path.** "ShardStore's extent append operation, which is the only way to write to disk, has the type signature: `fn append(&self, ..., dep: Dependency) -> Dependency`." [p. 838]
- **The API contract:**
  - "the append will not be issued to disk until the input dependency has been persisted. ShardStore's IO scheduler ensures that writebacks respect these dependencies."
  - Returned dependencies can be passed to later appends or combined, for example `dep1.and(dep2)`.
  - "Dependencies also have an is_persistent operation that clients can use to poll the persistence of an operation." [pp. 838-839]
- **What one put writes (Fig. 2).** Each put's graph has three writes:
  1. "the shard data is chunked and written to an extent";
  2. "the index entry for the put is flushed in the LSM tree";
  3. "the metadata for the LSM tree is updated to point to the new on-disk index data".

  In addition, "every time ShardStore appends to an extent it also updates the corresponding soft write pointer in the superblock (extent 0)." [p. 839]
- **Coalescing at run time:**
  - Puts #1 and #2 were allocated to the same extent, "so their writebacks can be coalesced into one IO by the scheduler, and thus their soft write pointer updates are combined into the same superblock update". Put #3's extent needs a separate pointer update.
  - All three puts share one LSM-tree flush. It writes a new chunk of LSM data on extent 12 and then updates the LSM metadata. The text says this metadata is "on chunk 9"; Fig. 2 labels it extent 9. [p. 839]
  - Fig. 2 caption: "Each put is only durable once both the shard data and the index entry that points to it are durable." [p. 839]
  - The direction of the edges in Fig. 2a cannot be recovered from the text layer.
- **Why a replicated system bothers with crash consistency.** "single-node crash consistency issues do not cause data loss. We instead see crash consistency as reducing the cost and operational impact of storage node failures. Recovering from a crash that loses an entire storage node's data creates large amounts of repair network traffic and IO load across the storage node fleet." It also keeps a node from needing "manual operator intervention" after a crash [SS §2.2, p. 839].
- **DERIVED reading of the bug table.** Issue #8 is "Writes did not include a dependency on the soft write pointer update" [Fig. 5, p. 847]. Together with the rule that reads beyond the write pointer are forbidden [p. 838], this implies a chunk counts as persistent only once the soft write pointer covering it is also persistent.

### 2.4 Request plane and control plane [SS §2.1, p. 838; §8.3, p. 846; §8.4, pp. 847-848; Fig. 5, p. 847]

- **The split.** "The RPC interface provides the usual request-plane calls (put, get, delete) and control-plane operations for migration and repair." [p. 838] Replication happens above the node: shards are "replicated by the control plane across multiple nodes" [p. 836].
- **Other control-plane operations, known only from the bug table [Fig. 5]:**
  - "listing and removal of shards" (#13, a race);
  - "bulk operations for creating and removing shards" (#16, a race);
  - "a disk was removed from service and then later returned" (#4, lost shards).
- **The gap the authors name.** "We also do not have a complete reference model (§3.2) for some operations ShardStore exposes to S3's control plane. As future work, we plan to model the parts of these control plane interactions that are necessary to establish durability properties." [SS §8.3, p. 846]
- **Not stated.** The full control-plane API, and how the RPC layer maps shard IDs to disks, are **UNVERIFIED**.

### 2.5 The validation method [SS §3-§7, pp. 839-845]

**2.5.1 Properties, and how they are split up [SS §3.1, pp. 839-840].**
- **Out of scope: availability and performance.** S3 establishes these by other means: "integration tests, load testing in pre-production environments, and staggered deployments with monitoring in production". That last reference is a Builders' Library article (NON-PEER-REVIEWED).
- **The durability property.** "the model and implementation remain in equivalent states after each API call. Since the system is a key-value store, we define equivalence as having the same key-value mapping."
- **Why it must be split.** Crashes lose data the model does not allow, and concurrency overlaps operations. The property is therefore checked in three parts:
  1. sequential crash-free executions, by direct equivalence (§4 of SS);
  2. sequential crashing executions, against a model "extend[ed] ... to define which data can be lost after a crash" (§5);
  3. "concurrent crash-free executions, we write separate reference models and check linearizability" (§6).
- **Not checked at all.** "(We do not currently check properties of concurrent crashing executions because we have not found an effective automated approach.)" [p. 840]

**2.5.2 Reference models [SS §1, p. 837; §3.2, p. 840].**
- **What a model is.** "an executable specification in Rust that provides the same interface as the component but using a simpler implementation". The index model "uses a simple hash table" in place of the LSM tree.
- **Size.** Models are "1% of the implementation code" [p. 837]; Fig. 6 counts 450 lines.
- **Failures are left out.** Models "can fail in limited ways (e.g., reads of keys that were never written should fail), but we choose to omit other implementation failures (IO errors, resource exhaustion, etc.) from the models." The cost is that "most availability or performance properties" cannot be reasoned about.
- **Why Rust, not a modelling language.** Alloy, Promela and P were considered. Writing "reference models in the same language as the implementation ... make[s] them easier for engineers to keep up to date."
- **Models double as mocks.** "unit tests at ShardStore's API layer use the index reference model (a hash map) as a mock of the index component". This "helps keep the reference models up-to-date over time".
- **Verifying the models themselves.** Experiments proving properties of the models with the Prusti verifier gave "limited but positive" experience.

**2.5.3 Conformance by property-based testing [SS §4.1, p. 841; Fig. 3].**
- **The check.** The implementation must refine the model: "any observable behavior of the implementation must be allowed by the model."
- **The harness:**
  - The test input is a sequence of operations drawn from a defined alphabet. Each operation is applied to both the model and the implementation, the outputs are compared, and invariants relating the two are checked after every step.
  - "the implementation under test uses an in-memory user-space disk, but all components above the disk layer use their actual implementation code."
- **Background work is in the alphabet.** The index alphabet includes `Reclaim` and `Reboot`. These are "no-ops in the reference model ... but including them validates that their implementations do not corrupt the index."
- **Scale.** "we routinely run tens of millions of random test sequences before every ShardStore deployment" [SS §4.2, p. 841]. The checks are "pay-as-you-go": run longer to find more, both locally and "at scale before deployments" [SS §1, p. 837].

**2.5.4 Reaching interesting states [SS §4.2, pp. 841-842].**
- **Argument bias.**
  - `Get` prefers keys that were `Put` earlier.
  - Sizes are biased toward "read/write sizes close to the disk page size, which in our experience are frequent causes of bugs". Bias is always probabilistic.
- **What did not help.** "we experimented with replicating production object size and Get/Put ratio distributions with no effect." The rule they settled on: "trusting default randomness wherever possible, and only introducing bias where we have quantitative evidence that it is beneficial".
- **Coverage.** The harnesses emit code-coverage metrics "to help us identify blind spots", such as new functionality the model does not know about.

**2.5.5 Minimization and determinism [SS §4.3, p. 842].**
- **An example.** For issue #9, "the first random sequence that failed the test had 61 operations, including 9 crashes and 14 writes totalling 226 KiB of data; the final automatically minimized sequence had 6 operations, including 1 crash and 2 writes totalling 2 B of data." Stock shrinking heuristics sufficed.
- **Two design rules make shrinking work:**
  1. Components are "as deterministic as possible". Rust's default `HashMap` hashing is randomized, and that is the example they give of non-determinism creeping in.
  2. Because the tool (proptest, SS ref. 30) prefers earlier enum variants when minimizing, "we arrange operation alphabets in increasing order of complexity."

**2.5.6 Failure injection [SS §4.4, pp. 842-843].**
- **Three failure classes:**
  1. "Fail-stop crashes (e.g., power outages, kernel panics)";
  2. "Transient or permanent disk IO failures (e.g., HDD failures, timeouts)";
  3. "Resource exhaustion (e.g., out of memory or disk space)".
- **I/O failures are operations.** For example, `FailDiskOnce(ExtentId)` makes the next I/O to that extent fail.
- **A relaxed check after a failure.** An operation's I/Os are not atomic with respect to an injected failure, so a "has failed" flag relaxes equivalence: "a Get operation with an injected IO error is allowed to fail by returning no data, but is never allowed to return the wrong data".
- **Two separate runs.** Tests run "with and without failure injection enabled" so the relaxation cannot hide unrelated bugs.
- **Resource exhaustion is not tested.** There is no oracle for it. Telling a space leak from expected overhead needs accounting for amplification that is "intrinsic to the implementation and difficult to compute abstractly".

**2.5.7 Crash consistency [SS §5, pp. 843-844].**
- **Two properties, defined over `Dependency`:**
  1. **persistence**: "if a dependency says an operation has persisted before a crash, it should be readable after a crash (unless superseded by a later persisted operation)";
  2. **forward progress**: "after a non-crashing shutdown, every operation's dependency should indicate it is persistent".

  Forward progress "rules out dependencies being so strong that they never complete". Dependencies stronger than necessary are allowed.
- **How crash states are generated.** A `DirtyReboot(RebootType)` operation chooses, per component, whether its volatile state is flushed by the crash (for example the LSM tree's in-memory section, or the buffer cache). Per-component flush operations such as `IndexFlush` can be interleaved, so partly flushed states arise.
- **Block-level enumeration was tried.** "Coarse flushes can miss bugs" compared with exhaustive block-level enumeration. A block-level `DirtyReboot` variant similar to BOB and CrashMonkey exists, but "this exhaustive approach has not found additional bugs and is dramatically slower to test, so we do not use it by default."
- **Worked example, issue #10** (found by a developer running the tests locally "before even submitting for code review"):
  1. A chunk's trailing UUID spilled onto a second page. A crash lost that page. The chunk was never considered persistent, so no violation yet.
  2. After recovery, a new chunk was written from the write pointer, which lay inside the torn chunk's span, and was flushed. It was now persistent.
  3. The lost UUID bytes happened to equal the magic bytes. Reclamation's scan therefore decoded the torn chunk "successfully", skipped the overlapping second chunk, and reset the extent.
  4. A persistent chunk was lost.

  It needed "a particular choice of random UUID to collide with the magic bytes, a chunk that was the right size to just barely spill onto a second page, and a crash that lost only the second page" [pp. 843-844].

**2.5.8 Concurrency: stateless model checking with Loom and Shuttle [SS §6, pp. 844-845; Fig. 4].**
- **Why it is needed in Rust.** Rust guarantees data-race freedom in safe code "but cannot make guarantees about higher-level race conditions (e.g., atomicity violations)".
- **The method.** Harnesses are hand-written and checked by stateless model checking, which explores interleavings and also finds deadlocks ("interleavings that end with all threads blocked").
- **Loom, for small code.** Loom "implements the CDSChecker algorithm for sound model checking in the release/acquire memory model, and uses bounded partial-order reduction". It does not scale to end-to-end tests: "even a relatively small test involves tens of thousands of atomic steps ..., and the largest tests involve over a million steps."
- **Shuttle, for large tests.** AWS therefore "developed and open-sourced the stateless model checker Shuttle, which implements randomized algorithms such as probabilistic concurrency testing". "The two tools offer a soundness–scalability trade-off." Loom checks "small, correctness-critical code such as custom concurrency primitives", and Shuttle checks "end-to-end stress tests of the ShardStore stack".
- **Linearizability versus what the example checks.** SS says it applies stateless model checking "to show that the implementation is linearizable" [§1, p. 837] and "In principle, we would like to check" linearizability [§6, p. 844]. The only harness shown (Fig. 4) checks read-after-write on a fixed history. Its three threads are:
  1. reclamation of one extent;
  2. LSM compaction;
  3. a thread that overwrites keys and reads them back.

  The persistent chunk store is mocked "as a conceit to scalability". No general linearizability checker is described (**UNVERIFIED** whether one is used).
- **Worked example, issue #14:**
  1. Compaction wrote a new LSM chunk into extent 0.
  2. Before compaction recorded the chunk in the in-memory LSM metadata, reclamation scanned extent 0. It evacuated the chunks the metadata referenced, dropped the unreferenced new chunk, and reset the extent.
  3. Compaction then published a dangling pointer, and the index entries on that chunk were lost.

  "The fix was to make compaction lock the extents it writes new chunks into until it can update the metadata to point to them." [pp. 844-845]

**2.5.9 Localized properties [SS §7, p. 845].**
- **Undefined behavior.** The unsafe Rust, "mostly for interacting with block devices", is covered by running the test suite under Miri. AWS extended Miri for threads and locks and "upstreamed that work".
- **Deserializers.** Data read from disk is treated as untrusted. With Crux (bounded symbolic evaluation) they "proved that for any sequence of on-disk bytes (up to a size bound), our deserializers cannot panic", and they fuzz the same code on larger inputs.

### 2.6 What it found and what it cost [SS §8.1-§8.2, pp. 846-847; Figs. 5-6]

**Figure 5: "ShardStore issues prevented from reaching production by our validation effort."** Reproduced exactly [p. 847].

| ID | Component | Description |
|---|---|---|
| *Functional correctness* | | |
| #1 | Chunk store | Off-by-one error in reclamation for chunks of size close to PAGE_SIZE |
| #2 | Buffer cache | Cache was not correctly drained after resetting an extent |
| #3 | Index | Metadata was not flushed correctly during shutdown if an extent was reset |
| #4 | API | Shards could be lost if a disk was removed from service and then later returned |
| #5 | Chunk store | Reclamation could forget chunks after a transient read IO error |
| *Crash consistency* | | |
| #6 | Superblock | Superblock Dependency for extent ownership was incorrect after a reboot |
| #7 | Superblock | Mismatch between soft and hard write pointers in a crash after an extent reset |
| #8 | Buffer cache | Writes did not include a dependency on the soft write pointer update |
| #9 | Chunk store | Reference model was not updated correctly after a crash during reclamation |
| #10 | Chunk store | Reclamation could forget chunks after a crash and UUID collision |
| *Concurrency* | | |
| #11 | Chunk store | Chunk locators could become invalid after a race between write and flush |
| #12 | Superblock | Buffer pool exhaustion could cause threads waiting for a superblock update to deadlock |
| #13 | API | Race between control plane operations for listing and removal of shards |
| #14 | Index | Race between reclamation and LSM compaction could lose recent index entries |
| #15 | Chunk store | Reference model could re-use chunk locators, which other code assumed were unique |
| #16 | API | Race between control plane bulk operations for creating and removing shards |

- **What kind of issues they were.** "Most issues related to data integrity—they could cause data to be lost or stored incorrectly—while one concurrency issue (#12) was an availability issue caused by a deadlock." [p. 846]
- **What the list undercounts.** It includes "only issues that reached (and were blocked by) our continuous integration pipeline". Anecdotally, more were caught during local development. Diagnosis "remains a developer-intensive manual effort" [p. 846].
- **DERIVED counts from the table:**
  - 5 functional-correctness, 5 crash-consistency and 6 concurrency issues.
  - 8 of 16 name reclamation or an extent reset (#1, #2, #3, #5, #7, #9, #10, #14).
  - 2 of 16 are races between control-plane operations (#13, #16).
  - 2 of 16 concern the reference model itself (#9, #15).

**Figure 6: lines of code, all Rust** [p. 847].

| Component | Lines |
|---|---|
| Implementation | 44,048 |
| Unit tests and integration tests | 19,540 |
| Reference models (§3.2) | 450 |
| Functional correctness checks (§3) | 4,860 |
| Crash consistency checks (§5) | 2,661 |
| Concurrency checks (§6) | 901 |
| Total | 72,460 |

- **Validation overhead:**
  - The text says the artifacts are "only 13% of the total code base and 20% of the size of the implementation code", against "3–10× overhead" reported by formal-verification projects [p. 846]. §1 says 12% [p. 837].
  - DERIVED from Fig. 6: models plus checks come to 8,872 lines, which is 12.2% of 72,460 and 20.1% of 44,048. The 13% figure does not follow from the table.
  - For comparison, VeriBetrKV needs "7 lines of proof for every line of implementation" [SS §9, p. 848].
- **Who did the work:**
  - "two formal methods experts working full-time for nine months and a third expert who joined for three months". Since then, most of the work has passed to engineers "none of whom have any prior formal methods experience".
  - "18% of the lines of code in the test harnesses were last edited by a non-formal-methods expert according to git blame". §1 words the same number as "written by the engineering team" [p. 837].
  - Three engineers have written more than 100 lines each, and four have written new stateless-model-checking harnesses. The need for such harnesses "is now a standard question during code review of new concurrent functionality" [pp. 846-847].
- **Plain unit tests.** The validation tests are ordinary Rust unit tests, "distinguished from other tests only by naming conventions and module hierarchy" [p. 846].

### 2.7 Limits the authors state, and their lessons [SS §8.3-§8.4, pp. 846-848]

- **Passing proves nothing.** "their reporting success does not mean the code is correct, only that they could not find a bug."
- **One known miss.** A bug on a cache-miss path went unfound because "the cache size was configured to be very large in all tests". After the cache was shrunk, the tests found it. This miss motivated the coverage metrics [p. 846].
- **Not yet validated.** "parsing of S3's messaging protocol, request routing, and business logic", and the control-plane operations that lack complete models [p. 846].
- **Model each component, early.** They modelled components one at a time as their APIs stabilized. A single model of the public interface "would have been the wrong decision and would have caught fewer bugs", because fault scenarios are easier to reach through internal component APIs [p. 847]. Having seen the early results, engineers asked for the tests to become "release blockers for continuous delivery" [p. 846].
- **Keep the models in the build.** "Developers must update the models whenever code changes in order to avoid breaking the build." Early models written in Alloy, SPIN and "Yggdrasil-style Python" were dropped for Rust after "we discussed long-term maintenance implications with the team". Success meant future changes would not need "new formal methods engagements" [p. 847].
- **Next step: the distributed system.** Their next target is the distributed-system level, "combining P with Rust" to reason about ShardStore's role in S3 [p. 848].

### 2.8 The tools today (NON-PEER-REVIEWED, observed 2026-09-28)

- **Shuttle [SHUTTLE].**
  - Status: Apache-2.0, not archived. Latest crate 0.9.4, released 2026-09-22 (0.9.x releases in April, August and September 2026). Last push 2026-09-28.
  - The README describes "randomized concurrency testing techniques, including" PCT, and says "Shuttle is not sound (a passing Shuttle test does not prove the code is correct), but it scales to much larger test cases than Loom."
  - A failing run prints a schedule string that `shuttle::replay` reproduces deterministically. Shuttle also replaces `rand`, so data non-determinism is replayable too.
  - Schedulers:
    - `check_random`;
    - `check_pct`, with a configurable "bug depth" (number of preemptions);
    - `check_dfs`, exhaustive and "not recommended" except for small primitives;
    - `check_urw` (exported; not documented in the crate overview);
    - `PortfolioRunner`, which runs several schedulers in parallel.
  - Adoption requires importing all synchronization primitives from one `sync` module switched by `cfg`.
- **Loom [LOOM].**
  - Status: MIT, not archived. Latest crate 0.7.2, released 2024-04-23 (MSRV 1.65). Last commit 2026-02-20.
  - The README says Loom "runs a test many times, permuting the possible concurrent executions of that test under the C11 memory model" with CDSChecker-style state reduction. Tests are gated by `RUSTFLAGS="--cfg loom"`.
  - Its documented caveats narrow SS's "sound" claim:
    - `SeqCst` accesses are treated as `AcqRel`, which can produce false alarms;
    - some load-buffering executions are never explored, so Loom is "not sound" for them.
- **Miri [MIRI].** A Miri program "has no access to most platform-specific APIs or FFI". Only a few APIs are implemented, "basic file system access" among them. Miri tests one execution per seed, and its weak-memory emulation "is not complete".

### 2.9 Implications for mantle

Each item is an **INFERENCE** and cites the facts it rests on. "crash.rs" means `crates/chunk/tests/crash.rs` as observed in the repository on 2026-09-28.

**Design: what to keep, and what ShardStore does differently.**

- **Keep mantle's logged index and one flush per batch; do not adopt soft updates.**
  - ShardStore chose soft updates to avoid "redirecting writes through a write-ahead log" and to place each shard's data by heat. The price was "global reasoning about all possible orderings", a run-time dependency graph, and bugs that are missing edges or vertices (#6, #8) [SS §2.2, Fig. 5].
  - Mantle writes data records and one index frame per batch, flushes once, and only then acknowledges (chunk-store.md §4). A put is therefore persistent exactly when it is acknowledged, and no dependency graph is needed.
  - What mantle gives up is ShardStore's per-shard placement by heat. mantle's two streams, client writes and cleaner relocations, are a coarse version of it (chunk-store.md §4). Revisit only if HDD measurements show placement matters; Warfield's ~120 random IOPS per HDD is NON-PEER-REVIEWED context, not a mantle measurement.
- **Mantle already avoids ShardStore's soft/hard write-pointer class (#7, #8).**
  - ShardStore persists soft write pointers separately, in the superblock [SS §2.1].
  - mantle derives an open segment's write position at recovery from the replayed log plus a verified roll-forward, and tags every record with the segment's incarnation (chunk-store.md §3.1, §6). No second copy of the pointer exists to fall out of step.
- **The in-memory index stays justified for chunk-sized keys.**
  - ShardStore's index is a persistent LSM tree with values stored outside it [SS §2.1], so the index is not limited by RAM.
  - mantle's index is in memory and bounded by a chunk budget, refusing with `Full` (chunk-store.md §5). At ~8 MiB chunks, a disk holds about 1.25 M keys (note 01, C1).
  - If mantle ever stores small objects one chunk per object instead of packing them into blocks (note 01, D3), a WiscKey-style LSM index is the evidence-backed alternative.
- **Crash consistency protects a cell's repair budget, not only a node.** ShardStore's rationale is repair traffic and I/O load across the fleet, plus avoiding manual intervention [SS §2.2, p. 839]. In a cellular mantle, a node that loses everything in a crash spends its cell's repair capacity, so this belongs in the cell-sizing argument (§9.3).
- **Split request plane from control plane at the chunk store now:**
  - request plane: put, append, read, delete;
  - control plane: list, bulk delete, relocate or migrate, retire or return a volume.

  Give the control plane its own model and concurrency harness before it grows. This is exactly where ShardStore's model is incomplete [SS §8.3], and where #13, #16 and #4 occurred [Fig. 5]. It is also the storage-node end of the cell data-plane/control-plane split: request-plane calls must not wait on control-plane work.
- **Version the chunk-store RPC so engines can coexist.** ShardStore could be rolled out gradually because either engine could serve the same requests [SS §2, p. 837]. mantle's node API should allow mixed versions within a cell during a staged rollout.

**Tests to add.**

- **Convert crash.rs into a model-based proptest state machine, with shrinking.**
  - Today crash.rs uses a hand-rolled `Rng` and a `HashMap` of acknowledged states per writer. The model is right but not minimized: mantle's bug notes cite raw seeds (3, 54, 10006, 10080, 189, 10762), not minimized histories (docs/bugs/2026-09-28-*.md).
  - `proptest` is already a workspace dev-dependency, used today for codec and record properties (Cargo.toml).
  - Order the alphabet by complexity so shrinking prefers simple operations [SS §4.3]: `Read`, `Put`, `Append{seal}`, `Delete`, `Checkpoint`, `CleanStep`, `ScrubStep`, `CleanReopen`, `Crash(Random|LoseAll|KeepAll)`, `FailNextIo(target)`, `FailNextFlush`. Add control-plane operations as they appear.
  - Check after every operation:
    1. the acknowledged state equals the model;
    2. index locations never overlap, which catches the class in docs/bugs/2026-09-28-batch-overwrote-open-segment.md;
    3. each segment's usage equals the sum of its live records;
    4. after a failed flush, every later request is refused (the fence in chunk-store.md §4).
- **Bias arguments where there is evidence:**
  - reuse keys;
  - sizes at ±1 of the 4 KiB block `B` and of the 64 KiB checksum block, following SS's page-size bias [SS §4.2] and #1 [Fig. 5];
  - tiny volumes, so cleaning, log wrap and checkpoints happen within a few dozen operations. SS's one missed bug came from an oversized test cache [SS §8.3].
- **Add ShardStore's forward-progress property.** crash.rs already checks persistence, with "acknowledged" standing in for `is_persistent`. Also require that after `CleanReopen` every submitted request has been either acknowledged or refused with a typed error [SS §5]. This catches stalls and deadlocks such as #12.
- **Crash states: keep mantle's sector-level random tearing.** It is finer than SS's default per-component `RebootType` [SS §5; chunk-store.md §10]. Add explicit component-flush operations (checkpoint, superblock write) so partly flushed states are reached often. Keep exhaustive block-level enumeration as a rare mode: SS found no extra bugs with it, and it was "dramatically slower" [SS §5].
- **Scenario tests translated from Fig. 5:**
  - **#10:** a torn record followed by a new record written from the recovered position. Every scanner (roll-forward, cleaner, scrubber) must resynchronize only on a header whose CRC verifies, and must never skip bytes on the strength of an unverified length.
  - **#5:** a transient read error during cleaning must not drop a live record.
  - **#7:** a crash after a segment is freed and reopened under a new incarnation.
  - **#3:** a clean shutdown immediately after a segment is freed.
  - **#2 analogue:** reading a chunk in a freed segment fails on the incarnation check.
  - **#4:** a volume retired and later returned, once mantle has retirement.

  mantle's own bugs so far sit in the same recovery-and-relocation corner (docs/bugs/2026-09-28-unflushed-relocation-dropped-the-chunk.md, …-recovery-decisions-lived-only-in-memory.md).
- **Concurrency: Shuttle end to end, Loom for primitives.**
  - crash.rs runs concurrent writers on OS threads, so the interleaving is not reproducible from the seed (DERIVED from the code).
  - Shuttle adds replayable schedules and PCT [SHUTTLE]. Its first harness should copy SS Fig. 4: the cleaner on one segment, a checkpoint and a writer that overwrites and reads back, all in parallel. That is where #14 and mantle's own relocation bug live.
  - Use Loom for small primitives such as the bounded request queue, index publication after a flush, and the fence flag, keeping its documented unsoundness in mind [LOOM].
  - Both require one `cfg`-switched `sync` module [SHUTTLE]. Both are permissively licensed (Apache-2.0, MIT) and allowed by mantle's deny.toml.
- **Go beyond ShardStore on linearizability.** Record per-key histories from Shuttle runs and check them with the P-compositional WGL checker planned in note 06 §A6.8. SS's shown harness checks a fixed read-after-write history only [SS Fig. 4].
- **Reuse the chunk-store model as the mock chunk store** for the metadata service, repair and rebalancer, and gateway tests. This double duty keeps it current [SS §3.2, §8.4].
- **Decoders and undefined behavior.**
  - mantle's lints already make production code panic-free (CLAUDE.md §1), which covers what SS proved with Crux for bounded inputs [SS §7].
  - Still add SS's fuzzing of large inputs: arbitrary bytes into the frame, record and superblock decoders must yield a typed error or a value whose CRC verifies.
  - Miri cannot run most platform APIs [MIRI], so it applies only to pure-Rust code. Which of mantle's `unsafe` sites Miri could run is **UNVERIFIED**.
- **Process.**
  - Make these tests release blockers [SS §8.4].
  - In review of any concurrent change, ask where its Shuttle or Loom harness is [SS §8.2].
  - Scale soaks pay-as-you-go before releases. `MANTLE_CRASH_SEEDS` already does this. SS runs "tens of millions" of sequences per deployment [SS §4.2]; mantle's soak is 20,000 runs (docs/STATUS.md).

### 2.10 UNVERIFIED / not found

- **Unknown sizes and counts:** the size of a shard, of an extent (only "tens of thousands" per disk), and of the disk; the number of disks per host.
- **Placement:** how the host RPC layer maps shard IDs to disks, and how S3 places shards on nodes.
- **Replicas or EC fragments?** SS says only "replicated" [pp. 836-837]. Whether S3 shards are erasure-coded fragments is not stated here (see §6).
- **Acknowledgement point.** Whether ShardStore acknowledges a put to its caller only once `is_persistent` is true. SS does not say.
- **Undefined internals:** the checksum algorithm and granularity; the meaning of "hard write pointer" (#7); the superblock flush cadence; the edge directions in Fig. 2a.
- **Control-plane API:** its full contents beyond migration, repair, listing, removal and bulk create/remove.
- **Linearizability checking:** whether a general checker is used, as opposed to fixed-history assertions.
- **After 2021:** ShardStore's deployment scale and share (WARFIELD23 gives none); whether "tens of millions" of sequences per deployment still holds.
- **Minor inconsistencies inside SS:** the 13% figure (the table gives 12.2%), and "written by" in §1 versus "last edited by" in §8.2 for the 18%.

---

## 3. DynamoDB (Elhemali et al., USENIX ATC '22): partitions, routing, admission control and splits

This is AWS's peer-reviewed account of a multi-tenant, range-partitioned, consensus-replicated store with a thin request-routing tier. It is the closest AWS analogue to mantle's metadata service, which is a set of key ranges, each a Raft group (STATUS.md, "Metadata service"). What it teaches:

- routing through cached partition maps;
- admission control decoupled from partitions;
- splitting by observed load;
- node-initiated rebalancing;
- a constant-work metadata cache.

### 3.1 Scope, structure and vocabulary

**Structure** [DDB, pp. 1037-1048]:

| § | Title | Subsections |
|---|---|---|
| 1 | Introduction | |
| 2 | History | |
| 3 | Architecture | |
| 4 | Journey from provisioned to on-demand | 4.1 Initial improvements to admission control (4.1.1 Bursting, 4.1.2 Adaptive capacity); 4.2 Global admission control; 4.3 Balancing consumed capacity; 4.4 Splitting for consumption; 4.5 On-demand provisioning |
| 5 | Durability and correctness | 5.1 Hardware failures; 5.2 Silent data errors; 5.3 Continuous verification; 5.4 Software bugs; 5.5 Backups and restores |
| 6 | Availability | 6.1 Write and consistent read availability; 6.2 Failure detection; 6.3 Measuring availability; 6.4 Deployments; 6.5 Dependencies on external services; 6.6 Metadata availability |
| 7 | Micro benchmarks | |
| 8 | Conclusion | |

**Absent vocabulary.**
- The words "cell", "blast radius" and "shuffle" do not occur anywhere in the text layer (grep).
- "Statically stable" occurs once, in §6.5 [DDB §6.5, p. 1046], citing the Builder's Library article "Static stability using availability zones" by B. Weiss and M. Furr (DDB ref. [18]).
- The paper therefore says nothing about whether DynamoDB is internally deployed as cells. It speaks of "millions of Paxos groups in a Region" [DDB §6.1, p. 1045] and describes the request-router, metadata and storage fleets without saying how many of each exist per Region. **UNVERIFIED**: any claim that DynamoDB is cellular.

**Scale and service levels:**
- In the 66-hour 2021 Prime Day, Amazon systems made "trillions of API calls to DynamoDB, peaking at 89.2 million requests per second" [DDB Abstract, p. 1037].
- "DynamoDB offers an availability SLA of 99.99 for regular tables and 99.999 for global tables" [DDB §1, p. 1038].
- DynamoDB "is designed to scale the resources dedicated to a table from several servers to many thousands as needed" [DDB §1, p. 1037].

**Tenancy:**
- "DynamoDB employs a multi-tenant architecture. DynamoDB stores data from different customers on the same physical machines to ensure high utilization of resources ... Resource reservations, tight provisioning, and monitored usage provide isolation between the workloads of co-resident tables." [DDB §1, p. 1037]

### 3.2 Keys and partitions

**Placement by hash, then sort key:**
- "The partition key's value is always used as an input to an internal hash function. The output from the hash function and the sort key value (if present) determines where the item will be stored." [DDB §3, p. 1039]
- "Each partition of the table hosts a disjoint and contiguous part of the table's key-range. Each partition has multiple replicas distributed across different Availability Zones for high availability and durability." [DDB §3, pp. 1039-1040]
- **DERIVED:** partitions are contiguous ranges over the space (hash(partition key), sort key). This is hash-then-range: hashing spreads unrelated keys, and the sort key keeps one partition key's items ordered and splittable. mantle's vshard-prefixed physical keys (note 01 §1.16 M3) have the same shape.

**The partition is the unit of elasticity:**
- "partitions could be further split and migrated to allow the table to scale elastically. Partition abstraction proved to be really valuable and continues to be central to the design of DynamoDB." [DDB §4, p. 1041]
- The same passage names the original flaw: "this early version tightly coupled the assignment of both capacity and performance to individual partitions, which led to challenges." [DDB §4, p. 1041]

**NON-PEER-REVIEWED** [DDB-DOCS, "Partitions and data distribution in DynamoDB"]:
- DynamoDB adds partitions when provisioned throughput rises "beyond what the existing partitions can support", or "If an existing partition fills to capacity and more storage space is required".
- "Partition management occurs automatically in the background and is transparent to your applications."
- "If your table doesn't have local secondary indexes, DynamoDB will automatically split your item collection over as many partitions as required to store the data and to serve read and write throughput."

### 3.3 Replication groups, leader leases, log replicas

**Consensus and leadership** [DDB §3, p. 1040]:
- "The replicas for a partition form a replication group. The replication group uses Multi-Paxos [14] for leader election and consensus. Any replica can trigger a round of the election. Once elected leader, a replica can maintain leadership as long as it periodically renews its leadership lease."
- "Only the leader replica can serve write and strongly consistent read requests."
- A write is acknowledged "once a quorum of peers persists the log record to their local write-ahead logs". "Any replica of the replication group can serve eventually consistent reads."

**Lease hand-over.**
- "The new leader won't serve any writes or consistent reads until the previous leader's lease expires." [DDB §3, p. 1040]
- That wait "only takes a couple of seconds" but blocks writes and consistent reads in the meantime [DDB §6.2, p. 1045].

**Two replica kinds:**
- *Storage replicas* hold "both the write-ahead logs and the B-tree that stores the key-value data" (Fig. 2).
- *Log replicas* (Fig. 3) "are akin to acceptors in Paxos. Log replicas do not store key-value data." [DDB §3, p. 1040]

**Healing after failures** [DDB §5.1, p. 1043; §6.1, p. 1045]:
- "When a node fails, all replication groups hosted on the node are down to two copies. The process of healing a storage replica can take several minutes because the repair process involves copying the B-tree and write-ahead logs." [§5.1]
- "Upon detecting an unhealthy storage replica, the leader of a replication group adds a log replica to ensure there is no impact on durability. Adding a log replica takes only a few seconds because the system has to copy only the recent write-ahead logs from a healthy replica to the new replica without the B-tree." [§5.1]
- "A healthy write quorum in the case of DynamoDB consists of two out of the three replicas from different AZs." If one replica is unresponsive, "the leader adds a log replica to the group." [§6.1]
- "Introducing log replicas was a big change to the system, and the formally proven implementation of Paxos provided us the confidence to safely tweak and experiment with the system to achieve higher availability. We have been able to run millions of Paxos groups in a Region with log replicas." [§6.1]

**WAL archival.** "Write ahead logs are stored in all three replicas of a partition." They are periodically archived to S3, and "The unarchived logs are typically a few hundred megabytes in size." [DDB §5.1, p. 1043]

### 3.4 Components: control plane and data plane

**Core services** [DDB §3, p. 1040, Fig. 4]. "DynamoDB consists of tens of microservices." The paper names four core ones:

- **Metadata service.** It "stores routing information about the tables, indexes, and replication groups for keys for a given table or index."
- **Request routing service.** It "is responsible for authorizing, authenticating, and routing each request to the appropriate server"; "The request routers look up the routing information from the metadata service." Data-definition requests go elsewhere: "All resource creation, update, and data definition requests are routed to the autoadmin service."
- **Storage service.** "Each of the storage nodes hosts many replicas of different partitions."
- **Autoadmin.** It "is built to be the central nervous system of DynamoDB. It is responsible for fleet health, partition health, scaling of tables, and execution of all control plane requests."
  - It "replaces any replicas deemed unhealthy (slow or not responsive or being hosted on bad hardware)".
  - When a storage node is unhealthy, "it kicks off a recovery process that replaces the replicas hosted on that node".

**Other services** not in Fig. 4: "point-in-time restore, on-demand backups, update streams, global admission control, global tables, global secondary indices, and transactions" [DDB §3, p. 1040].

**The paper's own terms.** It says "control plane requests" for autoadmin [p. 1040]. In §5.4 it separates "the replication protocol of the data plane" from "our control plane" [DDB §5.4, p. 1044].

**DERIVED:** the per-request path is request router → (GAC tokens, §3.6; partition-map cache / MemDS, §3.5) → the partition leader on a storage node, or any replica for an eventually consistent read. Autoadmin is not on the per-request path; it moves and replaces replicas.

### 3.5 Routing: request routers, partition maps, MemDS and the constant-work cache

**What a router needs.**
- A router must map a table's primary key to storage nodes [DDB §6.6, p. 1046].
- DDB-TX, one year later: "All operations sent to DynamoDB reach a fleet of frontend hosts called request routers. Request routers authenticate each request and route the request to the appropriate storage nodes based on the key being accessed. The mapping of key-range to storage nodes is maintained in a metadata subsystem." [DDB-TX §3.1, pp. 707-708]

**Original design: whole-table maps held in DynamoDB itself** [DDB §6.6, p. 1046]:
- "At launch, DynamoDB stored the metadata in DynamoDB itself." The routing information "consisted of all the partitions for a table, the key range of each partition, and the storage nodes hosting the partition."
- A router seeing a new table "downloaded the routing information for the entire table and cached it locally". Because partition configuration "rarely changes, the cache hit rate was approximately 99.75 percent."

**Why it failed: bimodal load.**
- "caching introduces bi-modal behavior. In the case of a cold start where request routers have empty caches, every DynamoDB request would result in a metadata lookup, and so the service had to scale to serve requests at the same rate as DynamoDB." [DDB §6.6, p. 1046]
- This happened "in practice when new capacity is added to the request router fleet. Occasionally the metadata service traffic would spike up to 75 percent." [DDB §6.6, p. 1046] The paper does not say what the 75 percent is of. Reading it as "of DynamoDB's request rate" is **UNVERIFIED**.
- The consequence is that adding routers "impacted the performance and could make the system unstable", and "an ineffective cache can cause cascading failures to other parts of the system as the source of data falls over from too much direct load". Ref [4] is AWS's summary of the 2015 us-east DynamoDB disruption [DDB §6.6, p. 1047].

**Fix, part 1: fetch per partition, not per table.** "When servicing a request, the router needs only information about the partition hosting the key for the request. Therefore, it was wasteful to get the routing information for the entire table, especially for large tables with many partitions." [DDB §6.6, p. 1047]

**Fix, part 2: MemDS**, "an in-memory distributed datastore" [DDB §6.6, p. 1047]:
- It "stores all the metadata in memory and replicates it across the MemDS fleet. MemDS scales horizontally to handle the entire incoming request rate of DynamoDB. The data is highly compressed."
- Each node holds a "Perkle" structure, "a hybrid of a Patricia tree [17] and a Merkle tree". It supports:
  - lookup by full key or by key prefix;
  - range queries ("lessThan, greaterThan, and between");
  - `floor` (the entry whose key is ≤ the given key) and `ceiling` (≥).
- **DERIVED:** `floor(key)` is exactly the lookup a range directory keyed by range start key needs.

**Fix, part 3: the constant-work cache.**
- "A new partition map cache was deployed on each request router host to avoid the bi-modality of the original request router caches. In the new cache, a cache hit also results in an asynchronous call to MemDS to refresh the cache. Thus, the new cache ensures the MemDS fleet is always serving a constant volume of traffic regardless of cache hit ratio." [DDB §6.6, p. 1047]
- The trade-off is stated: constant traffic "increases the load on the metadata fleet compared to the conventional caches where the traffic to the backend is determined by cache hit ratio, but prevents cascading failures to other parts of the system when the caches become ineffective." [DDB §6.6, p. 1047]

**Who is authoritative, and what happens on a stale route:**
- "DynamoDB storage nodes are the authoritative source of partition membership data. Partition membership updates are pushed from storage nodes to MemDS. Each partition membership update is propagated to all MemDS nodes." [DDB §6.6, p. 1047]
- "If the partition membership provided by MemDS is stale, then the incorrectly contacted storage node either responds with the latest membership if known or responds with an error code that triggers another MemDS lookup by the request router." [DDB §6.6, p. 1047]
- **DERIVED:** the directory is a cache fed by pushes from the replication groups. Correctness is enforced by the owner (the storage node), which redirects or rejects. The directory is never trusted for safety. "if known" implies a node that has left a group may not know its successor.

### 3.6 Admission control: from per-partition allocations to global admission control (GAC)

**Capacity units.** "For items up to 4 KB in size, one RCU can perform one strongly consistent read request per second. For items up to 1 KB in size, one WCU can perform one standard write request per second." [DDB §4, p. 1040]

**The original, fully local scheme** [DDB §4, p. 1041]:
- "Storage nodes independently performed admission control based on the allocations of their locally stored partitions."
- Two limits applied: "a cap on the maximum throughput that could be allocated to a single partition", and a node-level sum no greater than "the maximum allowed throughput on the node as determined by the physical characteristics of its storage drives".

**Allocation on split** [DDB §4, p. 1041]:
- "When a partition was split for size, the allocated throughput of the parent partition was equally divided among the child partitions. When a partition was split for throughput, the new partitions were allocated throughput based on the table's provisioned throughput."
- The paper's worked example assumes a 1000-WCU per-partition maximum:
  - a 3200-WCU table gets 4 partitions at 800 each;
  - raised to 3600, each gets 900;
  - raised to 6000, the table splits to 8 partitions at 750 each;
  - lowered to 5000, "each partition's capacity would be decreased to 675 WCUs".
- **DERIVED:** 5000/8 = 625, so "675" contradicts the equal-division rule the example illustrates. It is probably a typo.

**Two failure modes** [DDB §4, p. 1041]:
- **Hot partitions.** "Hot partitions arose in applications that had traffic going consistently towards a few items of their tables. The hot items could belong to a stable set of partitions or could hop around to different partitions over time."
- **Throughput dilution.** "Throughput dilution was common for tables where partitions were split for size."
- Splitting can make things worse: under non-uniform load, "splitting a partition and dividing performance allocation proportionately can result in the hot portion of the partition having less available performance than it did before the split."
- Customers responded by over-provisioning.

**Why the scheme was kept, then patched.** "We liked that enforcing allocations at an individual partition level avoided the need for the complexities of distributed admission control, but it became clear these controls weren't sufficient." [DDB §4.1, p. 1041]

**Bursting** [DDB §4.1.1, pp. 1041-1042]:
- Unused partition capacity is retained "for up to 300 seconds".
- A partition may burst only "if there was unused throughput at the node level".
- There are three token buckets: "two for each partition (allocated and burst) and one for the node".
- "Write requests using burst capacity required an additional check on the node-level token bucket of other member replicas of the partition. The leader replica of the partition periodically collected information about each of the members node-level capacity."

**Adaptive capacity** [DDB §4.1.2, p. 1042]:
- "If a table experienced throttling and the table level throughput was not exceeded, then it would automatically increase (boost) the allocated throughput of the partitions of the table using a proportional control algorithm."
- Autoadmin relocated boosted partitions to nodes with room.
- The result was "best-effort but eliminated over 99.99% of the throttling due to skewed access pattern".

**Why both were replaced** [DDB §4.2, p. 1042]:
- "Bursting was only helpful for short-lived spikes in traffic and it was dependent on the node having throughput to support bursting. Adaptive capacity was reactive and kicked in only after throttling had been observed."
- The lesson: "The salient takeaway from bursting and adaptive capacity was that we had tightly coupled partition level capacity to admission control."

**Global admission control (GAC)** [DDB §4.2, p. 1042]:
- "The GAC service centrally tracks the total consumption of the table capacity in terms of tokens."
- "Each request router maintains a local token bucket to make admission decisions and communicates with GAC to replenish tokens at regular intervals (in the order of few seconds)."
- "GAC maintains an ephemeral state computed on the fly from client requests. Each GAC server can be stopped and restarted without any impact on the overall operation of the service."
- "All the GAC servers are part of an independent hash ring. Request routers manage several time-limited tokens locally."
- GAC "estimate[s] the global token consumption and vends tokens available for the next time unit to the client's share of overall tokens". So "non-uniform workloads that send traffic to only a subset of items can execute up to the maximum partition capacity."
- Defense in depth: "the partition-level token buckets were retained for defense-in-depth. The capacity of these token buckets is then capped to ensure that one application doesn't consume all or a significant share of the resources on the storage nodes."
- **UNVERIFIED:** the key GAC's hash ring is partitioned by (presumably the table), the token time unit, and the estimation algorithm. None is given.

**NON-PEER-REVIEWED, current limits** [DDB-DOCS, "DynamoDB burst and adaptive capacity"]:
- Throttling occurs "if a single partition receives more than 3000 read operation [sic] or more than 1000 write operations".
- A single-item partition can get "up to the partition maximum of 3,000 RCUs and 1,000 WCUs".
- Burst capacity retains "up to five minutes (300 seconds) of unused read and write capacity".
- Adaptive capacity can rebalance "such that frequently accessed items don't reside on the same partition", and "might rebalance your data so that a partition contains only that single, frequently accessed item".
- It "will not split item collections across multiple partitions of the table when there is a local secondary index on the table".
- Splitting an item collection by sort key is ruled out when the traffic "is tracked by a monotonic increase or decrease of the sort key".

### 3.7 Placement: thousands of replicas per node, and node-initiated rebalancing

**Density and co-location.**
- "The latest generation of storage nodes hosts thousands of partition replicas. The partitions hosted on a single storage node could be wholly unrelated and belong to different tables." [DDB §4.3, p. 1042]
- Placement is "an allocation scheme that decides which replicas can safely co-exist without violating critical properties such as availability, predictable performance, security, and elasticity" [DDB §4.3, p. 1042].

**Overcommitment.**
- Under static provisioning, "Partitions were never allowed to take more traffic than their allocated capacity and, hence there were no noisy neighbors." [DDB §4.3, pp. 1042-1043]
- Once partitions could always burst, "the system packed storage nodes with a set of replicas greater than the node's overall provisioned capacity." [DDB §4.3, p. 1043]

**Balancing loop** [DDB §4.3, p. 1043]:
1. A background system proactively balances "based on throughput consumption and storage".
2. "Each storage node independently monitors the overall throughput and data size of all its hosted replicas. In case the throughput is beyond a threshold percentage of the maximum capacity of the node, it reports to the autoadmin service a list of candidate partition replicas to move from the current node."
3. "The autoadmin finds a new storage node for the partition in the same or another Availability Zone that doesn't have a replica of this partition."

- **DERIVED:** detection is local and proposes; placement is central and decides, under an anti-affinity constraint (one replica per node, AZ diversity preserved).
- **UNVERIFIED:** the threshold percentage and the move protocol. By analogy with healing (§3.3), a move presumably copies the B-tree and WAL, but the paper does not describe it.

### 3.8 Splitting for consumption (split-for-heat) and on-demand scaling

**Splitting for consumption** [DDB §4.4, p. 1043], quoted whole:

> "Once the consumed throughput of a partition crosses a certain threshold, the partition is split for consumption. The split point in the key range is chosen based on key distribution the partition has observed. The observed key distribution serves as a proxy for the application's access pattern and is more effective than splitting the key range in the middle. Partition splits usually complete in the order of minutes. There are still class of workloads that cannot benefit from split for consumption. For example, a partition receiving high traffic to a single item or a partition where the key range is accessed sequentially will not benefit from split. DynamoDB detects such access patterns and avoids splitting the partition."

**On-demand provisioning** [DDB §4.5, p. 1043]:
- DynamoDB "instantly accommodates up to double the previous peak traffic on the table". Beyond that it "automatically allocates more capacity as the traffic volume increases".
- "On-demand scales a table by splitting partitions for consumption. The split decision algorithm is based on traffic. GAC allows DynamoDB to monitor and protect the system from one application consuming all the resources."

**Splits and in-flight transactions** [DDB-TX §3.3, p. 708]. Per-transaction metadata (id, timestamp) "is attached to items that are part of the transaction and remain with the items during partition-related changes, such as split. This ensures that such changes do not interfere with transactions and can happen in parallel."

**Failover and in-flight transactions** [DDB-TX §3.5, p. 710]. On failover, transaction metadata accepted by the old primary "is persistently stored and replicated within the group, and so is immediately available to the new primary."

**UNVERIFIED (not described in DDB or DDB-TX):**
- how a split executes: whether it is a replicated log command, how children get replica sets, and how routers learn the new boundary beyond §3.5's push and redirect;
- the split threshold;
- how "sequential" access is detected;
- whether size and heat splits share one mechanism.

### 3.9 Durability and correctness mechanisms

**Checksums everywhere** [DDB §5.2, p. 1044]:
- "By maintaining checksums within every log entry, message, and log file, DynamoDB validates data integrity for every data transfer between two nodes."
- "a checksum is computed for every message between nodes or components and is verified because these messages can go through various layers of transformations before they reach their destination."

**Archival checks** [DDB §5.2, p. 1044]:
- Each archived log file has a manifest (table, partition, start and end markers).
- Before upload the agent checks:
  - "every log entry to ensure that it belongs to the correct table and partition";
  - checksums;
  - "that the log file doesn't have any holes in the sequence numbers".
- "Log archival agents run on all three replicas". If a file is already archived, the agent "downloads the uploaded file to verify the integrity of the data by comparing it with its local write-ahead log".
- Uploads carry a content checksum that S3 checks on PUT.

**Continuous verification (scrub)** [DDB §5.3, p. 1044]. Scrub "verifies two things: all three copies of the replicas in a replication group have the same data, and the data of the live replicas matches with a copy of a replica built offline using the archived write-ahead log entries". It works by comparing checksums with a snapshot generated from the archived logs. The lesson: "continuous verification of data-at-rest is the most reliable method of protecting against hardware failures, silent data corruption, and even software bugs."

**Formal methods and testing** [DDB §5.4, p. 1044]:
- "The core replication protocol was specified using TLA+ [12, 13]. When new features that affect the replication protocol are added, they are incorporated into the specification and model checked. Model checking has allowed us to catch subtle bugs that could have led to durability and correctness issues before the code went into production."
- They also run "extensive failure injection testing and stress testing". Formal methods "have also been used to verify the correctness of our control plane and features such as distributed transactions". This is the practice FM (§4) describes from its start.

**Backups and point-in-time restore (PITR)** [DDB §5.5, p. 1044]:
- Both are built from the archived WALs, so they "don't affect performance or availability".
- Backups "are consistent across multiple partitions up to the nearest second".
- PITR covers "the previous 35 days". Partition snapshot frequency "is decided based on the amount of write-ahead logs accumulated for the partition".

### 3.10 Availability engineering

**Power-off tests** [DDB §6, p. 1045]. "Using realistic simulated traffic, random nodes are powered off using a job scheduler. At the end of all the power-off tests, the test tools verify that the data stored in the database is logically valid and not corrupted."

**Gray-failure-aware elections** [DDB §6.2, p. 1045]. The problem: "a replica that isn't receiving heartbeats from a leader will try to elect a new leader" even when the leader is healthy. The fix:

> "a follower that wants to trigger a failover sends a message to other replicas in the replication group asking if they can communicate with the leader. If replicas respond with a healthy leader message, the follower drops its attempt to trigger a leader election."

The result was that this "significantly minimized the number of false positives in the system, and hence the number of spurious leader elections."

**Measuring availability** [DDB §6.3, p. 1045]:
- "Availability is calculated for each 5-minute interval as the percentage of requests processed by DynamoDB that succeed."
- Alongside customer-facing alarms, availability is measured from the client side: internal Amazon services report it, and canaries "are run from every AZ in the Region, and they talk to DynamoDB through every public endpoint".

**Deployments** [DDB §6.4, pp. 1045-1046]:
- "The rolled-back state might be different from the initial state of the software. The rollback procedure is often missed in testing and can lead to customer impact."
- "DynamoDB runs a suite of upgrade and downgrade tests at a component level before every deployment. Then, the software is rolled back on purpose and tested by running functional tests."
- **Read-write deployments:** "The first step is to deploy the software to read the new message format or protocol. Once all the nodes can handle the new message, the software is updated to send new messages."
- Deployments go to "a small set of nodes before pushing them to the entire fleet", with automatic rollback on alarm thresholds.
- On storage nodes, "The leader replicas relinquish leadership and hence the group's new leader doesn't have to wait for the old leader's lease to expire."

**Dependencies and static stability** [DDB §6.5, p. 1046]:
- "all the services that DynamoDB depends on in the request path should be more highly available than DynamoDB. Alternatively, DynamoDB should be able to continue to operate even when the services on which it depends are impaired."
- For IAM and KMS: "DynamoDB employs a statically stable design [18], where the overall system keeps working even when a dependency becomes impaired."
- "DynamoDB caches result [sic] from IAM and AWS KMS in the request routers ... DynamoDB periodically refreshes the cached results asynchronously. If IAM or KMS were to become unavailable, the routers will continue to use the cached results for pre-determined extended period. Clients that send operations to request routers that don't have the cached results will see an impact."
- **UNVERIFIED:** the length of the "pre-determined extended period".

### 3.11 Evaluation (§7) and stated lessons (§1)

**Micro benchmarks** [DDB §7, p. 1047]:
- YCSB workload A (50% reads, 50% updates) and B (95% reads, 5% updates), with "a uniform key distribution and items of size 900 bytes".
- Run "against production DynamoDB in the North Virginia region", scaled "from 100 thousand total operations per second to 1 million total operations per second".
- Reported only as p50/p99 latencies in Figs. 5-6 that "show very little variance". The text gives no latency values. **UNVERIFIED:** any numeric latency read off the figures.

**Lessons, verbatim** [DDB §1, p. 1038]:

- "Adapting to customers' traffic patterns to reshape the physical partitioning scheme of the database tables improves customer experience."
- "Performing continuous verification of data-at-rest is a reliable way to protect against both hardware failures and software bugs in order to meet high durability goals."
- "Maintaining high availability as a system evolves requires careful operational discipline and tooling. Mechanisms such as formal proofs of complex algorithms, game days (chaos and load tests), upgrade/downgrade tests, and deployment safety provides the freedom to safely adjust and experiment with the code without the fear of compromising correctness."
- "Designing systems for predictability over absolute efficiency improves system stability. While components such as caches can improve performance, do not allow them to hide the work that would be performed in their absence, ensuring that the system is always provisioned to handle the unexpected."

### 3.12 Implications for mantle (INFERENCE)

- **I1. A metadata range is a DynamoDB partition.**
  - What it is: a contiguous key range, one consensus group, a leader-only strong path, and many ranges from many tenants per node [DDB §3, pp. 1039-1040; §4.3, p. 1042].
  - Implication: mantle's planned multi-Raft ranges (STATUS.md; note 06 §A4) already have this shape. What DynamoDB adds is the operational envelope around that unit (I2-I8).
- **I2. The range directory holds per-range entries, looked up by `floor(key)`, and owners enforce correctness.**
  - Gateways cache *per-range* descriptors, never a whole bucket's map [DDB §6.6, p. 1047].
  - The directory index is ordered by range start key, so lookup is `floor` [DDB §6.6, p. 1047; compare Bigtable's METADATA hierarchy, note 06 §A4.1].
  - The range's Raft group is authoritative and pushes descriptor changes to the directory [DDB §6.6, p. 1047].
  - A replica that receives a request for a key outside its current descriptor answers with its newer descriptor or a typed stale-route error, never with data. This is the TiKV epoch check (note 06 §A4.4) and DynamoDB's "latest membership if known ... or ... an error code" [DDB §6.6, p. 1047].
  - The directory is never on the safety path.
- **I3. Design the directory cache for constant work.** DynamoDB lost stability because cache misses scaled metadata load with request rate [DDB §6.6, pp. 1046-1047].
  - Adopt the property that directory load does not depend on cache state.
  - Rule 2 of `CLAUDE.md` needs a bound, and one-refresh-per-hit is not bounded per range. A mantle variant: coalesce refreshes to at most one in flight per (gateway, range) per refresh interval, issued whether the lookup hit or missed.
  - That keeps load equal in cold and warm states: the number of touched ranges per interval.
  - This variant is mantle's own design and is not in the paper.
- **I4. Admit tenants at the gateway, not per range.**
  - DynamoDB's central lesson is that coupling capacity to partitions causes throughput dilution on split and hot-partition throttling [DDB §4, p. 1041; §4.2, p. 1042].
  - mantle should enforce per-tenant (account/bucket) rates at S3 gateways with GAC-style vended, time-limited tokens [DDB §4.2, p. 1042]. This is the same shape as Tectonic's client-side distributed-counter leaky bucket and TrafficGroups (note 01 §1.11).
  - Keep per-range and per-node caps only as protective ceilings [DDB §4.2, p. 1042; Tectonic's 10 KQPS per-shard cap, note 01 §0 item 9].
  - A split must never divide a tenant's quota.
- **I5. Account writes on every replica's node.** DynamoDB's burst writes checked "the node-level token bucket of other member replicas" [DDB §4.1.1, p. 1042].
  - A mantle range write costs disk time on every voter, so admission at the leader must see follower-node headroom.
  - The unit should be disk time, as in Tectonic (note 01 §1.11).
- **I6. Split for heat at an observed key, and skip futile splits.**
  - Split point: choose it from sampled per-key load, not the midpoint [DDB §4.4, p. 1043]. CockroachDB also splits on load (note 06 §A4.3).
  - Detect and skip:
    - **single-key heat.** A split cannot help; replicate reads or cache instead;
    - **sequential access.** Monotonic keys: the new hot spot follows the tail [DDB §4.4; DDB-DOCS on monotonic sort keys].
  - S3-style workloads with time-ordered key names are the sequential case. mantle's hashed directory prefix (note 01 §1.16 M3) removes cross-directory sequentiality but not within one hot directory.
  - "Order of minutes" is DynamoDB's figure, not a mantle target.
- **I7. Nodes propose moves; one placement authority decides.**
  - A node self-monitors against a *measured* capacity (CLAUDE.md rules 4-5) and reports candidate ranges above a threshold. The placement service picks a target that holds no replica of that range and preserves failure-domain diversity [DDB §4.3, p. 1043]. This is Tectonic's rebalancer/repair split (note 01 §1.10).
  - Rebalance on storage too, not just throughput [DDB §4.3, p. 1043].
- **I8. Restore quorum fast, rebuild state slowly.**
  - DynamoDB separates the two: a log replica takes seconds, a full B-tree plus WAL rebuild takes minutes [DDB §5.1, p. 1043; §6.1, p. 1045].
  - For mantle's Raft ranges the analogous tool is a log-only (witness-like) voter or a learner promoted later. It changes Raft's commit and membership rules, so it needs a TLA+ spec before code, as DynamoDB relied on its proven Paxos when adding log replicas [DDB §6.1, p. 1045; note 07 §1.6].
- **I9. Scrub across replicas, not just within records.**
  - mantle's chunk-store scrub verifies per-record CRCs (docs/design/chunk-store.md §9).
  - Add, per DynamoDB [DDB §5.3, p. 1044]:
    - cross-replica digest comparison of a range's state at the same applied index;
    - cross-replica checksum comparison of a block's chunks via the Block layer.
  - DynamoDB's archive-time identity and no-sequence-hole checks [DDB §5.2, p. 1044] mirror the chunk store's frame-LSN rules (chunk-store.md §3.2).
- **I10. Leader hygiene.**
  - Asking peers whether the leader is healthy before starting an election [DDB §6.2, p. 1045] is PreVote plus CheckQuorum in Raft terms (note 06 §A1.3).
  - Relinquishing leadership before a deployment [DDB §6.4, p. 1046] is Raft leadership transfer before planned restarts.
  - Both matter more with leases, because a lost lease costs "a couple of seconds" of unavailability [DDB §6.2, p. 1045].
- **I11. Format evolution: read-new everywhere, then write-new; test the rollback.** This applies to mantle's versioned on-disk frames (chunk-store.md §3.2 frame header "version") and wire messages [DDB §6.4, p. 1046].
- **I12. Static stability at the gateway.**
  - Cache authentication and authorization results (SigV4 credential lookups, bucket policies) with asynchronous refresh, and keep serving from cache for a bounded period while the identity or config service is down [DDB §6.5, p. 1046].
  - The bound is a mantle constant that needs its own cited rationale. DynamoDB's value is UNVERIFIED.
- **I13. Do not route through yourself without a root.** DynamoDB first stored routing metadata in DynamoDB [DDB §6.6, p. 1046]. mantle's range directory needs a fixed bootstrap range (Bigtable's unsplittable root tablet, note 06 §A4.1).
- **I14. Measure availability as DynamoDB does.** Use success ratio per 5-minute window and client-side probes through every gateway [DDB §6.3, p. 1045]. mantle's end-to-end suites (STATUS.md, S3 gateway) can report the same metric.
- **I15. Laptop degeneration.** With one process, the router, directory and GAC collapse to in-process lookups and a local token bucket. The authority rule in I2 (owner rejects stale routes) still holds, so the single-node code path is the fleet code path (CLAUDE.md preamble).

### 3.13 UNVERIFIED / not found in DDB

- Whether DynamoDB is internally divided into cells; the paper never uses the term (§3.1).
- The mechanics of splits and replica moves: log command vs copy, cut-over, router notification beyond push and redirect, and thresholds (§3.7-§3.8).
- The GAC ring key, token time unit and estimation algorithm (§3.6).
- The meaning of the "75 percent" metadata spike (§3.5).
- The IAM/KMS cache validity period (§3.10).
- Latency values in Figs. 5-6 (§3.11).
- Partition size limits: none is stated in DDB. The DDB-DOCS per-partition throughput maxima (3,000 RCU / 1,000 WCU) are NON-PEER-REVIEWED.

---

## 4. Newcombe et al., "How Amazon Web Services Uses Formal Methods" (CACM 2015): S3, DynamoDB and fault-tolerant replication

### 4.1 Provenance and scope

**Versions read.**
- FM is the published CACM article (printed pp. 66-73), read in full.
- FM-TR (29 Sept 2014) was read for comparison. Its prose matches FM closely, but the wording of the systems table differs (§4.2).

**Peer-review status.** CACM's current author guidelines say section submissions are peer-reviewed. We saw this only through a search-result snippet, so the 2015 refereeing policy is **UNVERIFIED**.

**Scope.** Per the brief, this section covers only what FM says about S3 and DynamoDB internals and about replication and fault-tolerance designs.

**What FM does and does not reveal.** FM deliberately gives no design details: "we have not included snippets of specifications because their unfamiliar syntax can be off-putting to potential new users" [FM p. 68]. What it gives is component names, spec sizes, bug counts and trace length, plus how the specs were used.

### 4.2 The systems table, reproduced exactly

Caption: "Applying TLA+ to some of Amazon's more complex systems." [FM p. 69]. The table is unnumbered in the text layer; the prose calls it "the table here" [FM p. 68].

| System | Components | Line Count (Excluding Comments) | Benefit |
|---|---|---|---|
| S3 | Fault-tolerant, low-level network algorithm | 804 PlusCal | Found two bugs, then others in proposed optimizations |
| S3 | Background redistribution of data | 645 PlusCal | Found one bug, then another in the first proposed fix |
| DynamoDB | Replication and group-membership system | 939 TLA+ | Found three bugs requiring traces of up to 35 steps |
| EBS | Volume management | 102 PlusCal | Found three bugs |
| Internal distributed lock manager | Lock-free data structure | 223 PlusCal | Improved confidence though failed to find a liveness bug, as liveness not checked |
| Internal distributed lock manager | Fault-tolerant replication-and-reconfiguration algorithm | 318 TLA+ | Found one bug and verified an aggressive optimization |

The last row's "replication-and-reconfiguration" is our rejoining of a line-break hyphen in the text layer ("replication-and" / "reconfiguration"). FM-TR has "Fault tolerant replication and reconfiguration algorithm".

**FM-TR wording differences** [FM-TR PDF p. 3, table "Applying TLA+ to some of our more complex systems"]:
- S3 rows: "Found 2 bugs. Found further bugs in proposed optimizations." and "Found 1 bug, and found a bug in the first proposed fix."
- DynamoDB row: "Replication & group-membership system", "Found 3 bugs, some requiring traces of 35 steps".
- Lock-manager rows: "Found 1 bug. Verified an aggressive optimization."
- The line counts are identical.
- Note the DynamoDB row in particular: FM says "up to 35 steps", FM-TR says "some ... 35 steps". The prose in both says the *shortest* trace for the data-loss bug had 35 high-level steps (§4.3).

### 4.3 DynamoDB: replication and group membership

- **Context.** DynamoDB launched in January 2012 as a store that "replicates customer data across multiple data centers while promising strong consistency" [FM p. 70].
- **Verification before TLA+.** Author T.R. created DynamoDB's replication and fault-tolerance mechanisms [FM p. 70]. Before TLA+ he:
  - ran "extensive fault-injection testing using a simulated network layer to control message loss, duplication, and reordering";
  - stress-tested on real hardware;
  - wrote informal correctness proofs, which "did indeed find several bugs in early versions of the design". The authors add: "conventional informal proofs can miss very subtle problems".
- **Model checking set-up.** The spec took "a couple of weeks". It was model-checked with distributed TLC on "10 cc1.4xlarge EC2 instances, each with eight cores plus hyperthreads and 23GB of RAM" [FM p. 70].
- **The data-loss bug.**
  - "the model checker found a bug that could lead to losing data if a particular sequence of failures and recovery steps would be interleaved with other processing. This was a very subtle bug; the shortest error trace exhibiting the bug included 35 high-level steps." [FM pp. 70-71]
  - It "had passed unnoticed through extensive design reviews, code reviews, and testing" [FM p. 71].
  - "The model checker later found two bugs in other algorithms, both serious and subtle." [FM p. 71]
- **A move/migration feature was also bug-prone.** "After DynamoDB was launched, T.R. worked on a new feature to allow data to be migrated between data centers." It was added to the existing replication spec, and "The model checker found the initial design would have introduced a subtle bug" [FM p. 71].
- **Still the practice in 2022.** DDB says the replication protocol is still maintained in TLA+ and every replication-affecting feature is model-checked [DDB §5.4, p. 1044]. It also credits the formally proven Paxos for making log replicas safe to introduce [DDB §6.1, p. 1045].

### 4.4 S3

- **Scale (NON-PEER-REVIEWED underneath).** S3 launched in 2006. "In the following six years, S3 grew to store one trillion objects. Less than a year later it had grown to two trillion objects and was regularly handling 1.1 million requests per second." [FM p. 66] FM sources these figures to two AWS blog posts (FM refs. 3-4).
- **Fault-tolerant network algorithm.** "a team working on S3 asked for help using TLA+ to verify a new fault-tolerant network algorithm" [FM p. 71].
  - Its documentation "consisted of many large, complicated state-machine diagrams". The team had considered "writing a Java program to brute-force explore possible executions: essentially a hard-wired form of model checking".
  - Author F.Z. wrote two versions of the spec in PlusCal "over a couple of weeks".
  - Results: "Model checking revealed two subtle bugs in the algorithm and allowed F.Z. to verify fixes for both." Later feature and optimization experiments revealed that "some of these changes would have introduced bugs."
- **More S3 specs.** "Engineers from those teams wrote specs for two additional critical algorithms and for one new feature." [FM p. 71]
- **B.M.'s spec** (B.M. was then "in the AWS S3 Engines group", author bio [FM p. 73]):
  - It targeted an algorithm "known to contain a subtle bug. The bug had passed unnoticed through multiple design reviews and code reviews and had surfaced only after months of testing."
  - TLC found it "in seconds" [FM p. 71].
  - With the team's already-reviewed fix added to the spec, "The model checker found the problem still occurred in a different execution trace. A stronger fix was proposed, and the model checker verified the second fix." [FM p. 72]
  - B.M.'s next spec "did not uncover any bugs but did uncover several important ambiguities in the documentation for the algorithm" [FM p. 72].
- **DERIVED, not stated.** B.M.'s story (a bug, then a bug in the first fix) has the same shape as the table's "Background redistribution of data" row. FM does not connect the two, so the identification is **UNVERIFIED**. FM describes neither what S3's "low-level network algorithm" nor its "background redistribution of data" does. Reading the latter as S3 partition splitting or rebalancing would be speculation.

### 4.5 Other replication and fault-tolerance work

- **Internal distributed lock manager.** It had two specs (table rows above):
  - the fault-tolerant replication-and-reconfiguration spec found one bug and verified an aggressive optimization;
  - the lock-free data-structure spec *missed* a liveness bug because liveness was not checked [FM p. 69].
- **One of AWS's most important new distributed algorithms.** Author M.D. used TLA+ "to find a critical bug in one of AWS's most important new distributed algorithms" [FM p. 72].
  - C.N. independently wrote a spec "quite different in style" and "both found the same bug".
  - "Both specs were later used to verify that a crucial optimization to the algorithm did not introduce any bugs."
- **A replication system's write latency.** C.N. took the prose design of "a fault-tolerant replication system" and model-checked specs at "two levels of concurrency". That let him "propose a major protocol optimization that radically reduced write-latency in the system" [FM p. 72].
- **Breaking up atomic operations.** "designers often break atomic transactions into finer-grain operations chained together through asynchronous workflows; TLA+ can help explore the consequences of such changes with respect to isolation and consistency." [FM p. 72] Tectonic-style non-atomic cross-shard metadata operations are this pattern (note 01 §1.6).
- **Adoption at the time of writing.**
  - "10 large complex real-world systems".
  - "seven teams".
  - Engineers were "able to learn TLA+ from scratch and get useful results in two to three weeks" [FM p. 68].

### 4.6 Method details that transfer to storage protocols

- **Properties.**
  - Safety example: "at all times, all committed data is present and correct" [FM p. 68].
  - Liveness example: "whenever the system receives a request, it must eventually respond to that request" [FM pp. 68-69].
- **Environment.** The environment is specified explicitly. Assumptions include "If a process has not restarted, then it retains its local state, modulo any intentional modifications". Events include "network errors and repairs, disk errors, process crashes and restarts, data-center failures and repairs, and actions by human operators" [FM p. 69].
- **Framing.** The stance is "what needs to go right", in contrast to "the ad hoc 'what might go wrong' approach" [FM p. 69].
- **Specs as a change-safety tool.** Changes and optimizations are checked against the spec: "removing or narrowing locks or weakening constraints on message ordering" [FM p. 69].
- **Working practice.** Start from a prose design and refine parts of it into PlusCal or TLA+ [FM p. 72].

### 4.7 Stated limits

- **Emergent performance failures.** They are out of reach: "surprising 'sustained emergent performance degradation' of complex systems that inevitably contain feedback loops". The example given is a GC pause that causes timeouts, retries and more load. "We do not yet know of a feasible way to model a real system that would enable tools to predict such emergent behavior." [FM pp. 69-70]
- **Code conformance.** "How do we know that the executable code correctly implements the verified design?" "The answer is we do not know." [FM p. 72]
  - No tool could verify code at their scale, and static analysis finds only "local" issues [FM p. 73].
  - TLC-guided test generation (Tasiran et al.) had "not yet" been tried on software [FM p. 73].
- **Unchecked liveness.** Liveness left unchecked let a liveness bug through (lock-manager row) [FM p. 69].
- **Models are not systems.** "Formal methods deal with models of systems, not the systems themselves" [FM p. 73].

### 4.8 Implications for mantle (INFERENCE)

- **F1. Write TLA+ specs, in this order, for the protocol classes where AWS found design bugs.** The classes are:
  - replication and group membership (the 35-step data-loss bug);
  - background redistribution of data (a bug, then a bug in its first fix);
  - replication and reconfiguration;
  - a data-migration feature layered on an existing replication protocol [FM pp. 69-71].

  For mantle, those map to:

  1. **Range split and merge with descriptor epochs and cached routes.**
     - Safety:
       - each key is owned by exactly one range per epoch;
       - no acknowledged write is lost across a split or merge;
       - a stale-epoch request is never served.
     - Liveness: a started split eventually commits or aborts.
     - Grounding: note 06 §A4.4; §3.5 and §3.8 above.
  2. **Ownership transfer.**
     - What moves: leader lease or leaseholder moves, plus Raft membership changes by joint consensus.
     - Safety: disjoint leases; no write committed under a superseded lease.
     - Grounding: note 06 §A4.3.
  3. **Chunk placement moves and repair.**
     - Protocol: copy, then cut over the Block-layer entry, concurrently with deletes and GC.
     - Safety: a block never loses its last readable copy; GC never deletes the only copy of a live block.
     - Grounding: note 01 §1.10.
  4. **The chunk store's local relocation.** This is lower priority for TLA+: it is a single-node crash-consistency problem, and the crash simulator already covers it (STATUS.md). Relocation is still where mantle has already had a data-loss bug (docs/bugs/2026-09-28-unflushed-relocation-dropped-the-chunk.md). That bug mirrors the "background redistribution" row at node scale.
- **F2. Check liveness from day one.** The lock-manager row shows the cost of skipping it [FM p. 69]. Examples: splits and moves terminate, and fenced ranges eventually recover a leader.
- **F3. Bridge spec and code.** FM had no answer for conformance [FM p. 72]. mantle should drive its deterministic simulation and conformance tests from the spec (note 06 §C.d; focal-raft already ships a TLA+ model, note 07 §1.6).
- **F4. Model operators and disks, not just networks.** Put operator actions (drain, add capacity, retire disk; STATUS.md "Multi-machine operation") and disk errors into the environment [FM p. 69]. mantle's simulated device already injects the disk half (STATUS.md).
- **F5. Get evidence for overload behaviour elsewhere.** Retry storms and metastability are outside TLA+ [FM pp. 69-70]. They need admission control (§3.6), constant-work designs (§3.5) and load tests.
- **F6. Keep specs living.**
  - Every change to split, move or membership updates the spec first, as DynamoDB does [DDB §5.4, p. 1044; FM p. 71].
  - Every proposed fix is re-checked in the spec. FM reports a first proposed fix that was itself wrong: table row 2 [FM p. 69] and B.M.'s story [FM p. 72], possibly the same case.

### 4.9 UNVERIFIED / not found

- What S3's "fault-tolerant, low-level network algorithm" and "background redistribution of data" do, and whether either concerns S3 partitioning.
- The link between B.M.'s story and a specific table row.
- FM's refereeing status (policy snippet only, §4.1).
- Newcombe, "Why Amazon Chose TLA+" (ABZ 2014, LNCS 8477, pp. 25-39) was **not reviewed**: we found no open-access copy.

---

## 5. AWS cell-based architecture guidance (all NON-PEER-REVIEWED)

**Everything in §5 is NON-PEER-REVIEWED**: AWS whitepapers, Amazon Builders' Library articles, an AWS blog post and AWS source code. Labels used below:

- *No label*: stated in the cited AWS document.
- **DERIVED**: this note's own arithmetic or reading.
- **INFERENCE**: design reasoning for mantle.

The peer-reviewed cell design (Physalia) is in §1. The shuffle-sharding math and its literature are in §8.

### 5.1 What these sources are, and what they are not

- **CELL-WP is guidance for AWS customers.** It does not describe the internals of any named AWS service.
  - It says of itself: "This isn't a conclusive description, but some of the learnings that AWS service teams have learned from implementing cell-based architecture" [CELL-WP, "Implementing a cell-based architecture", p. 15].
  - Its only claim about AWS's own use is general: "For more than a decade, our service teams have used cell-based architecture to build more resilient and scalable services" [CELL-WP, "Introduction", p. 1]. It names no service, gives no cell sizes and reports no measurements.
  - It expands the Well-Architected best practice "REL10-BP04 Use bulkhead architectures to limit scope of impact" [CELL-WP p. 1; "Further reading", p. 51].
  - Its further reading lists "Millions of Tiny Databases" (§1 of this note), FIB, and the re:Invent 2018 talk "How AWS Minimizes the Blast Radius of Failures (ARC338)" [p. 51]. The talk was not reviewed.
- **FIB describes AWS's own services.** "This paper details how AWS uses these boundaries to create zonal, Regional, and global services" [FIB, "Abstract", p. 1].
- **The four Builder's Library articles are first-person accounts** by AWS engineers of specific systems: EC2, Route 53, Hyperplane and the NAT gateway. They come from the builders, but they are not peer-reviewed and cannot be checked independently.

### 5.2 What a cell is

- **Bulkhead origin.** "A cell-based architecture comes from the concept of a bulkhead in a ship" [CELL-WP, "What is a cell-based architecture?", p. 4].
- **Definition.** "A cell-based architecture uses multiple isolated instances of a workload, where each instance is known as a cell. Each cell is independent, does not share state with other cells, and handles a subset of the overall workload requests." If 10 cells serve 100 requests and one cell fails, "90% of the overall requests would be unaffected" [p. 5].
- **Failures a cell contains.** "unsuccessful code deployments or requests that are corrupted or invoke a specific failure mode (also known as poison pill requests)" [p. 5].
- **Routing by partition key.** "A cell routing layer distributes requests to individual cells based on the partition key and presents a single endpoint to clients." [p. 5]
- **Three components** [pp. 7-8]:
  - *Cell router:* "We also refer to this layer as the thinnest possible layer, with the responsibility of routing requests to the right cell, and only that."
  - *Cell:* "A complete workload, with everything needed to operate independently."
  - *Control plane:* "Responsible for administration tasks, such as provisioning cells, de-provisioning cells, and migrating cell customers."
- **Shape.** Services are broken "down internally into cells" with "thin layers to route traffic to the right cells. This type of architecture can be zonal, regional, or global." [p. 7]
- **Cells need not add hardware.** An application with 30 hosts can keep "the same 30 hosts, but with a cell router and with tasks that are distributed or grouped between cells" [p. 8].
- **Independence is the ideal, not always reachable.** "Cells should have no dependency on each other at all (that is, no cross-cell API calls, no shared resources like databases or S3 buckets.) Even the use of separate AWS accounts is encouraged." "Cross-cell dependencies can quickly eliminate the benefits of a cellular architecture, so try to do this as little as possible, or only at specific transitory times." [CELL-WP, "Cell design", pp. 16-17]

### 5.3 Claimed benefits, when to use cells, and the stated limit

- **Scale out with a capped cell.** Scaling out gives "Workload isolation", "Maximally-sized components" and components that are "Not too big to test". "Each cell, a complete independent instance of the service, has a fixed maximum size. Beyond the size of a single cell, regions grow by adding more cells." [CELL-WP, "Scale-out over scale-up", p. 9]
- **Test at the cap.** "it is reasonable to simulate the largest workload that can fit into a cell, which should match the largest workload that a single customer can send to your application" [p. 12].
- **The availability argument is qualitative.** "a system with n cells will have n times as many failure events, but each with 1/n of the impact. But the higher MTBF and lower MTTR afforded by cells means fewer shorter failures events per cell, and higher overall availability." [pp. 12-13] **DERIVED:** the MTBF and MTTR claims rest on reasoning alone; the whitepaper reports no data.
- **Canary cells.** "the first cell deployed to in a phased cell deployment can be a canary cell" [p. 13].
- **When to use** [CELL-WP, "When to use a cell-based architecture?", p. 14]:
  - applications where downtime has a large impact;
  - "Ultra-scale systems that are too big/critical to fail";
  - "Less than 5 seconds of Recovery Point Objective (RPO)";
  - "Less than 30 seconds of Recovery Time Objective (RTO)";
  - "Multi-tenant services where some tenants require fully dedicated tenancy".

  The framing question: "Is it better for 100% of customers to experience a 5% failure rate, or 5% of customers to experience a 100% failure rate?"
- **Costs** [p. 14]: more architectural complexity, higher infrastructure cost, specialized operational tools, and the "Necessity to invest in a cell routing layer".
- **Stated limit: cells are not failover domains.** Cells target cascading failures: "Mainly, they are excessive load of resources and deployments with problems or bugs." "Cells were not intended to mitigate dependency failures or single points of failure. Therefore, they were not designed as failover domains." [CELL-WP, "Should a Single-AZ cell fail over if an AZ becomes unavailable?", p. 20]

### 5.4 Control plane, data plane and static stability

**Definitions.**

- A control plane is "the machinery involved in making changes to a system—adding resources, deleting resources, modifying resources—and getting those changes propagated to wherever they need to go to take effect". A data plane is "the daily business of those resources" [BL-STATIC p. 3].
- In FIB's terms, control planes "provide the administrative APIs used to create, read/describe, update, delete, and list (CRUDL) resources" and "tend to be complicated orchestration and aggregation systems". "Data planes are intentionally less complicated". FIB lists "creating an Amazon Simple Storage Service (Amazon S3) bucket" as a control-plane action and "getting and putting objects in an S3 bucket" as data plane [FIB, "Control planes and data planes", p. 7].
- For cells, the control plane provides the APIs "to provision, move, migrate, update, remove, deploy, and monitor cells". After it provisions a cell and informs the router, "both the router and the cell will be just performing the work they're supposed to (data plane)" [CELL-WP, "Control plane and data plane", p. 15].

**Why the planes are separated** [BL-STATIC pp. 3-4]:

- The data plane operates "at a higher volume (often by orders of magnitude)", so each plane should scale on its own dimensions.
- The control plane "tends to have more moving parts than its data plane, so it's statistically more likely to become impaired". The EC2 data plane "is far simpler than its control plane" and targets higher availability.

**Static stability.**

- **Definition.** "In a statically stable design, the overall system keeps working even when a dependency becomes impaired ... everything it was doing before the dependency became impaired continues to work" [BL-STATIC p. 2].
- **Mechanisms** [FIB, "Static stability", p. 8]:
  - "preventing circular dependencies in our services that could stop one of those services from successfully recovering";
  - maintaining existing state: "Data plane access to resources, once provisioned, has no dependency on the control plane". S3 buckets and objects are named as having this property.
  - "The most reliable recovery and mitigation mechanisms are the ones that require the fewest changes to achieve recovery."
- **EC2's example.** The host "has local access to all of the information it needs to route packets". During a control-plane impairment it misses updates, but "the traffic it had been able to send and receive before the event will continue to work" [BL-STATIC p. 3].
- **Applied to cells** [CELL-WP p. 16]:
  - "control planes are designed to fail rather than corrupt or provide incorrect information (CP in the CAP theorem), while data planes generally prefer AP in the CAP theorem (Data planes try their best to remain available, even if they depend on stale information for decisions.)"
  - The example is a router whose routes "are loaded into memory from an S3 bucket. Even if the control plane, Amazon S3 or a zone is unavailable, the router is still able to direct traffic to the cells."
- **Pre-provisioned capacity is part of it.**
  - With three AZs, "we overprovision by 50 percent", so each AZ "is operating at only 66 percent of the level for which we have load-tested it" [BL-STATIC pp. 4-5]. FIB's example: 6 instances are needed across 3 AZs, so 9 are run [FIB p. 12].
  - Active-standby systems "elect the new leader node from a standby candidate instead of launching a replacement 'just in time.'" [BL-STATIC p. 6]
- **Zonal independence.**
  - Two AZs "will receive a given deployment on different days" [BL-STATIC p. 7; FIB p. 3].
  - A request through two regional services avoids an impaired AZ with probability "2/3 * 2/3 = 4/9"; through N regional services, "(2/3)^N ... versus remaining constant at 2/3 for N zonal services". Foundational data-plane components such as the NAT gateway are therefore zonal, while "we replicate any hard state across multiple Availability Zones for disaster recovery purposes" [BL-STATIC pp. 8-9].
  - Zonal services "fail independently in each Availability Zone"; a regional control plane "serves as an aggregation and routing layer on top of the zonal control planes" [FIB p. 10].

**What FIB says about S3** (feeds §6):

- "Amazon S3, for example, spreads requests and data across multiple Availability Zones and is designed to automatically recover from the failure of an Availability Zone. However, you only interact with the Regional endpoint of the service." [FIB, "Regional services", p. 13]
- Listed S3 bucket-configuration operations "have an underlying dependency on us-east-1 in the aws partition". The list includes PutBucketPolicy, PutBucketVersioning, PutBucketLifecycle and PutBucketReplication [FIB, "Global Single-Region operations", p. 19].
- "all calls to the CreateBucket and DeleteBucket APIs depend on us-east-1, in the aws partition, to ensure name uniqueness". FIB advises: "Do not rely on deleting or creating new S3 buckets or updating S3 bucket configurations as part of your recovery path." [FIB p. 20]
- **DERIVED:** in AWS's own classification, bucket lifecycle and bucket configuration are control-plane operations, some of them centralized in one Region. Object GET and PUT are data plane.

**How the control plane feeds the data plane.** CELL-WP cites the following two articles for keeping routers current [CELL-WP pp. 27, 32].

- **Scale mismatch [BL-SMALLER]:**
  - The data-plane fleet exceeds the control-plane fleet "frequently by a factor of 100 or more" [p. 1].
  - Correlated data-plane behaviour can drive the control plane past the point where "its goodput ... quickly drops to zero". The triggers named are bugs, recovery after an outage, and retries [p. 2].
  - *Remedy 1: configuration in S3.* The control plane writes configuration to S3 and data-plane servers poll it, so "the data plane can continue running with the last known configuration ... This property, called static stability". Hyperplane works this way. The remedy does not fit large, fast-changing configuration (EC2) or propagation needed "in single digit seconds or faster" [pp. 4-5].
  - *Remedy 2: let the smaller fleet set the pace.* One way is to push, with consistent hashing assigning data-plane servers to control-plane servers [pp. 5-6]. The preferred way is to "decouple the direction of discovery from the direction of the control flow": each data-plane server "opens a long-lived connection to a single control plane server", which pushes over it and "can reject the connection" when busy [p. 6]. At a mismatch of 1000× or more, the data plane sends a small UDP request and the control plane opens the connection [p. 7].
- **Constant work [BL-CONSTANT]:**
  - Constant-work systems "don't scale up or slow down with load or stress", "don't have modes", and if they vary at all, "do less work in times of stress" [p. 2]. By contrast, "most caches have modes" [pp. 2-3].
  - Route 53 health checkers and aggregators "use a cellular design", with each cell's limit tested [p. 4]. Result sets are always full-size ("The other 9,990 entries are dummies"), and aggregators push "a fixed-size table of health check statuses" every few seconds [p. 4].
  - Hyperplane nodes reload a full, maximum-sized configuration file from S3 every few seconds, even when nothing changed [p. 5].
  - The pattern self-heals: it is "always operating in 'repair everything' mode", whereas "a workflow type system is usually edge-triggered" [p. 6]. Many configuration systems "can be as simple as 'apply a full configuration each time in a loop.'" [p. 7]

### 5.5 Partitioning: partition key → cell

**Choosing the key** [CELL-WP, "Cell partition", p. 22]:

- Keys "must be chosen to match the grain of the service, or the natural ways that a service's workload can be subdivided with minimal cross-grain interactions".
- "A good partition key is one that is easily accessible in most API calls, either as a direct parameter or a direct transformation of a parameter."
- For customers that outgrow a cell: "A good strategy is to define a second dimension more aligned with your type of business to be part of the partition key".
- Cross-grain work such as scatter-gather should be a minority. "instead of letting the cells talk directly to each other, any cross-cell calls have to go back through the normal cell router."

**What any mapping algorithm needs** [p. 23]: "A mechanism to serve or distribute state used by these algorithms", and "Accommodations for gracefully handling migration when cells are added and removed". The list of approaches that follows is "a non-exhaustive list of partitioning algorithms presented without specific recommendations".

| Approach | Mechanism (quoted) | Stated advantages | Stated disadvantages | Pages |
|---|---|---|---|---|
| Full mapping | "explicitly map every key to a cell" | simple; "More control over distribution to control hot cells and to perform a cell migration" | "a critical read and write dependency on the mapping table, a read-your-writes consistency requirement, and a large amount of state"; cost at high cardinality; "longer cell router bootstrap time" if held in memory | 23-24 |
| Prefix and range-based | "map ranges of keys (or hashes of keys) to cells" | lower cardinality than full mapping | "More likely to have a hot cell, as there is no control over which keys within each range might have the most traffic" | 24-25 |
| Naïve modulo | modular arithmetic, "typically on a cryptographic hash of the key" | "an effective zero peak-to-average ratio (very even distribution) and requires minimal state (just the count of cells)" | "high churn (cell reassignment) when adding or removing cells"; changing the cell count rebalances every cell | 25-26 |
| Consistent hashing | "a family of algorithms that map keys to buckets (cells) with a small amount of fairly stable state and a minimal amount of churn" | changing the cell count does not rebalance every cell | "Can suffer from significant high peak-to-average ratios (uneven spread)" | 26-27 |

**The recommended consistent-hashing shape is two-level:** "a system that is configured with a fixed large number (for example, tens of thousands) of logical buckets which are explicitly mapped to much smaller number of physical cells. Mapping a key to a cell is a two-step process. First, the key is mapped to its logical bucket using naïve module [sic] mapping. Then the cell for that bucket is located using a bucket to cell mapping table." [p. 26]

The whitepaper names three algorithms, none reviewed here [p. 26]:
- the Ring Consistent Hash (Karger et al., as used in Chord);
- "A Fast, Minimal Memory, Consistent Hash Algorithm" (Lamping and Veach);
- "Multi-probe consistent hashing" (Appleton and O'Reilly).

**Override table, whatever the algorithm:** "it's important to also use an override table to force specific keys to specific cells ... This can be useful for testing, quarantining, and special-case routing for particularly heavy partition keys." Mapping a new customer to a cell and registering it with the router "is the control plane's task" [p. 27].

**DERIVED:** this two-level scheme matches two others:
- note 01 M3's virtual-shard scheme for metadata: `vshard = H(shard key) mod V`, with physical shards owning vshard ranges;
- Route 53's 2048 "virtual name servers", which "don't correspond to the physical servers hosting Route 53. We can move them around to help manage capacity." [BL-SHUFFLE p. 6]

### 5.6 Routing: the thin layer

- **The router cannot itself be split into cells.**
  - "The router layer is a shared component between cells, and therefore cannot follow the same compartmentalization strategy as with cells."
  - It should map keys "in a computationally efficient manner, such as combining cryptographic hash functions and modular arithmetic to map partition keys to cells".
  - "To avoid multi-cell impacts, the routing layer must remain as simple and horizontally scalable as possible, which necessitates avoiding complex business logic within this layer."
  - CELL-WP cites BL-CONSTANT at this point [CELL-WP, "Cell routing", p. 27].
- **Required properties** [p. 28]:
  - "Be simple as possible, but not simpler."
  - "Have request dispatching isolation between cells."
  - "Minimize the amount of business logic in this layer."
  - Hide the cellular implementation from clients.
  - "Fast and reliable."
  - "Continue operating normally in other cells even when one cell is unreachable."
- **Four router designs** [pp. 29-33]:
  1. *DNS (Route 53):* "Give each tenant a custom DNS record they use to reach your service, then configure the DNS record to point at a specific cell to which the tenant has been assigned" [p. 29].
  2. *API Gateway*, with the map in DynamoDB [pp. 30-31].
  3. *A compute fleet reading a map the control plane writes to S3* [pp. 31-32]. "the only responsibility should be to inspect the request data and identify which cell the request should be forwarded to". "the cell mapping lives in memory on the router. With each change in the S3 bucket, another process or thread is in listener mode and updates the memory map when necessary." BL-SMALLER is suggested for the synchronization design.
  4. *Non-HTTP:* the router consumes a queue or stream and forwards each message to its cell [pp. 32-33].
- **Resilience of the router** [CELL-WP, "About resilience of the cell router", p. 34]:
  - "In a cell-based architecture, the only component that has the shared state of all cells is the cell router. It presents itself as a single point of failure."
  - The router must itself be built "as a cellular component" and follow the same sizing and observability guidance.
  - "the routing layer still has to scale infinitely, but the set of problems that you have to solve for scaling the thinnest possible layer should be a subset of the scaling challenges that non-cellularized application would have to face."

### 5.7 Cell sizing and scaling dimensions

- **Three opposing forces** [CELL-WP, "Cell sizing", p. 34]:
  - "Big enough to fit the largest workloads."
  - "Small enough to test at full scale (and to operate efficiently)".
  - "Big enough to gain economies of scale benefits."

  Cells should have a capped maximum size, consistent across installations (for example, AZs or Regions).
- **Trade-off table** [pp. 35-36]. "The maximum cell size will vary per-service." The whitepaper pairs the rows as follows (paraphrased, row by row):

  | Smaller cells | Larger cells |
  |---|---|
  | "Will have more cells" to deploy and manage | "Will have fewer cells" |
  | an outage or drain affects a small share of the fleet | an outage or drain affects a large share of the fleet |
  | "Less likely to hit scaling limitation" (Region and account quotas; unknown limits that appear at scale) | "More likely to reach scaling limitation" |
  | "Reduced scope of impact": 10 cells → 10% of customers each; 100 → 1% | "Reduced splits": client workloads stay in one cell rather than being split across cells |
  | "Easier to test", and cheaper | "Easier to operate": fewer replicas, though tooling is still needed |
  | "Less idle resources" | "Better capacity utilization" (economies of scale) |

- **Know the limits** (REL01-BP01): "How many transactions per second can a cell handle? How many customers or tenants does it support? How many GB of transfer per second or stored capacity does it support?" [p. 36]
- **Scaling dimensions** [p. 37]:
  - With client ID as the only dimension, a client that grows past the cell's capacity (the example is 10K TPS) forces scale-up or cannot be served.
  - A second dimension lets true outliers get a dedicated cell, or several. The cost can be "the need to have a scather/gather router [sic]".
  - Dedicated cells are also a product: "If a customer really wants it, and is willing to pay for it (and a surprising number are), you can dedicate a cell totally to them."
- **Cost** also bounds cell size [p. 39].
- **Enforce the limits** [CELL-WP, "Know and respect your cell's limit", p. 39]:
  - "Using load shedding to avoid overload is fundamental".
  - Find the limits by load testing and chaos engineering.
  - A custom router can use a token bucket; "An example of the use of token bucket is made by Amazon EC2 API."

### 5.8 Cell placement: the control plane's allocation job

- **Placement belongs to the control plane.** "Cell placement it is [sic] another responsibility of the control plane", covering onboarding tenants and creating cells [CELL-WP, "Cell placement", p. 39].
  - It needs each cell's capacity and used capacity, each tenant's share of usage, and each cell's quotas and limits.
- **Leave headroom (REL01-BP06).** "It is not because your cell supports 10K TPS that you must constantly be operating on the threshold of this limit ... Ensure that a sufficient gap exists between the current quotas and the maximum usage to accommodate failover." [p. 40]
- **For stateful workloads, allocation is central.** "For data and state heavy workloads, the problem of allocation becomes a core competency." That includes migrating tenants when "a customer or partitioned key starts to dominate a cell" [p. 40]. The inputs listed:
  - each cell's dimensions, which may be fixed or change;
  - each partition key's dimensions, which change over time;
  - "The cost of moving a partition key between cells";
  - the benefit of co-tenancy or affinity.

### 5.9 Cell migration

- **One trigger.** A customer "becomes too big and requires it to have a dedicated cell" [CELL-WP, "Cell migration", p. 41].
- **Online migration and the mapping transition.** "Stateful cell-based architectures will almost certainly require online cell migration to adjust placement when cells are added or removed. One consideration of online cell migration is handling mapping decisions during the transitionary period. This may involve cross-cell redirects, performing multiple iterations of the mapping algorithm when necessary, or both, against different versions of the mapping algorithm state." [p. 41]
- **Moving state safely, in four phases** [p. 41]:
  1. "Clone the data from the current location into the new location, as a non-authoritative copy."
  2. "Flip the new location copy to be authoritative."
  3. "Redirect from old location to new location."
  4. "Forget the data from the old location."
- **Alternative.** "careful coordination between the router and the cells": the control plane moves clients and ensures "this state transition before the cell is ready to receive traffic" [p. 41].
- **Best practice.** "Start with a cell migration mechanism from day one" [CELL-WP, "Best practices", p. 47].
- **Not specified:** how writes that arrive during phases 1-2 are handled, how the flip is made atomic with respect to routers holding the old map, and how long phase 4 waits. The whitepaper defers these: "This will be system-dependent" [p. 41]. Peer-reviewed mechanisms are in §7, and mantle's protocol is proposed in §9.6.

### 5.10 Deployment, observability and best practices

- **Deploy in waves.** The point is "to deploy in waves, cell by cell or set of cells", whether or not the cells are AZ-aligned [CELL-WP, "Cell deployment", pp. 41-43].
- **Cell-aware observability.** "Your entire observability stack needs to be cell-aware ... It is important to be able to track each request and identify which cell it is destined for." [CELL-WP, "Cell observability", p. 45]
- **Best practices** [p. 47]:
  - "Your current instance/stack is your cell zero": add the router above the existing stack.
  - "Start with multiple cells from day one", which "will bring you the adverse issues and experience needed to operate in this type of environment, reducing surprises".
  - "Start with a cell migration mechanism from day one".
  - "Perform a failure mode analysis of your cell".

### 5.11 Multi-AZ versus single-AZ cells

- **Multi-AZ (regional) cells** can use regional managed services. Their weakness is "Less control over an AZ failure, particularly gray failures" [CELL-WP, "Multi-AZ cells", pp. 17-18].
- **Single-AZ cells** make it possible "to accurately detect in which AZ a problem is occurring" [CELL-WP, "Single-AZ cells", pp. 18-20]. Their listed disadvantages:
  - "Requires three cell routers, and requires clients to chose [sic] the correct zonal endpoint."
  - They need extra disaster-recovery machinery, because "Cell state needs to be replicate [sic] to another, which in turn can break the cell concept".
- **Replicating single-AZ cells across AZs** is "more complex and driven by a much higher cost". Unless the service is itself zonally scoped, "the Multi-AZ cell is a better approach to consider" [pp. 21-22].

### 5.12 Shuffle sharding as AWS describes it (the math is in §8)

**Relation to cells.** "Although shuffle-sharding is an excellent fault-isolating mechanism, they are not the same thing." "We can use shuffle-sharding within a cell, but cross-cells should not be used by definition. Shuffle-sharding can also be a bit trickier for stateful components." [CELL-WP, "What about shuffle-sharding?", p. 49]

**BL-SHUFFLE (2019).**

- **Origin.** Route 53 needed DDoS isolation without buying scrubbing appliances for every domain, which "would cost tens of millions of dollars" [p. 2].
- **Worked example** [pp. 3-6]:
  - Unsharded, a poisonous request or flood cascades through all eight workers: "everything and everyone" [p. 4].
  - Four plain shards of two workers cut the impact to 25% [p. 4].
  - Shuffle shards give each customer two of the eight workers. When one customer's pair fails, "at most one of another shuffle shard's workers will be affected", and service continues "If the requestors are fault tolerant and can work around this (with retries for example)" [p. 5].
  - "With eight workers, there are 28 unique combinations of two workers ... the scope of impact due to a problem is just 1/28th. That's 7 times better than regular sharding." [p. 6]
- **Route 53** [p. 6]:
  - "a total of 2048 virtual name servers", which "don't correspond to the physical servers hosting Route 53. We can move them around to help manage capacity."
  - Shards of four per domain: "a staggering 730 billion possible shuffle shards".
  - AWS can "ensure that no customer domain will ever share more than two virtual name servers with any other customer domain".
  - Attacked domains are isolated to "special dedicated attack capacity".
- **Recursive shuffle sharding** shards "items at multiple layers, thus isolating a customer's customer" [pp. 6-7].

**SS-BLOG (2014).**

- **Clients do the isolating.** The technique requires "simple retry logic in the client that causes it to try every endpoint in a Shuffle Shard, until one succeeds". Hence "With 3 retries – a common retry value – we can use four instances in total per shuffle shard." [SS-BLOG, "Shuffle Sharding"]
- **Its counts.** It gives "56 potential shuffle shards" for 2 of 8 workers and an impact of "1/1680" for 4 of 8 [ibid.].
  - **DERIVED:** these are *ordered* counts, P(8,2) and P(8,4). Isolation depends only on the set of workers, so the counts should be C(8,2) = 28 (as BL-SHUFFLE has it) and C(8,4) = 70.
  - The author's correction note fixes only the card-hand figure ("I wrote 7 million, based on permutations, instead of 300,000 based on combinations").
- **DNS.** "If customers (or objects) are given specific DNS names to use ... then DNS can be used to keep per-customer cleanly separated across shards." [SS-BLOG, "Sharding and Bulkheads"]
- **Infima's two modes** [SS-BLOG, "Infima and Shuffle Sharding"]. Both are "compartmentalization aware", e.g. they "choose 2 endpoints from each zone".
  - "Stateless shuffle sharding uses hashing, much like a bloom filter does" and "can be easily used, even directly in calling clients".
  - "Stateful Searching Shuffle Sharding" can guarantee that "no two shuffle shards ever share more than two particular endpoints".
- **General principle** [SS-BLOG, "Post-script"]: allow shards "to partially overlap in their membership, in return for an exponential increase in the number of shards the system can support". It applies to "queues, rate-limiters, locks and other contended resources".

**INFIMA (source code).**

- A `Lattice` places endpoints on fault dimensions (AZ, software version, datastore) and "may also be used to simulate failures directly". The README states the impact as "1/(N choose K)".
- `SimpleSignatureShuffleSharder` seeds `java.util.Random` with the first 64 bits of MD5(application seed, identifier), shuffles each dimension, and takes `endpointsPerCell` endpoints from each chosen lattice cell (in a one-dimensional lattice, every cell, e.g. every AZ). Javadoc: "It's therefore important to use a seed likely to be unique to your application, to protect against targeted collision attacks."
- `StatefulSearchingShuffleSharder` records every (maximumOverlap + 1)-subset of each assigned shard in a caller-supplied `FragmentStore`. It runs "a recursive backtracking search" for a shard with no already-used subset and throws `NoShardsAvailableException` when none exists. Javadoc example: endpoints A-E, size 3, maximum overlap 1; after [A,B,C] and [A,D,E], "No other shard is computable".
- `RubberTree` turns shards and lattices into weighted Route 53 record trees with standby branches. The repository is archived (last push 2022-08-05).

### 5.13 Implications for mantle (INFERENCE)

Each bullet names the facts it rests on. A Tectonic cluster is already a datacenter-local, top-level deployment unit (note 01 §1.2). The Tectonic paper describes no routing layer above clusters, no size cap tested at full scale, and no moving of tenants between clusters. Those three are what the AWS guidance adds.

1. **A mantle cluster is the regional-level cell.** It is a complete, independent stack (gateways, metadata ranges, chunk stores, background services) with a fixed, load-tested maximum size [CELL-WP pp. 5, 9, 12, 34]. Growth past the cap adds clusters. A laptop is one cell and has no router, which is CELL-WP's "cell zero" [p. 47].
2. **The partition key is the bucket.** Every S3 data-plane request names its bucket, in the host or the path (note 05 §14), so it meets the "easily accessible in most API calls" test [p. 22].
   - Size the cell so the largest bucket fits ("Big enough to fit the largest workloads", p. 34), and scale a bucket inside its cell with range splits (§3, §7). The cell cap then bounds the blast radius of deployments and poison requests, not per-bucket throughput.
   - The cross-grain calls are ListBuckets, and CopyObject or UploadPartCopy from a source bucket in another cell. They go through the router, never cell to cell [p. 22].
3. **Map bucket → cell with a full, versioned table plus an override table.** Full mapping gives the most control over hot cells and migration [pp. 23-24]. Its costs are bounded here: buckets are few and are created by a control-plane action [FIB p. 7]. Use the two-level hash → slot → cell form [p. 26] only where keys must be spread without per-key state.
4. **A stateless, thin router.**
   - It extracts the bucket, looks it up in an in-memory map and forwards the request. It does no SigV4 verification and no metadata reads [pp. 27-28], so a poison request crashes a cell, not the shared router.
   - The map arrives as a full, fixed-size snapshot on a jittered constant-work loop [BL-CONSTANT pp. 4-7; BL-SMALLER pp. 2-4]. If the control plane is down, the router keeps the last snapshot [CELL-WP p. 16].
5. **No circular dependency** [FIB p. 8]. Never distribute the router's map or cell configuration through mantle's own S3 object path, which needs that map to route. Serve it from the control plane's own replicated metadata group.
6. **Stale routing must be safe.** CELL-WP's "data planes generally prefer AP" [p. 16] fits routing hints only; object semantics need a single authoritative owner.
   - The source cell keeps a tombstone naming the new cell and the map version, and answers a stale route with a redirect.
   - The router refreshes and retries once, within a bounded budget.
   - This is CELL-WP's "cross-cell redirects ... against different versions of the mapping algorithm state" [p. 41], made safe by fencing epochs (§7; §9.6).
7. **Build bucket migration now,** following the four phases [p. 41] and "from day one" [p. 47]:
   1. Copy the bucket's metadata ranges and chunks to the target cell as a non-authoritative follower.
   2. Fence source writes, catch up, and flip the map entry with an epoch bump.
   3. Redirect requests from the source.
   4. Forget after a grace period, as the chunk store already does for deleted data (docs/design/chunk-store.md §8).
8. **A statically stable data plane.**
   - Storage nodes, metadata Raft groups and gateways keep serving reads and writes with no control plane. Only placement changes stop: new cells, bucket moves, rebalancing, repair scheduling [FIB p. 8; BL-STATIC p. 3]. Tectonic already runs its rebalancer and repair as separate background services (note 01 §1.10).
   - Pre-provision failover headroom instead of scaling on failure: +50% for three AZs [BL-STATIC pp. 4-5; FIB p. 12].
9. **Deploy one cell at a time, canary first** [CELL-WP pp. 13, 43]. Within a cell, deploy one AZ or failure domain at a time, on different days [BL-STATIC p. 7]. Tag every metric with its cell [CELL-WP p. 45].
10. **The control plane is the smaller fleet.** Data-plane nodes either open long-lived connections that the control plane paces, or poll snapshots. Storage nodes must never make synchronous, synchronized calls to it after an outage [BL-SMALLER pp. 2, 6].
11. **Cells are not failover domains** [CELL-WP p. 20]. Cross-AZ durability comes from chunk placement inside a multi-AZ cell (note 04), not from failing buckets over between cells.
12. **Shuffle sharding stays inside a cell.** Use it for gateways, queues and throttles, never across cells [CELL-WP p. 49]. See §8.5.

### 5.14 UNVERIFIED / not found

- **Which AWS services use which pattern.** CELL-WP names no service for any router, mapping or migration pattern it describes. Its "more than a decade" claim is unspecific [p. 1]. S3's internal cell structure is **UNVERIFIED** here; see §6.
- **Evidence for the claimed benefits.** CELL-WP gives no numbers for its MTBF, MTTR or availability claims [pp. 12-13].
- **Talks and videos.** re:Invent 2018 ARC338 and the "Physalia: Cell-based Architecture to Provide Higher Availability on Amazon EBS" video [p. 51] were not reviewed.
- **Route 53's overlap guarantee.** Whether Route 53, or any other service, uses Infima's stateful sharder or an equivalent overlap guarantee in production is unknown. BL-SHUFFLE states that no domain will "share more than two virtual name servers" with another [p. 6], but not how that is enforced.
- **Builder's Library dates.** These are known only from the PDF copyright lines (2019, 2019, 2020, 2021). The HTML pages now load an AWS Builder Center single-page app that was not readable without JavaScript.
- **S3 dependency list.** FIB's list of S3 operations that depend on us-east-1 [p. 19] reflects the 2026-09-28 build and may change.

---

---

## 6. S3 internals: partitioning, request-rate scaling, routing and strong consistency (what AWS has published)

Everything in this section is **NON-PEER-REVIEWED** unless it is tagged PARIS86. No peer-reviewed paper describes S3's index, partitioning or consistency protocol.

Related material elsewhere:

- ShardStore, the one peer-reviewed S3 component, is in §2.
- The TLA+ work AWS reported for S3 is in §4.
- Note 05 already covers the client-visible side: the consistency model's API semantics (note 05 §13), the `SlowDown` error code (note 05 §11.2), listing order (note 05 §6.1) and addressing (note 05 §14).

This section covers the mechanisms behind those semantics.

### 6.1 The architecture AWS has drawn in public

- **Four fleets.** "S3 is an object storage service with an HTTP REST API. There is a frontend fleet with a REST API, a namespace service, a storage fleet that's full of hard disks, and a fleet that does background operations." [WARFIELD23, "How S3 works"]
  - The accompanying whiteboard photo (alt text "Whiteboard drawing of S3") labels the boxes "WebServer Fleet", "Name Space (Big KV Store)", "Storage Management (Background Async stuff)" and "Storage Fleet" over "Hard Disks".
- **Microservices.** "AWS tends to ship its org chart"; "S3 today is composed of hundreds of microservices" [WARFIELD23, "How S3 works"]. The 2023 re:Invent deck says "350+ microservices" and "All AWS Regions" [STG314-23, slide 9].
- **Three layers** [STG314-23, slides 10, 27, 45]:
  - "Front end: Web servers, DNS, and network"
  - "Index: Key/value mapping to storage"
  - "Storage: Data durably stored on devices"
- **Metadata subsystem.** "Per-object metadata is stored within a discrete S3 subsystem. This system is on the data path for GET, PUT, and DELETE requests, and is responsible for handling LIST and HEAD requests. At the core of this system is a persistence tier that stores metadata." [VOGELS21, "S3's Metadata Subsystem"] The 2026 post calls it "the index subsystem" [S3-BLOG26].
- **Rust rewrite.** "Over the past 8 years, AWS has been progressively rewriting performance-critical code in the S3 request path in Rust. Blob movement and disk storage have been rewritten" [S3-BLOG26]. The disk layer is ShardStore [WARFIELD23, "The human factors"]; see §2.
- **DERIVED mapping to Tectonic** (see note 01 §1.2 and §1.5):
  - S3 front end ≈ mantle's S3 gateway.
  - S3 index ≈ Tectonic's Name and File layers.
  - S3 storage fleet ≈ Tectonic's Chunk Store.
  - Whether S3's index or a separate service holds object-to-disk locations (Tectonic's Block layer) is **UNVERIFIED**. No source says.

### 6.2 The index is range-partitioned by key name and splits for load and for size

#### 6.2.1 The current documented contract

- **Headline rates.** "Amazon S3 automatically scales to high request rates. For example, your application can achieve at least 3,500 PUT/COPY/POST/DELETE or 5,500 GET/HEAD requests per second per partitioned Amazon S3 prefix. There are no limits to the number of prefixes in a bucket." [S3UG-PERF]
  - Worked example: "if you create 10 prefixes in an Amazon S3 bucket to parallelize reads, you could scale your read performance to 55,000 read requests per second."
- **Scaling is gradual.** "The scaling, in the case of both read and write operations, happens gradually and is not instantaneous ... While Amazon S3 is scaling to your new higher request rate, you may see some 503 (Slow Down) errors. These errors will dissipate when the scaling is complete." [S3UG-PERF]
- **Definition of prefix.** "A prefix is a string of characters at the beginning of the object key name. A prefix can be any length, subject to the maximum length of the object key name (1,024 bytes)." [S3UG-PREFIX]. The slides say the same: "Any string of characters after the bucket name" [STG314-23, slide 34].
  - *DERIVED:* A "partitioned prefix" is therefore not a `/`-delimited folder. It is whatever key range the index currently treats as one partition. AWS never defines its boundaries (§6.9).

#### 6.2.2 What AWS said about the "keymap" in 2012 and 2017 (historical)

These sources predate the 2018 rate increase and may not describe today's system. They are the only primary sources that describe the mechanism in words.

- **The keymap.** "S3 must maintain a 'map' of each bucket's object names, or 'keys' ... each key in this 'keymap' (that's what we call it internally) is stored and retrieved based on the name provided when the object is first put into S3" [S3-BLOG12]. "Internally, the keys are all represented in S3 as strings like this: bucketname/keyname. Further, keys in S3 are partitioned by prefix." [S3-BLOG12]
- **Split triggers.** "S3 has automation that continually looks for areas of the keyspace that need splitting. Partitions are split either due to sustained high request rates, or because they contain a large number of keys (which would slow down lookups within the partition)." [S3-BLOG12]
- **Split cost.** "There is overhead in moving keys into newly created partitions ... This split operation happens dozens of times a day all over S3 and simply goes unnoticed from a user performance perspective. However, when request rates significantly increase on a single partition, partition splits become detrimental to request performance." [S3-BLOG12]
- **Multi-way splits.** "S3 even has an algorithm to detect this parallel type of write pattern and will automatically create multiple child partitions from the same parent simultaneously – increasing the system's operations per second budget as request heat is detected." [S3-BLOG12]
- **Sequential identifiers.** With incrementing IDs or timestamps, "all new content will necessarily end up being owned by a single partition", and older partitions "get cold much faster ... effectively wasting the available operations per second that each partition can support" [S3-BLOG12].
- **Planning figure.** "If we target conservative targets of 100 operations per second and 20 million stored objects per partition, a four character hex hash partition set in a bucket or sub-bucket namespace could theoretically grow to support millions of operations per second and over a trillion unique keys" [S3-BLOG12].
- **Key order (2017).** "Amazon S3 maintains an index of object key names in each AWS region. Object keys are stored in UTF-8 binary ordering across multiple partitions in the index. The key name dictates which partition the key is stored in." [S3-RRPC17] This is the same order ListObjects returns (note 05 §6.1).
- **2017 thresholds.** Key-naming guidance applied above "100 PUT/LIST/DELETE requests per second or more than 300 GET requests per second". For "a rapid increase in the request rate for a bucket to more than 300 PUT/LIST/DELETE requests per second or more than 800 GET requests per second", AWS recommended opening a support case "to prepare for the workload and avoid any temporary limits on your request rate" [S3-RRPC17].
- **Longer partition prefixes.** "for very large workloads (more than 2000 requests per seconds or for bucket that contain billions of objects), Amazon S3 can use more characters for the partitioning scheme. Amazon S3 can automatically split these partitions further as the key count and request rate increase over time." [S3-RRPC17]

#### 6.2.3 The 2018 change, and today's mixed guidance

- **The 2018 announcement.** "at least 3,500 requests per second to add data and 5,500 requests per second to retrieve data ... Each S3 prefix can support these request rates ... Performance scales per prefix ... There are no limits to the number of prefixes." [S3-WN18]
  - "This S3 request rate performance increase removes any previous guidance to randomize object prefixes to achieve faster performance. That means you can now use logical or sequential naming patterns in S3 object naming without any performance implications." [S3-WN18]
  - The consistency product page still says "you do not need to randomize object prefixes" [S3-CONSPAGE, "Performance"].
- **Today's User Guide says the opposite for hot workloads.** Its section "Optimizing for high-request rate workloads" recommends: "Distribute requests across multiple prefixes – Use a randomized or sequential prefix pattern to spread requests across multiple partitions. For example, instead of using sequential object names like log-2024-01-01.txt, use randomized prefixes like a1b2/log-2024-01-01.txt." [S3UG-PATTERNS]
  - The re:Invent 2022 deck makes the same point: "Entropy added to the start of the prefix"; "Entropy can help for bursts" [STG203-22, slides 36–37].
  - *DERIVED:* Randomization is no longer needed to reach the per-prefix floor. It still matters for how fast a new hot range can gain capacity.

#### 6.2.4 The 2022–2023 re:Invent decks: split-for-heat, drawn

- **Scale.** "The S3 index is big: 350 trillion objects, 100+ million requests per second" [STG314-23, slide 29].
- **Capacity chart** [STG314-23, slide 30]. It is labelled "Usage" and "Capacity": capacity rises in steps just ahead of usage, and a red "!" marks the point where usage reaches capacity.
- **Range diagram** [STG314-23, slides 31–32]:
  - Slide 31 shows key ranges "A-F", "G-M", "N-S", "T-Z", each served by its own group of servers. N-S has many more clients and a load gauge in the red.
  - Slide 32 shows "N-S" replaced by "N-P" and "Q-S" with "SPLIT" between them, each on its own servers, with both gauges back in the green.
- **Split tree** [STG314-23, slides 36, 38]:
  - "3,500 PUT requests / 5,500 GET requests per prefix".
  - `reinvent-bucket` (5,500 GETs/second) splits into `.../prefix1` and `.../prefix2`. Each of those splits into `/a` and `/b`, and each leaf is labelled 5,500 GETs/second.
  - "Total TPS to reinvent-bucket is 5,500 x 4 = 22,000 TPS".
- **Key-naming guidance** [STG314-23, slides 41–44]:
  - "Keep cardinality to the left in key names" and "Keep dates to the right in key names".
  - With the date first: "The partitions from day1 are now unused", "We will need to split day2 as it sees sustained load", "Likely throttling as that process occurs".
- **Bursty workloads** [STG203-22, slide 35]. A chart titled "Designing prefixes for 'bursty' workloads" shows five writers that first "May 'plateau' around 3,500 PUTs/sec (total)". After "Prefix has scaled up, raising 'goodput'", each writer runs at about 3,000–3,500 TPS. The time axis is labelled only "Time" (1–19), with no unit.

#### 6.2.5 Synthesis (DERIVED from 6.2.1–6.2.4)

The public model of S3's general-purpose index:

1. It is an ordered key-value index over `bucket/key`, range-partitioned in UTF-8 binary order.
2. Each partition has a capacity floor of ≥3,500 write-class and ≥5,500 read-class requests/s.
3. A partition splits when *sustained* load or key count warrants it. A parent can split into several children at once. Split points fall at any character position, not only at `/`.
4. Aggregate throughput grows with the number of partitions.
5. While a split is under way, the partition answers 503 Slow Down, and clients are expected to back off.

Merging of cold partitions is never mentioned (§6.9).

### 6.3 Backpressure: 503 Slow Down is S3's load-shedding signal

- **Documented behavior** [S3UG-PATTERNS, "Timeouts and retries for latency-sensitive applications"]:
  - "Amazon S3 maps bucket and object names to the object data associated with them. If an application generates high request rates (typically sustained rates of over 5,000 requests per second to a small number of objects), it might receive HTTP 503 slowdown responses. If these errors occur, each AWS SDK implements automatic retry logic using exponential backoff."
  - "Amazon S3 automatically scales in response to sustained new request rates ... While Amazon S3 is internally optimizing for a new request rate, you will receive HTTP 503 request responses temporarily until the optimization completes."
- **Why SlowDown happens.** "S3 has protection mechanisms which detect intentional or unintentional resource over-consumption and react accordingly. SlowDown errors can occur when a high request rate triggers one of these mechanisms." [S3DG-ERR, "Amazon S3 error best practices"]
- **Error code and billing.** The code is `SlowDown`, "Please reduce your request rate.", HTTP "503 Slow Down" (note 05 §11.2). "Bucket owners aren't billed for HTTP 5XX server error responses, such as HTTP 503 Slow Down errors." [S3DG-ERR]
- **Knowledge Center** [REPOST-5XX]:
  - "When you create a prefix, Amazon S3 doesn't automatically assign additional resources for the supported request rate. Amazon S3 scales based on request patterns."
  - "In some scenarios, rapid concurrent requests to the same key can result in a 503 response."
  - Exponential backoff "allows Amazon S3 time to monitor the request patterns and scale in the backend".
  - Splitting a bucket's objects across `images` and `videos` prefixes means "the bucket can manage double the request rate".
- **Client-side guidance** [S3UG-PATTERNS; S3UG-GUIDE]:
  - Ramp up gradually "rather than immediately jumping to peak rates. This allows Amazon S3 to scale proactively". The timed-retry rules are in the appendix rows.
  - "When you retry a request, we recommend using a new connection to Amazon S3 and performing a fresh DNS lookup" [S3UG-PATTERNS]. S3UG-GUIDE adds that "if the first request is slow, a retried request is likely to take a different path and quickly succeed."

### 6.4 Routing requests to the right place

- **DNS routing with redirect on misroute** [S3DG-ROUTE]:
  - "Amazon S3 uses the Domain Name System (DNS) to route requests to facilities that can process them. This system works effectively, but temporary routing errors can occur. If a request arrives at the wrong Amazon S3 location, Amazon S3 responds with a temporary redirect that tells the requester to resend the request to a new endpoint."
  - Misroutes are "most likely to occur immediately after buckets are created or deleted".
  - "Don't reuse an endpoint provided by a previous redirect response. It might appear to work (even for long periods of time), but it might provide unpredictable results and will eventually fail without notice."
  - The example `TemporaryRedirect` body says "Continue to use the original request endpoint for future requests."
  - The prose says the redirect is "an HTTP 302 response", but the example is `HTTP/1.1 307 Temporary Redirect`. That is an inconsistency in AWS's own page.
  - Regions launched after March 20, 2019 return 400 instead of redirecting (note 05 §14).
- **Front-end fleet via multi-value DNS** [STG314-23, slides 20–24]:
  - Under "Spreading requests across the fleet / IT'S ALWAYS DNS", `nslookup s3.amazonaws.com` returns eight addresses, labelled "Multi-value answers" and "NEW".
  - The AWS Common Runtime implements "Built-in retry logic using multiple IPs".
  - "DNS queries for Amazon S3 cycle through a large list of IP endpoints" [S3UG-PATTERNS], and "Amazon S3 doesn't have any limits for the number of connections made to your bucket" [S3UG-GUIDE].
  - Slide 72, "Availability during zone failure / IT'S ALWAYS DNS", shows one zone's servers marked with an X while the DNS answer lists five addresses. *DERIVED reading:* failed-zone front ends are withdrawn from DNS answers.
- **Directory buckets make the placement cell explicit** [S3UG-DIRB; S3UG-XEND; S3-BLOG23X]:
  - The bucket name embeds the zone: "bucket-base-name--zone-id--x-s3".
  - "Bucket-level (or control plane) API operations are available through Regional endpoints ... Examples ... are CreateBucket and DeleteBucket."
  - "Object-level (or data plane) API operations are available through Zonal endpoints ... Examples ... are CreateSession and PutObject."
  - `CreateSession` "returns a session token that grants access to a specific bucket for five minutes" [S3-BLOG23X].
- **Isolation units.** In the sources reviewed here, AWS never calls S3 "cell-based". The isolation units S3 does name are:
  - **Regions:** "The availability of one AWS Region can never affect the availability of another"; "Regional isolation — A LEARNED TENET — Amazon S3: 2006 - 2010" [STG314-23, slides 68–69].
  - **Availability Zones:** "Regional storage classes span 3+ AZs" [STG314-23, slide 67]. Slide 73 is headed "AZ fault tolerance: Not just for AZ faults!" and lists "Software deployments", "New hardware adoption", "Configuration changes" and "Separate fault domains".

### 6.5 Directory buckets: a hierarchical, unsorted index with per-bucket TPS quotas

- **Namespace** [S3UG-DIRB, "Directories"]:
  - "Directory buckets organize data hierarchically into directories as opposed to the flat storage structure of general purpose buckets. There aren't prefix limits for directory buckets, and individual directories can scale horizontally."
  - "The directory bucket indexing model returns unsorted results for the ListObjectsV2 API operation."
  - Prefixes "must end in a delimiter and only '/' can be specified as the delimiter" [S3UG-XDIFF].
  - Deleting an object also "recursively deletes any empty directories in the object path" [S3UG-XDIFF].
  - The launch post: "list operations return results without first sorting them, so you cannot do a 'start after' retrieval" [S3-BLOG23X].
- **Rates.** "By default, each directory bucket supports up to 200,000 reads and up to 100,000 writes per second", raisable "up to 2 million reads and up to 200,000 writes per second" through AWS Support [S3UG-XPERF]. The quota table lists 200,000 GET/HEAD TPS and 100,000 PUT/DELETE TPS per directory bucket [S3UG-DIRB].
- **Placement.** "Objects are stored and replicated on purpose built hardware within a single AWS Availability Zone" [S3-BLOG23X]. The 2023 deck states "99.999999999% data durability WITHIN A SINGLE AVAILABILITY ZONE" and warns that "Full or partial loss of an Availability Zone may lose my data in S3 Express One Zone" [STG314-23, slides 60–63].
- **Idle buckets.** A bucket idle for at least 90 days goes inactive. On the next request it reactivates "typically within a few minutes", and until then "reads and writes return an HTTP 503 (Service Unavailable) error code" [S3UG-DIRB].
- *DERIVED:* AWS's high-throughput bucket type gave up sorted flat listing in favor of a hierarchical, per-directory index with unsorted listing. That is the shape Tectonic chose for its hash-partitioned Name layer (note 01 §1.5, §6.2). How directory buckets actually partition directories is **UNVERIFIED**.

### 6.6 Strong consistency: a cache-coherence protocol with a "witness"

- **Where eventual consistency came from.** "Our persistence tier uses a caching technology that is designed to be highly resilient. S3 requests should still succeed even if infrastructure supporting the cache becomes impaired. This meant that, on rare occasions, writes might flow through one part of cache infrastructure while reads end up querying another. This was the primary source of S3's eventual consistency." [VOGELS21, "S3's Metadata Subsystem"]
- **The option they rejected.** Bypassing the cache "wouldn't meet our bar for no tradeoffs on performance. We needed to keep the cache." [VOGELS21]
- **The mechanism** [VOGELS21, "Cache Coherence"]:
  - "We had introduced new replication logic into our persistence tier that acts as a building block for our at-least-once event notification delivery system and our Replication Time Control feature. This new replication logic allows us to reason about the 'order of operations' per-object in S3. This is the core piece of our cache coherency protocol."
  - "We introduced a new component into the S3 metadata subsystem to understand if the cache's view of an object's metadata was stale. This component acts as a witness to writes, notified every time an object changes. This new component acts like a read barrier during read operations allowing the cache to learn if its view of an object is stale. The cached value can be served if it's not stale, or invalidated and read from the persistence tier if it is stale."
- **Availability of the witness** [VOGELS21, "High Availability"]:
  - Witnesses "only need to track a little bit of state, in-memory, without needing to go to disk"; "We can continue to scale this fleet out as S3 continues to grow."
  - "We built automation that can respond rapidly to load concentration and individual server failure. Because the consistency witness tracks minimal state and only in-memory, we are able to replace them quickly without waiting for lengthy state transfers."
- **Correctness bar** [VOGELS21, "Correctness"]:
  - The protocol must hold under "concurrent writes to the same object", and must not let values "flicker" between old and new. It must also hold under "very high concurrency on GET, LIST, PUT, and DELETE while having versioning enabled and having a deep version stack".
  - "even if something happens only once in a billion requests, that means it happens multiple times per day within S3".
- **How it was verified.** "integration tests, deductive proofs of our proposed cache coherence algorithm, model checking to formalize our consistency design and to demonstrate its correctness, and we expanded on our model checking to examine actual runnable code." These "were more work, in fact, than the actual implementation itself" [VOGELS21].
  - "P was used for creating formal models of all the core distributed protocols involved in S3's strong consistency" [PCASES, "Amazon S3 Strong Consistency"].
  - In 2026: "When engineers check in code to the index subsystem, automated proofs verify that consistency hasn't regressed." [S3-BLOG26]
- **Scope, cost and isolation claims** [S3-BLOG20; S3-CONSPAGE]:
  - "all S3 GET, PUT, and LIST operations, as well as operations that change object tags, ACLs, or metadata, are now strongly consistent ... There's no impact on performance, you can update an object hundreds of times per second if you'd like, and there are no global dependencies."
  - "without sacrificing regional isolation for applications, and at no additional cost".
  - Vogels contrasts this with providers "implementing consistency with dependencies across regions which undermine the regional availability of a service" [VOGELS21].
- **The witness idea's lineage (peer-reviewed).** VOGELS21 hyperlinks the word "witness" to PARIS86.
  - Pâris's witnesses are "mere recordings of the version number of the file with no data attached to it", which "participate like them [copies] to the collection of quorums" [PARIS86 §3.1].
  - "A witness contains only a version number that always reflects the most recent write recorded by the witness" [PARIS86 §3.2].
  - "every quorum must include at least one current copy" [PARIS86 §3.3].
  - Pâris's witnesses "are to be stored in stable storage" [PARIS86 §3.1].
  - *DERIVED contrast:* S3's witness is in-memory and acts as a read barrier in front of caches. AWS does not say it votes in quorums. The shared idea is a data-less record of the latest version that can tell a reader whether its copy is current.
- *DERIVED cost:* Every cached read consults the witness, so the witness is on the read path of the whole metadata tier. That explains the emphasis on in-memory state, high request rates and quick replacement.

### 6.7 Scale, heat and placement

- **Scale over time.** The figures are in the appendix rows. They are as stated at each date (2021, 2023, 2026) and are not comparable metrics. Drive counts went from "Millions of them" [WARFIELD23] to "tens of millions S3 hard drives" [S3-BLOG26].
- **HDD physics** [WARFIELD23, "Technical Scale"]:
  - Random I/O: "you can expect about 120 operations per second".
  - With 200 TB drives, "if we divide up our random accesses fairly across all our data, we will be allowed to do 1 I/O per second per 2TB of data on disk."
- **Heat** [WARFIELD23, "Managing heat"]:
  - "By heat, I mean the number of requests that hit a given disk at any point in time."
  - "hotspots at individual hard disks create tail latency". Stalls "get amplified by dependent I/Os for metadata lookups or erasure coding".
  - Aggregation: "once you aggregate to a certain scale you hit a point where it is difficult or impossible for any given workload to really influence the aggregate peak at all!"
  - 2026: "The larger S3 gets, the more de-correlated workloads become, which improves reliability for everyone." [S3-BLOG26]
- **Placement for heat** [WARFIELD23, "Replication" and "The impact of scale on data placement strategy"]:
  - Replication "gives you the freedom to read from any of the disks".
  - "While individual objects may be encoded across tens of drives, we intentionally put different objects onto different sets of drives, so that each customer's accesses are spread over a very large number of disks."
  - "A customer's data only occupies a very small amount of any given disk, which helps achieve workload isolation, because individual workloads can't generate a hotspot on any one disk."
  - A burst "can be served by over a million individual disks", and "tens of thousands of customers with S3 buckets ... are spread across millions of drives".
- **Durability and guardrails:**
  - Durability rests on "End-to-end integrity checking of requests", "Data always stored on redundant devices" and "Periodic durability auditing for data at rest" [STG314-23, slide 50]. At scale that means "Adequate spare capacity always on hand" [STG314-23, slide 54].
  - "auditor services examine data and automatically trigger repair systems" [S3-BLOG26].
  - Durability reviews separate "risk from countermeasures" and favor coarse-grained "guardrails" [WARFIELD23, "The human factors"]. The deck's examples of guardrails are "Shadow mode" and "Control plane limits" [STG314-23, slide 74].
  - ShardStore's executable model is "about 1% of the size of the real system" (§2) [WARFIELD23].

### 6.8 Implications for mantle (INFERENCE)

1. **Emulate the per-range contract S3 clients are tuned for, not AWS's numbers.**
   - Clients and SDKs expect three things: a capacity floor per key range; more capacity when load is *sustained*, gained by splitting; and 503 `SlowDown` while that happens, which they retry with exponential backoff [S3UG-PERF; S3UG-PATTERNS].
   - mantle's gateway should map two states, a range over its admission budget and a range in mid-split, to 503 `SlowDown` (note 05 §11.2), and should not count throttled requests against tenant request budgets [S3DG-ERR].
   - The per-range capacity must be measured on mantle's hardware (CLAUDE.md §4). The 3,500/5,500 figures describe AWS's fleet only.
2. **Split metadata ranges on sustained load and on size, anywhere in the key.**
   - S3 splits on "sustained high request rates" or "a large number of keys" [S3-BLOG12; S3-RRPC17].
   - It splits at arbitrary character positions [STG314-23, slides 31–32, 38].
   - It can create several children at once when a parent is uniformly hot [S3-BLOG12].
   - Each child is an independent capacity unit [STG314-23, slide 38].
   - This matches the multi-Raft split-as-a-log-command design in note 06 (A4.3, A4.4). Only *sustained* load should trigger a split, so bursts do not cause churn.
3. **Sequential-key hotspots are inherent to a sorted index.**
   - S3 did not remove this hotspot for general-purpose buckets. It publishes key-naming advice and accepts throttling while the hot tail splits [STG314-23, slides 41–44; STG203-22, slides 35–37; S3UG-PATTERNS].
   - A range-partitioned mantle index that serves S3's lexicographic LIST (note 05 §6.1) inherits the same pathology.
   - AWS's own escape hatch is a second bucket type: hierarchical directories, unsorted LIST, and per-bucket TPS quotas [S3UG-DIRB; S3UG-XDIFF]. That is the Tectonic hash-partitioned-directory shape (note 01 §6.2, option A).
   - Evidence exists for either choice, or for both behind a bucket-type flag. The decision belongs in the design record.
4. **Keep any metadata cache coherent with a per-read freshness check.**
   - S3 kept its caches and added a read barrier that "sees" every write [VOGELS21].
   - In mantle the Raft leaseholder of the key's range already sees every write. A gateway or node cache can validate an entry by asking the leaseholder for the key's current version (ReadIndex-style, note 06 A1.4). This gives S3's witness semantics without a new fleet.
   - Entries that name immutable, versioned data (a sealed object version, a block's chunk list at a layout epoch) need no barrier.
   - LIST must stay consistent too [S3-BLOG20], so the listing index must be updated in the same linearizable step as the object's version pointer (note 05 §13).
5. **Consistency stays inside the region cell.** S3 offers strong consistency with "no global dependencies" and a "Regional isolation" tenet [S3-BLOG20; STG314-23, slides 68–69]. A mantle cluster should need no other cluster on its request path. Cross-cluster features (replication, global bucket names) must be asynchronous.
6. **Keep routing thin, and redirect on misroute.**
   - S3 routes with DNS to any front end. A request that reaches the wrong place is redirected, and the redirect is explicitly *not* a cache entry [S3DG-ROUTE].
   - Directory buckets encode their zone in the bucket name and split control-plane endpoints from data-plane endpoints [S3UG-DIRB; S3UG-XEND].
   - For mantle:
     - Any gateway accepts any S3 request.
     - Gateway-to-range routing uses cached range descriptors with epochs, and a stale descriptor is rejected with a redirect (note 06 A4.1, A4.4).
     - Object operations must not depend on the bucket-management (control) path being available.
7. **Balance heat, not only bytes.**
   - At about 120 random IOPS per HDD, I/O per TB shrinks as drives grow [WARFIELD23].
   - S3 spreads each customer's objects over very many drive sets so that no workload can hot-spot a disk [WARFIELD23].
   - mantle's placement (note 01 C2: copysets drawn from about 100 shuffles and chosen per block ID) already spreads one bucket's blocks across many copysets.
   - The rebalancer should also track per-disk demand, using Tectonic's disk-time accounting (note 01 §1.11), and not just fullness. On a single-disk laptop this degenerates to a no-op.
8. **Bound the control plane's reach.** "Control plane limits" and "Shadow mode" are S3's named guardrails [STG314-23, slide 74]. mantle's rebalancer, splitter and drain logic should have explicit rate caps: ranges moved, disks drained and splits per unit time. This limits the blast radius of a control-plane bug.
9. **Model-check the consistency protocol and gate commits on it.**
   - S3 modeled its consistency protocols in P [PCASES] and runs automated proofs on every index-subsystem check-in [S3-BLOG26].
   - mantle's range split, lease and cache-validation protocol should have a checked model (TLA+ or P) in CI, alongside the deterministic simulation in note 06 C.d.

### 6.9 UNVERIFIED / not found

AWS has **not** published any of the following, so mantle cannot infer them from S3:

- **Partition boundaries** and how split points are chosen. Also undefined: what exactly a "partitioned prefix" is (the docs say "per partitioned prefix", the slides say "per prefix").
- **Split thresholds**: the load or key-count level, measurement windows, and hysteresis. The 2012 "100 operations per second and 20 million stored objects per partition" is an old planning figure [S3-BLOG12], not a current threshold.
- **Split latency.** The docs say only "gradually" and "not instantaneous" [S3UG-PERF]. The STG203-22 chart has no time unit.
- **Merges.** Whether cold partitions are ever merged is never stated. STG314-23 says only that "partitions from day1 are now unused".
- **Replication of index partitions**: the replica count, consensus or replication protocol, AZ placement, and leader or lease mechanics.
- **Witness details**: how the witness is sharded (per object, per partition?) and replicated; how a replacement witness learns current state "without waiting for lengthy state transfers"; and what a reader does when no witness is reachable, whether it fails or bypasses the cache. VOGELS21 gives none of these.
- **The "order of operations" replication logic**: its protocol and its relation to event notifications and Replication Time Control, beyond VOGELS21's sentence.
- **The CACM 2025 correctness article and the re:Invent 2024/2025 decks**: whether they add any of the above. They were not accessible or not reviewed (see Sources).
- **Whether S3 calls any internal unit a "cell"**: not stated in the sources reviewed here. §5 covers AWS's generic cell guidance.
- **Directory-bucket internals**: how directories are indexed and partitioned (hash or range?), and what "individual directories can scale horizontally" means mechanically.
- **Object-to-disk location**: where the mapping lives (index or a separate service), and how "different objects onto different sets of drives" is chosen: random, shuffled or copyset-like.
- **Why 503 on a single hot key** [REPOST-5XX]: whether this is a per-key limit, witness or cache concentration, or partition overload.

---

## 7. Moving and rebalancing partitions safely

This section covers the peer-reviewed systems that publish how they move, split, merge and rebalance the units they shard, and how they keep clients routing correctly while they do. Note 06 §A4 already covers Bigtable, Spanner, CockroachDB and TiDB for range location and splits. Note 01 §5 covers ZippyDB. The subsections below add what those notes lack.

| § | System | What it contributes |
|---|---|---|
| 7.1 | Slicer (Google, OSDI '16) | A centralized assigner kept off the request path. Its weighted-move balancer has a churn budget, and its measured behavior without the control plane is published. |
| 7.2 | Centrifuge (Microsoft, NSDI '10) | Manager-granted leases on ranges, and what they cost in availability. |
| 7.3 | Shard Manager (Meta, SOSP '21) | Graceful primary handoff, gating of planned maintenance, a constraint-solver allocator, and a sharded control plane. ZippyDB runs on it. |
| 7.4 | Windows Azure Storage (Microsoft, SOSP '11) | A complete peer-reviewed cell design for a blob store: stamps, a location service, inter-stamp migration, and exact split, merge and move procedures. |
| 7.5 | Aurora (AWS, SIGMOD '17/'18) | Replica replacement by overlapping quorum sets and membership epochs, without consensus. |
| 7.6 | CockroachDB (SIGMOD '20, plus docs) | Rebalancing signals, lease placement, and merge safety (aligned replicas, freeze, generation counter). |
| 7.7 | Dynamo (Amazon, SOSP '07) | Fixed partitions versus random tokens, as measured in production, and client-side routing tables. |
| 7.8 | Akkio (Meta, OSDI '18) | Moving small application-defined units between replica sets: two protocols and a fenced, resumable mover. |
| 7.9 | Spanner (Google, OSDI '12) | Movedir (background copy, then an atomic cutover), plus a cross-system comparison table. |

### 7.1 Slicer (Adya et al., OSDI '16): Google's general-purpose auto-sharder

**Why it is here.** Slicer is the most detailed peer-reviewed description of a **centralized assignment service** kept off the request path. The Slicer Service computes a key-range → server assignment. Clients and servers cache the assignment and route on it locally. The paper measures what happens when the central service is down. Of the three systems in this section, it has the most exact load-balancing algorithm and the most complete production numbers.

#### 7.1.1 Scope and goals

- **Goal:** "Sharding is a fundamental building block of large-scale applications, but most have their own custom, ad-hoc implementations. Our goal is to make sharding as easily reusable as a filesystem or lock manager." [SLI Abstract, p. 739]
- **Architecture thesis.** "Slicer has the consistency and global optimization of a centralized sharder while approaching the high availability, scalability, and low latency of systems that make local decisions. It achieves this by separating concerns: a reliable data plane forwards requests, and a smart control plane makes load-balancing decisions off the critical path." [SLI Abstract, p. 739]
- **Headline numbers.** "It currently handles 2-7M req/s of production traffic. The median production Slicer-managed workload uses 63% fewer resources than it would with static sharding." [SLI Abstract, p. 739]
- **Why custom sharders fail.** They "typically make do with simplistic static sharding that is unresponsive to changes in workload distribution and task availability". When a datacenter fails, "a great wave of traffic sloshes over to the remaining datacenters, dramatically altering the request mix" [SLI §1, p. 739].
- **What it does dynamically.** Slicer "monitors the request load to detect hotspots. It monitors task availability changes due to service provisioning, system updates, and hardware failures. It rebalances the key mapping to maintain availability of all keys and reduce load imbalance among tasks while minimizing key churn." [SLI §1, p. 740]
- **Why the control plane is a separate service.** "In a production environment, customers cannot tolerate flag days (synchronized restarts). By separating the forwarding data plane from the policy control plane, Slicer simplifies customer-linked libraries and keeps complexity in a central service where the team can more easily coordinate changes." [SLI §1, p. 740]
- **Unit of management: one job in one datacenter.** "Slicer is a general-purpose sharding service that splits an application's work across a set of tasks that form a job within a datacenter" [SLI §2, p. 740].
  - "Slicer makes assignments for one job in one datacenter at a time. Customers who run jobs in multiple datacenters use a higher-level Google load balancer to route a request to a datacenter, and then within that datacenter, use Slicer to pick one task from the job." [SLI §4.1, p. 744]
  - **DERIVED:** in cell terms, Slicer's scope is a per-datacenter cell, and a separate global layer routes requests to the cell.
- **Production uses.** There are three categories: in-memory cache, in-memory store and aggregation [SLI §3, p. 742]. Examples:
  - Flywheel's website-reachability tracker [SLI §3.1.1, p. 742].
  - The speech recognizer, which assigns one language model per key [SLI §3.2.1, p. 743].
  - Cloud DNS, "which hosts millions of domains", in affinity mode [SLI §3.2.2, p. 743].
  - Event pipelines that aggregate writes by key [§3.3, p. 743].
- **Scale of adoption.** "Slicer is used by more than 20 client services at Google, and it balances 2-7M requests per second with more than 100,000 application client processes and server tasks connected to it" [SLI §3, p. 742].

#### 7.1.2 Sharding model: hashed keys, slices, assignments, redundancy

- **Keys are the unit of placement, and Slicer never sees state.** "Keys are an atomic unit of work placement: all state associated with a single key will be collocated on those task replicas to which the key is assigned, but different keys may be assigned to different tasks. Slicer does not observe application state; it merely notifies the task of the keys the task should serve." [SLI §2.1, p. 741]
- **Hashed range space.** "Slicer hashes each application key into a 63-bit slice key; each slice in an assignment is a range in this hashed keyspace." [SLI §2.1, p. 741]
  - Manipulating ranges makes "Slicer's workload independent of whether an application has ten keys or a billion".
  - Applications create keys "without Slicer on the critical path", so "there is no limit on the number of keys nor must they be enumerated" [p. 741].
- **Why hash.** "Hashing keys simplifies the load balancing algorithm because clusters of hot keys in the application's keyspace are likely uniformly distributed in the hashed keyspace." [SLI §2.1, p. 741]
- **The cost: locality.** "The cost is lost locality: contiguous application keys are scattered." Many Google applications already use single-key operations rather than scans [SLI §2.1, p. 741].
  - Mitigations for applications that must scan their store: "prefixing the primary key with the hashed slice key or by adding a secondary index" [SLI §2.2, p. 742].
  - "In future work, Slicer will support unhashed application-defined keys and implement range sharding to preserve locality among adjacent application-defined keys." [SLI §2.2, p. 742]
- **Two consistency regimes, chosen per application:**
  - **Exclusive.** Some applications "require all requests for the same key to be served by the same task, for example, to maintain a write-through cache"; Slicer offers "a consistency guarantee on what assignments a Slicelet can observe (§4.5)".
  - **Overlapping.** For others, "weaker semantics are correct even when requests for the same key are served by different tasks". Three kinds are named: read-only data (Google Fonts), weak consistency to users (Cloud DNS), and a strongly consistent underlying store (event aggregation) [SLI §2.1, p. 741].
- **Asymmetric key redundancy.** Applications with overlapping semantics "can configure Slicer with key redundancy, allowing assignment of each slice to multiple tasks. Slicer honors a minimum redundancy to protect availability and automatically increases replication for hot slices, which we call asymmetric key redundancy." [SLI §2.1, p. 741]
- **Load metric.** "By default, Slicer load balances on request rate (req/s)." The Slicelet measures per-slice request rate through the RPC system. "An extension to the API of Figure 3 lets tasks report a custom load metric." [SLI §2.2, p. 742]

#### 7.1.3 API: Slicelet (server side) and Clerk (client side)

- **Components.** There are three: "a centralized Slicer Service; the Clerk, a library linked into application clients; and the Slicelet, a library linked into application server tasks." [SLI §2, p. 741]
  - "The Slicer Service generates an assignment mapping key ranges ("slices") to tasks and distributes it to the Clerks and Slicelets, together called the subscribers."
  - "Application code interacts only indirectly with the Slicer Service via the Clerk and Slicelet libraries." [p. 741]
- **Slicelet API** [SLI §2.2, Fig. 3, p. 741]:
  - `isAffinitizedKey(key)`, used by "a few affinity-mode applications ... to discover misrouted requests";
  - `getSliceKeyHandle(key)` and `isAssignedContinuously(handle)`;
  - a listener `onChangedSlices(assigned, unassigned)`, "so it can prefetch and garbage-collect state".
- **Exclusive-ownership usage pattern** (the API is "inspired by Centrifuge"). "The task calls getSliceKeyHandle when a request arrives, and passes the handle back to isAssignedContinuously before externalizing the result. Note that checking assignment at beginning and end is insufficient, since the slice may have been unassigned and reassigned in the meantime." A handle may be cached across requests, for example "to cache a user's inbox during a session" [SLI §2.2, pp. 741-742].
- **Clerk API.** One function: `Set<Addr> getAssignedTasks(String key)` [SLI §2.3, Fig. 4, p. 742].
  - Most applications use transparent integration instead. Stubby accepts "an additional slice key argument with each RPC", and the GFE HTTP proxy can treat "any such feature" of a request (URL, parameters, cookies, headers) "as a slice key".
  - With global load balancing, "the global load balancer picks a datacenter, and Slicer picks the task from the job in that datacenter." [SLI §2.3, p. 742]

#### 7.1.4 Architecture: Assigner, Distributors, Backup Distributor, assignment store

- **The Assigner** "collects health, task provisioning, and load signals. It uses its central view of those signals to produce a coherent assignment of work to tasks (§4.4) that is strongly consistent for applications that need it (§4.5)." [SLI §4, p. 743]
- **Logically central, physically distributed.** "Though the Slicer Service is conceptually centralized (Figure 2), the implementation is highly distributed (Figure 5)." [SLI §4, p. 743]
  - Assigners run "in several Google datacenters around the world. Any Assigner may generate an assignment for any job in any datacenter." [SLI §4.1, p. 743]
- **Convergence through versioned, conditional writes.** "To facilitate convergence, Assigners write decisions into optimistically-consistent storage. An Assigner reads the stored assignment, generates a new assignment, and assigns it a monotonic generation number. It writes the new assignment back to storage transactionally conditioned on overwriting the previously read value. If a concurrent write has occurred, the transaction fails, the Assigner abandons its new assignment, retrieves the new current assignment, and tries again." [SLI §4.1, pp. 743-744]
- **Preferred Assigner.** "in the steady state only a single preferred Assigner generates an assignment for a particular job".
  - Preference means "network-closest", found by polling the global load balancer. "This definition is eventually consistent: there may be brief periods when multiple Assigners are preferred."
  - "Assignment storage makes the distributed Assigners act as a single logical process. When failure causes a change in preferred Assigner, the new one learns the decisions of the prior one and carries them forward. Should two Assigners both believe they are preferred, they will thrash, but storage concurrency control prevents divergence." [SLI §4.1, p. 744]
  - The stored prior assignment is also an **input** to the algorithm. Figure 5: the Assigner makes an assignment "informed by a stored prior assignment to minimize churn" [SLI Fig. 5 caption, p. 744].
- **Distribution tree.** Distribution "becomes a computational and network bottleneck" at scale, so Slicer uses "a two-tier distribution tree: an Assigner generates and distributes an assignment to a tier of Distributors, which distribute it to the subscribers. Nothing in our model precludes adding an additional tier to the tree." [SLI §4.2, p. 744]
  - "Distribution is a pull model". A subscriber asks a Distributor, which asks the Assigner on a miss.
  - Each Clerk and Slicelet "maintains a long-lived stream with the Distributor service", routed to the closest instance.
  - "Assignment distribution is asynchronous. Affinity applications can tolerate temporary inconsistency, and consistent applications ensure consistency via a separate control channel (§4.5)." [SLI §4.2, p. 744]
- **A rejected alternative.** Peer-to-peer distribution through the subscriber library was rejected. The Slicer team provisions its own distribution resources, "but the benefit is to minimize logic linked into customer binaries". The logic that identifies the preferred Assigner also lives in the Distributor tier, not in subscriber libraries [SLI §4.2, p. 744].

#### 7.1.5 Fault tolerance: the data plane keeps flowing without the control plane

This subsection is the peer-reviewed evidence for what the AWS guidance calls static stability, applied to a sharding control plane.

- **Principle.** "Slicer's control-plane separation ensures that most failures merely hinder timely re-optimization of the assignment, yet requests continue to flow." [SLI §4.3, p. 744]
- **Backup Distributor.**
  - The risk it covers: Distributors "share a nontrivial code base and thus risk a correlated failure due to a code or configuration error. We have yet to experience such a correlated failure, but our paranoia and institutional wisdom motivated us to guard against it."
  - The Backup Distributor "satisfies application requests simply by reading the assignment from the store (§4.1)". It is "simple, slowly evolving, and mostly independent of the Distributor and Assigner code base." [SLI §4.3, pp. 744-745]
- **Degraded mode.** "If the Backup Distributor is the only one operating, the system degrades to static sharding based on slightly stale load and health information." The mode requires only:
  1. library code linked into application binaries;
  2. the Backup Distributor service;
  3. "a valid assignment in persistent storage".

  "Because it does not react to load shifts or server task failure, degraded mode is intended as a stopgap until an oncall engineer restores the Assigner and Distributor network." [SLI §4.3, p. 745]
- **Geography.**
  - Any subscriber can reach any Distributor, and any Distributor can reach any Assigner [SLI §4.3, p. 745].
  - The preferred Assigner is the network-closest one, and "a Distributor runs wherever there is an Assigner".
  - "If customers demanded it, Slicer Service could run in every customer cell, eliminating all cross-datacenter dependency." [p. 745]
- **Fate-shared storage placement.** Assignments can be stored in the job's own datacenter with a local Assigner, so the job "can tolerate a network partition of the datacenter". However, "no production customers are configured this way" [SLI §4.3, p. 745].
- **Service-independent mode.** "Ultimately, even if every component of the Slicer Service fails, requests continue to flow using the most recent assignment cached in applicaion [sic] libraries. This mode has the same limitations as the Backup Distributor mode, plus new or restarted application client tasks are unable to initialize." [SLI §4.3, p. 745]
- **Measured Assigner outage** [SLI §5.2.2, p. 750]:
  - Setup: power-law skewed load on twenty tasks, then the Assigner was killed for 2 hours, "causing clients and server tasks to continue using the last-generated assignment".
  - During the outage, load balance degraded "since the assignment stagnated while the load changed". It was still "better than uniform sharding (not shown in Figure 12) on the same workload".
  - After restoration, "the Assigner rebalanced load".
  - With two Assigners, when the active one was killed, "the other became initialized 17.1 s later (σ = 2.7 s)", which is the preferred-Assigner polling period.

#### 7.1.6 Load balancing: objective, the weighted-move algorithm, split/merge, suppression

- **Objective.**
  - "The ultimate goal of load balancing is to minimize peak load; this enables a service to be provisioned with fewer resources."
  - Balance is kept because future surges are unknown: "Maintaining the system in a balanced state maximizes the buffer between current load and capacity for each task" [SLI §4.4, p. 745].
- **Initial state.** The initial assignment "divides the keyspace equally among available tasks, assuming that key load is uniform (key distribution is uniform due to hashing)." [SLI §4.4, p. 745]
- **The metric.**
  - Load imbalance is "the ratio of the maximum task load to the mean task load. In a perfectly balanced job where each task is handling the same load, the imbalance is 1."
  - The worst case Slicer can cause is n/r (r = minimum key redundancy, n = task count). For example, with n = 10 and r = 2 the worst decision gives imbalance 5 [SLI §4.4, p. 745].
- **Levers and costs.**
  - Imbalance is reduced "by adding or removing redundant tasks for a key or by reassigning keys from one task to another".
  - Slicer must respect configured minimum and maximum tasks per key and "limit key churn, the fraction of the key space affected by reassignment. Key churn itself creates load and increases overhead." [SLI §4.4, p. 745]
- **Split and merge keep the representation compact.**
  - Slicer "must split a hot slice—replace a key range [a, c) with two ranges [a, b), [b, c)".
  - "To prevent unbounded assignment size growth", it merges by "assigning adjacent cool slices to the same tasks, then merging the slice representations into a single range." [SLI §4.4, p. 745]
- **Out of scope.** "At Google, independent mechanisms (sometimes humans) decide when to add or remove tasks from a job ... Thus Slicer focuses exclusively on redistributing imbalanced load among available tasks, not on reprovisioning resources for sustained load changes." [SLI §4.4, p. 745]
- **The weighted-move algorithm, in full** [SLI §4.4.1, p. 746]. When resharding is needed, whether from load changes or task-set changes, it runs these phases:
  1. "Reassign keys away from tasks that are no longer part of the job (e.g., due to hardware failure)."
  2. "Increase/decrease key redundancy as required to conform to configured constraints".
  3. "Merge adjacent cold slices, moving one onto the same task as the other, to defragment the assignment." This proceeds as long as:
     - (a) "there are more than 50 slices per task in aggregate";
     - (b) "merging two slices creates a slice with less than mean slice load";
     - (c) "merging two slices does not drive the receiving task's load above the maximum task load";
     - (d) "no more than 1% of the keyspace has moved."
  4. "the sharding algorithm picks a sequence of moves with the highest weight, which we define as the reduction in load imbalance for the tasks affected by the move (benefit) divided by the key churn (cost). Moves are applied to the assignment in descending weight order until a key churn budget (9% of the keyspace) is exhausted."
  5. "Split hot slices without changing their task assignments. Splitting captures finer-grained load measurements and opens new move options in the next round." This proceeds as long as:
     - (a) "the split slice is at least twice as hot as the mean slice";
     - (b) "there are fewer than 150 slices per task in aggregate."
- **Phase-4 inner loop** [SLI §4.4.1, p. 746]:
  - "only moves affecting the hottest task can reduce load imbalance". For each slice on the hottest task, three moves are considered:
    - "reassigning the slice to the coldest task to displace the load";
    - "redundantly assigning the slice to the coldest task to spread the load";
    - "removing the slice which offsets the load to existing assignees."
  - Moves that violate the job's redundancy configuration are disqualified. "The algorithm greedily makes the best move and repeats until the key churn (cost) budget is exhausted."
- **How the constants were chosen.** "The constants in the algorithm (50–150 slices per task, 1% and 9% key movement per adjustment) were chosen by observing existing applications. Experience suggests the system is not very sensitive to these values, but we have not measured sensitivity rigorously." [SLI §4.4.1, p. 746] **Consequence for mantle:** these constants are cited starting points, not measurements.
- **Rebalancing suppression.** "When balancing CPU and the maximum task load is less than 25% (an arbitrary threshold), Slicer suppresses rebalancing: Because no task is at risk of overload, churn is waste." [SLI §4.4.2, p. 746]
- **Stated limitations** [SLI §4.4.3, p. 746]:
  - Request-rate balancing "ignores task heterogeneity"; CPU balancing adjusts for it.
  - Memory is not modeled: many cold, memory-heavy keys can exhaust a task's memory "despite manageable CPU load".
- **Reaction time.** In a synthetic shift test, the median time to restore max/mean < 1.2 was 480 s. This is "a function of the 1 m delay from the Google monitoring system and Slicer's 5 m load observation window", and one window is usually not enough [SLI §5.2.3, p. 750]. Figure 13's annotation gives 99% < 719 s.

#### 7.1.7 Why load-aware consistent hashing was abandoned

This is peer-reviewed evidence against computed placement, in addition to Tectonic's (note 01 §1.10, C2).

- A variant of consistent hashing "with load balancing support yielded both unsatisfactory load balancing and large, fragmented assignments". Some applications had "too few slice keys (tens to hundreds per task) for consistent hashing to result in good statistical load balancing." [SLI §4.4.4, p. 746]
- **Assignment size.** Consistent hashing "works best with many (1000) virtual nodes per physical task but introduces a significant cost distributing decoded assignments". Slicer distributes decoded assignments because "evolving clients is burdensome" [p. 746].
- **No directed moves.** "consistent hashing gives us less control over hot spots. We can cool off a task by reducing its virtual node count, but the displaced traffic ends up randomly distributed, not directed at a cool task, giving a poor tradeoff between key movement and balance improvement." [p. 746]
- **Statelessness does not survive load balancing.** "We were originally drawn to the statelessness of consistent hashing ... In practice, once the Assigner begins balancing load, creating a profitable reassignment requires knowledge of the previous assignment, and thus it is important that a recovering Assigner have access to prior state." [SLI §4.4.4, pp. 746-747]
- **Replacement.** "After 18 months in service, we replaced it with the weighted-move algorithm, which balances better with less key churn (§5.2.1)." [SLI §4.4.4, p. 747]
- **Production measurement.** On a key/value cache, "Under consistent hashing, the hottest task was 50% hotter than the mean". The weighted-move rollout improved balance [SLI §5.1.2, p. 748]. The conclusion claims "an order of magnitude less key churn" than load-aware consistent hashing [SLI §7, p. 752].

#### 7.1.8 Strong consistency: job, guard and bridge leases

- **The guarantee.** It "defines an authoritative assignment for every moment and guarantees that no task ever believes a key is assigned to it if that assignment does not agree. By configuring the job for at most one replica of each key, at no time will two Slicelets believe they are both assigned the same key." [SLI §4.5, p. 747]
- **Deployment status: not deployed.** "The consistency feature is implemented, but it is not yet deployed by customers in production." [SLI §4.5, p. 747] Every production number in §7.1.9 is therefore from **affinity (eventually consistent) mode**.
- **Why not one lease per key.** "The simplest way to provide strong consistency guarantees for keys would be to allocate a lease for each key from a central lease manager. We opted against this model, because it would require provisioning lease manager resources in proportion to the number of keys, and hundreds of millions of keys per sharded job are common." Existing lease managers such as Chubby "do not scale to that level" [SLI §4.5, p. 747].
- **Design: three Chubby locks per job.**
  - "Slicer builds on Chubby to provide a scalable lease-per-key abstraction using only three Chubby locks per job. The scheme ensures that only the keys being reassigned are unavailable during an assignment change."
  - "even if Slicer Service is down, RPCs continue to flow with strong consistency, since lease granting and maintenance is performed by the highly-available and battle-tested Chubby. The Assigner is only required for resharding." [SLI §4.5, p. 747]
- **Protocol** [SLI §4.5, p. 747]:
  1. **Job lease.** "an Assigner acquires the exclusive job lease to ensure that exactly one Assigner performs the work for writing". If it crashes mid-change, another Assigner acquires the job lease and resumes. "Only Assigners interact with the job lease."
  2. **Guard lease.** "the Assigner distributes assignments in the usual way, then writes the assignment generation number as the value of the guard lease. A consistent Slicelet may only use an assignment once it acquires the guard lease for reading."
  3. **Recall.** "Changing the assignment entails recalling the guard lease from Slicelet readers so the Assigner can rewrite its value." Recall "often means waiting out the expiration period for any task that may have died while holding its lease. This recall period entails complete application unavailability."
  4. **Bridge lease.** "when an assignment A1 is replaced by A2, there is no reason to make unavailable the unchanged slices, those that have identical assignments in A1 ∩ A2." The Assigner "writes and distributes assignment A2, creates the bridge lease, delays for Slicelets to acquire the bridge lease for reading, and only then does it recall and rewrite the guard lease. A Slicelet is allowed to use the intersection if it holds the bridge lease."
  5. **Clerks need no lease**, "since the only harm of a transient inconsistent assignment at the Clerk is a misrouted request bounced back for retry."
- **Cost of recall.** On a synthetic benchmark, the median lease recall period was 2.6 s and the 99th percentile 4.1 s, "implying that absent a bridge lease an entire application would suffer seconds of unavailability whenever an assignment changes." [SLI §4.5, p. 747]
- **Generalization.** "Nothing about the consistent-assignment mechanism limits it to the simple consistency propery [sic] of at most one Slicelet per key; the Assigner could easily enforce an at-most-three policy." Plural replicas "would require the application to consistently coordinate those replicas, perhaps with state machine replication." [SLI §4.5, p. 747]
- **Benchmark** [SLI §5.2.5, p. 751]:
  - Workload: 25 clients, 43 Kreq/s, 50 server tasks.
  - "Over three days, 99.85% of requests were satisfied; absent bridging, only 99.19% of requests would have been satisfied."
  - Call costs: `getSliceKeyHandle` 153 µs, `isAssignedContinuously` 94 µs.

#### 7.1.9 Evaluation (production unless stated)

- **Routing availability, client side** [SLI §5.1.1, p. 748]:
  - "Over a one-week period, Slicer performed 260 billion task selections for a subset of its Stubby clients, of which 99.98% succeeded."
  - The authors call this a pessimistic measure, because some failures may have been all-tasks-unhealthy cases.
- **Routing availability, server side.** "272 billion requests arrive at server tasks, of which only 11.6 million (0.004%) had been misrouted." [SLI §5.1.1, p. 748]
- **Availability of the Slicer service itself.** A monitoring probe asks each Distributor for an assignment: "99.75% of 329,978 requests succeeded". This underestimates availability because the probe forces computation of a new assignment, "whereas the common path returns a cached one" [SLI §5.1.1, p. 748].
- **Load balance.**
  - "Peak loads varied between 1.3× – 2.8× the mean load" for production jobs sampled over 5-minute windows [SLI §5.1.2, p. 748].
  - The Figure 6 caption states that tasks "rarely experience load 5% greater than the mean task load". **DERIVED reading:** the caption describes typical windows, and the prose range describes the peaks.
- **Churn.**
  - "The median hour in every job sees less than 20% of the keyspace move" [Fig. 7 caption, p. 748].
  - Cloud DNS "moves up to 40% of its keys per hour"; Flywheel "moves only 16% of its keys" [SLI §5.1.2, p. 748].
  - Churn is reported as a fraction of keyspace, "not bytes of objects actually unloaded and reloaded because, by design, Slicer does not know which keys in the key space actually exist" [p. 748].
- **Against a static model.** The model uses customer load estimates or a uniform split, with 100 slices per task. "Service operators provision for peak loads; Slicer provides a median reduction of 63% and as much as 99.3% for the most skewed job." For underloaded jobs, Slicer defers balancing, and they are elided [SLI §5.1.2, p. 749].
- **Aggregate scale** [SLI §5.1.3 table, p. 749]:

  | Metric | Total | Mean per service | Mean per job |
  |---|---|---|---|
  | Services | 22 | | |
  | Jobs | 263 | | |
  | Tasks (Slicelets) | 11,387 | 517 | 43 |
  | Clerks | 113,338 | 5,151 | 430 |
  | Requests/s | 6M | 266K | 22K |
  | Assignments/hour | 662 | 30 | 2.5 |
  | Assignment traffic (MBps) | 180 | 8.2 | 0.37 |
  | Key churn/hour | 4% | | |

- **Control-plane cost.** The service ran six Assigners of three cores each. The median one-minute sample used 0.13 core and the 99th percentile 2.34 cores. The whole service (Assigners, Distributors, Backup Distributors) "uses 0.3% of the CPU and 0.2% of the RAM used by the sliced services and their clients" [SLI §5.1.3, p. 749].
- **Propagation.**
  - "Assignments generally arrive within the second" [SLI §5.1.4, p. 749].
  - Figure 10 annotations: 95% < 1.7 s, 99% < 5.9 s, 99.9% < 9.0 s. The caption reads "95% of assignments reach subscribers within 2 s".
- **Computation.** "the 64th percentile is 17ms and the maximum a few seconds" [SLI §5.1.5, p. 749].
- **Algorithm comparison** (replayed production traces of Client Push, Cloud DNS and Flywheel) [SLI §5.2.1, pp. 749-750]:
  - "weighted-move with redundancy – significantly outperforms both other algorithms on load imbalance, with reduced key churn relative to consistent hashing (though not static sharding, which being static has no key churn)".
  - "Asymmetric replication provides significant load balancing benefits, though with a small increase in key churn".
- **Central authority vs cached assignment** [SLI §5.2.4, p. 751]:
  - The baseline routes every request through an authority: clients ask it first, and servers confirm with it. It "saturates its CPU at 5 Kreq/s, but Slicer scales smoothly since every component's workload is independent of aggregate client request rate."
  - The authors note the simple experiment "lacks admission control" [Fig. 14 caption, p. 751].

#### 7.1.10 Positioning, as the paper states it

- **Against Centrifuge** [SLI §6, p. 751]. Centrifuge is the most similar system: a central manager, ranges of a hashed keyspace, leases. Slicer claims four differences:
  1. **Availability.** "If the Centrifuge manager is unavailable, all leases expire and no RPCs flow. Slicer's control-plane separation ensures that assignments remain valid and RPCs flow even if the entire Slicer service fails."
  2. **Scale.** Distribution is separated from generation, which "enables much higher scales". Slicer states that an Assigner can serve 10^4 Distributors and a Distributor 10^4 subscribers. The text layer prints "104" (with the typo "servie"); reading it as 10^4, a flattened superscript, is DERIVED.
  3. **Multi-cluster.** Centrifuge is single-cluster.
  4. **Load balancing.** Slicer achieves "better balance while moving an order-of-magnitude fewer keys".
- **Against Orleans and Ringpop.** "Slicer uses a centralized algorithm rather than a client-based consistent hashing, which allows it to provide better load balancing and to offer consistency guarantees, which those systems cannot." [SLI §6, p. 751]
- **Against storage-embedded sharders** (Bigtable, HBase, Spanner). They "are not usable outside of the storage system", and "Bigtable requires at most one task per tablet to enforce consistency, whereas Slicer is free to add redundant copies of keys if permitted by the application" [SLI §6, p. 751].
- **Against network load balancers.** They "implement static hashing for requests or sessions; when they react to load shifts, they do not maximize affinity." They give no prefetch or termination signals, no asymmetric redundancy and no assignment consistency [SLI §6, p. 752].

---

### 7.2 Centrifuge (Adya, Dunagan, Wolman, NSDI '10): leases on ranges from a central manager

**Why it is here.** Centrifuge is Slicer's predecessor and the lease-based alternative: the central manager's lease *is* the ownership. Its protocol details show what a correct lease-on-ranges scheme needs. Slicer's critique (§7.1.10) shows what it costs in availability.

#### 7.2.1 Architecture and manager-directed leasing

- **Components** [CEN §2, Fig. 1, p. 3]:
  - "Servers that want to send requests link in a Lookup library, while servers that want to receive leases and process requests link in an Owner library."
  - Both talk to "a logically centralized Manager service that is implemented using a replicated state machine."
- **Manager-directed leasing.** The Manager maps "the key space into variable-length ranges using consistent hashing with 64 virtual nodes per Owner library". It conveys each Owner's subset "using a lease protocol" [CEN §2, p. 3].
  - Clients never request leases; "they simply ask which leases they have been assigned" [CEN §1, p. 2].
  - "Centrifuge only grants exclusive leases" [CEN §2, p. 2].
- **Manager high availability.** The deployment is "three standby servers and five servers running Paxos". "Paxos is only used to implement a leader election protocol and a highly available state store. Most of the complicated program logic runs in the leader and can be non-deterministic." [CEN §2.1.1, p. 4]
- **Lease table.** Each leased range carries "a lease generation number". A joining Owner receives "new 60 second leases" once the Manager has recalled the affected ranges. A change log is truncated after 5 minutes [CEN §2.1.2, p. 4].
- **Not in production.** Adaptive load management (move a virtual node from any Owner more than 10% from the mean load) and state migration were "not yet present in the version running in production" [CEN §2.1.2, pp. 4-5].

#### 7.2.2 Lease safety

- **Renewal margin.** Owners renew every 15 s for 60 s leases, so "3 consecutive lease requests have to be lost before a lease will spuriously expire" [CEN §2.3, p. 5].
- **Grants are not renewals.** A restarted Owner refuses renewals of leases it held before the restart. The Manager then re-grants them under a new lease generation number, which guarantees "that Lookups will appropriately trigger loss notifications" [CEN §2.3, p. 5].
- **Full-state messages.** Every Manager message carries "the complete set of ranges where the Owner should now hold a lease", about 2 KB. The authors chose this for debuggability [CEN §2.3, pp. 5-6].
- **Clock rate, not synchronization.** "we assume clock rate synchronization, but not clock synchronization". The Manager's clock may advance at most 65 s while the Owner's advances 60 s, and the Owner's timer starts first. The technique is Liskov's [CEN §2.3.1, p. 6].
- **Message races.** Every lease message carries "two sequence numbers". A message that is out of step is dropped and resent after "a random backoff". A "session nonce" stops pre-crash messages from being acted on [CEN §2.3.2, pp. 6-7].

#### 7.2.3 What clients can rely on

- **Lookups cache the whole table.** It is about 200 KB (100 owners × 64 virtual nodes × 32 B). Lookups poll every 30 s by log sequence number and fall back to a snapshot when the log has been truncated [CEN §2.2-§2.2.1, p. 5].
- **Routing is a hint.** "The semantics of Lookup() are that it returns hints". The owner rejects a stale route and the caller retries after backoff [CEN §3.1, p. 7].
- **Owner pattern** [CEN §3.2, Figs. 8-10, pp. 7-8]:
  1. call `CheckLeaseNow` on arrival;
  2. compare any stored state with the lease number it was written under ("discard state that turns out to be stale");
  3. perform the operation;
  4. call `CheckLeaseContinuous` before returning.
- **Lease numbers as fencing tokens.** A downstream service processes a request only "if the included lease number is greater than or equal to any previously seen lease number". This is Chubby's "sequencers" pattern [CEN §3.2, p. 9].
- **Loss notifications.** Lookups raise a notification when a range's lease generation changes, unless the state "was cleanly migrated". Clients then republish the lost in-memory state [CEN §2.2, p. 5].
  - Replicating Owner state was rejected because "replication cannot handle widespread or correlated failures" [CEN §2.5, p. 7].

#### 7.2.4 Scale, production and the availability weakness

- **Scope.** It was designed for "a cluster of a thousand or fewer machines" [CEN §2.4, p. 7].
- **Production (Live Mesh)** [CEN §5, pp. 11-13]:
  - about 130 Owners and about 1,000 Lookups;
  - a per-Owner mean time to unplanned lease loss of 19.5 months;
  - 12 race-dropped messages in 1.5 months;
  - 0 failed lease checks in over 53 million calls.
- **The weakness.** An Owner crash leaves its key space "unavailable until the Owner's lease has expired" [CEN §5.1.1, p. 12]. If the manager is down, "all leases expire and no RPCs flow" [SLI §6, p. 751]. **DERIVED:** the control plane's liveness sits on the data plane's critical path, which is what Slicer and Shard Manager were built to avoid.

---

### 7.3 Shard Manager (Lee et al., SOSP '21): Meta's shard-management framework

**Why it is here.** Shard Manager (SM) is the Meta-side counterpart to Slicer. It manages ZippyDB, the metadata store under Tectonic (note 01 §5), so it is the closest peer-reviewed description of how shard placement, moves and maintenance work in the Tectonic ecosystem. Tectonic itself is **not** mentioned in the SM paper (a full-text search finds no occurrence).

#### 7.3.1 Problem and adoption findings

- **Two adoption barriers.** From an analysis of "hundreds of sharded applications at Facebook", the authors name:
  1. "lack of support for geo-distributed applications, which account for most of Facebook's applications";
  2. "inability to maintain application availability during planned events such as software upgrades, which happen ≈1000 times more frequently than unplanned failures." [SM Abstract, p. 553]
- **Planned events.** "Treating planned events as failures amplifies unavailability by ≈1000x as every failover causes a period of shard unavailability". Existing frameworks also "cannot perform shard migration (e.g., due to load balancing) without impacting in-flight client requests" [SM §1.1, p. 554].
- **Adoption and scale** [SM Abstract, p. 553; §1.2, p. 554]:
  - "hundreds of applications running on over one million machines, which account for about 54% of all sharded applications at Facebook";
  - "billions of requests per second, which is ≈100 times higher than the reported request rate of Slicer applications".
- **Named applications.** They include "a Paxos-based database" (the citation is the ZippyDB post), "a blob store" (the citation is the Haystack paper), a queue service, a time-series database and a pub/sub system [SM §1.2, p. 554].
- **Census** [SM §2.2.1, p. 555; §2.2.4-§2.3, p. 557]:
  - "static sharding is ≈3x more popular than consistent hashing, indicating that resharding is rare and the overhead of resharding is not prohibitive".
  - Custom-sharded mega-stores are "only 1% of sharded applications but 27% of server usage".
  - 55% of SM applications balance on shard count alone, while multi-metric balancing covers 65% of server usage.
  - "about 70% of SM applications choose to gracefully drain shards before a container restart". Most "drain their primaries but not their secondaries".

#### 7.3.2 Sharding abstraction, replication roles, persistence advice, ZippyDB

- **App keys, app-defined shards.** "Slicer chooses UUID-key and framework-sharding, whereas ASF and SM choose app-key and app-sharding." That supports more applications but makes placement and load balancing harder [SM §3.1, p. 558].
  - "Slicer's UUID-key approach destroys key locality ... without key locality it is impossible to support certain popular operations such as prefix scans in a key-value store."
  - Laser "processes nearly one billion queries per second at peak; 9% of those queries are prefix scans".
  - Framework-chosen splits would misalign externally built per-shard indices; Laser's daily MapReduce is the example [SM §3.1, pp. 558-559].
- **Roles.** A shard has "at most one primary replica plus an arbitrary number of secondary replicas" [SM §2.2.3, p. 556]. The strategies are:
  - **primary-only:** "SM guarantees that no two servers serve the same shard at the same time";
  - **secondary-only:** equal replicas;
  - **primary-secondary:** "one SM-elected primary replica plus one or more secondary replicas. The primary often handles writes."

  Primary-only is popular because "the unused capacity of the application's running containers serves as cold standbys" [SM §2.2.3, p. 556].
- **Persistence advice** [SM §2.4, pp. 557-558]:
  - "Our colleagues initially developed a Paxos library, hoping it would be used along with SM to build many applications. However, it eventually had only one use case, i.e., ZippyDB".
  - "most applications do not need strong consistency. Applications that do need strong consistency almost always prefer the simple solution of a primary replica accessing external databases."
  - Persistent state via consensus, the most complex of the five options, "is only used to build very few persistent stores such as ZippyDB". It "needs to handle complex issues such as continuous data-consistency auditing to guard against bit rot".
- **ZippyDB on SM** [SM §2.5, p. 558]. These are peer-reviewed facts that add to note 01 §5.1.
  - ZippyDB "is a Paxos-based geo-distributed database ... It was started on SM in 2013".
  - "Each ZippyDB shard has a primary serving as the Paxos leader and proposer, and multiple secondaries serving as acceptors and learners."
  - "Load balancing is based on multiple metrics, including CPU, storage, and shard count. To reduce hardware costs, most ZippyDB deployments use one primary plus only two secondaries per shard and rely on SM to carefully handle maintenance operations to avoid losing two replicas at the same time (§4)."
  - **DERIVED:** ZippyDB's primary, which is the Paxos leader, is SM-elected (primary-secondary strategy, SM §2.2.3). This fits the NON-PEER-REVIEWED ZippyDB blog's statement that leaders are assigned by ShardManager (note 01 §5.2).

#### 7.3.3 Architecture and programming model

- **Components** [SM §3.2, Fig. 10, p. 559]:
  - **Application servers** host shards and link the SM library.
  - **Clients** link a service router that "learns from the service discovery system about which application server is responsible for which shards and routes requests accordingly".
  - The **orchestrator** "monitors the health and resource consumption of shards", "invokes the allocator to generate a new shard-to-server assignment", and publishes it "via the service discovery system, which internally uses a multi-level data-distribution tree to fan out". It "makes direct RPC calls to application servers for load-information collection and shard-assignment notification".
  - Twine, the cluster manager, "informs SM's TaskController of upcoming hardware maintenance events, kernel updates, and container starts/stops/moves via the TaskControl protocol".
- **ZooKeeper's three roles** [SM §3.2, p. 559]:
  1. It "stores the orchestrator's persistent state".
  2. "during start-up, an application server reads its shard assignment from ZooKeeper, without dependency on the SM control plane".
  3. The orchestrator watches "SM-library-created ephemeral nodes" to detect server failures.
- **APIs** [SM §3.3, Fig. 11, p. 559]:
  - Servers implement `add_shard(shardID, role)`, `drop_shard(shardID)`, `change_role(shardID, current_role, new_role)`, `prepare_add_shard(shardID, current_owner, role)` and `prepare_drop_shard(shardID, new_owner, role)`.
  - Clients call `get_client(app_name, key)` and then issue the RPC.
- **Features** [SM §3.4, pp. 559-560]:
  - geo-distributed deployments;
  - lifecycle negotiation;
  - graceful migration "without dropping any request in flight";
  - regional placement preference;
  - replica spread "across fault domains at all levels, including regions, data centers, and racks";
  - multi-metric load balancing "across heterogeneous application servers";
  - "Shard scaling" of each shard's replica count;
  - automatic failover.

#### 7.3.4 Planned events: TaskController, drains and graceful primary migration

- **Negotiable events** (application upgrades, auto-scaler changes) [SM §4.1, p. 560]:
  - "SM never approves unsafe operations".
  - Twine periodically sends pending container operations. The TaskController "responds with a subset of approved operations that will not endanger the availability of any shard".
  - The policy has three parts. The last two are the caps:
    1. whether to drain;
    2. "a global cap on the number of allowed concurrent container operations";
    3. "a per-shard cap on the number of replicas that are allowed to be temporarily unavailable".
  - "These two caps account for the containers and shard replicas that are already unavailable due to ongoing unplanned outage such as hardware failures."
  - For geo-distributed applications, the caps are enforced "across all involved Twine instances". The TaskController can approve one region's restart and make the other wait "to avoid losing both the shard's replicas at the same time".
- **Non-negotiable events** (hardware maintenance, kernel upgrades) [SM §4.2, p. 560]:
  - Twine gives "an advanced notice of the start and end time" and the impact: "network unavailability, runtime state loss, full state loss, and full machine loss".
  - Example: for a short rack-switch network loss, SM may leave secondaries in place and "demote the primary replicas on those machines while promoting their corresponding secondary replicas on unaffected machines".
- **Graceful primary-replica migration** [SM §4.3, Fig. 12, p. 560]. The old primary P_old forwards to the new primary P_new until the migration finishes:
  1. `prepare_add_shard()`: P_new "processes a primary-related request (e.g., write) only if the request is forwarded from Pold". Clients still send to P_old.
  2. `prepare_drop_shard()`: "Pold starts to forward all primary-related requests to Pnew."
  3. `add_shard()`: P_new "now officially holds the primary role and can accept primary-related requests directly from application clients."
  4. "SM instructs the service discovery system to notify application clients to send future requests to Pnew."
  5. `drop_shard()`: "Pold keeps forwarding client requests to Pnew and drops its replica when no more requests arrive." "Throughout the migration process, no client request is dropped."
- **Why Slicer cannot do this** (SM's claim). The SM orchestrator uses direct RPCs to control ordering. "By contrast, Slicer's application servers do not directly communicate with Slicer's controller. Hence, Slicer cannot orchestrate live migration that requires precise ordering of operations performed by distributed application servers." [SM §4.3, p. 561]
- **Measured effect** [SM §8.2, p. 564]. Setup: a primary-only application with 10,000 shards on 60 servers, allowing 10% of containers to restart at once.
  - With SM, the success rate "stays at ≈100%".
  - "Without graceful primary-replica migration, the success rate drops to ≈ 98%."
  - With neither graceful migration nor graceful container-restart handling, "although the upgrade finishes earlier (800 seconds vs. 1,500 seconds), the success rate further drops below 90%."
  - In production, a messaging queue service at "billions of requests per second" does "a rolling upgrade every weekday", and "the "client error rate" curve hardly changes" [SM §8.2, p. 564].

#### 7.3.5 Placement and load balancing with a constraint solver

- **Priority order.** The allocator optimizes "availability, performance, and efficient use of hardware, in that order" [SM §5.1, p. 561].
- **Hard constraints** [SM §5.1, p. 561]:
  1. "System stability: Cap the number of concurrent shard moves per server and per application to limit churns that may threaten system stability. Similarly, cap the number of a shard's replicas that can be moved concurrently."
  2. "Server capacity: For each load-balancing (LB) metric (e.g., CPU), the aggregate consumption of all shards on a server should not exceed the server's capacity."
- **Soft goals, high to low priority** [SM §5.1, p. 561]:
  1. region preference;
  2. spread of replicas "across fault domains at all levels, including regions, data centers, and racks";
  3. planned maintenance (drain ahead of time);
  4. utilization threshold, "e.g., 90%";
  5. global load balancing, e.g. "no server's utilization should exceed the average utilization of all servers by 10%";
  6. regional load balancing;
  7. "Parallel shard failover: Evenly distribute shards on a failed server to multiple other servers for faster recovery."
- **Two modes** [SM §5.1, p. 561]:
  - The emergency mode is "triggered upon detecting unavailable shards". It "tries to place unavailable shards as quickly as possible while satisfying hard constraints, but may temporarily deteriorate soft goals".
  - The periodic mode "takes a longer time to optimize the placement of all shards, and must not deteriorate soft goals."
- **Heuristics to a solver.** The heuristics "became complex, brittle, and hard to extend". A rewrite to "yet another supposedly simpler heuristic" was abandoned halfway, and SM moved to the ReBalancer constraint solver [SM §5.2, p. 561].
  - A MIP backend "can solve an optimization problem with millions of assignment variables in tens of minutes, whereas SM needs to solve a problem with billions of variables in tens of seconds" [SM §5.2, p. 562].
  - The solver cut the allocator's code "to ≈20% of the hand-crafted heuristics". Replica spread "takes only 180 lines of production code" [SM §5.2, p. 562].
- **Scaling the solver** [SM §5.3, p. 562]:
  - partition large applications and solve partitions in parallel;
  - "Local Search" instead of MIP. Starting "from the current shard assignment", it moves shards from hot to cold servers, prioritizing the worst violations, until it "cannot find improvements or uses up a predetermined time and move budget";
  - detect equivalent shards;
  - O(log n) incremental evaluation over a variable tree;
  - two-way and n-way swaps;
  - sample move targets across server groups such as regions;
  - batch goals by priority, with longer timeouts for critical batches;
  - "evaluate large shards earlier", which "reduces the number of shard assignment changes".
- **Solver performance** [SM §8.4, p. 565]. The test uses a ZippyDB snapshot balanced on storage, CPU and shard count. "the largest shard's load is 20 times higher than that of the smallest shard", and storage capacity "varies by up to 20%".
  - From 75K shards on 1K servers to 375K shards on 5K servers, solve time grew "6.8x from 30 seconds to 205 seconds". Production P90 is ≈10 s and P99 ≈50 s.
  - Without the domain-knowledge optimization, the solver "cannot even finish in 300 seconds and the resulting solution requires 22% more shard moves".
  - The largest problem in Figure 21 "involves 1.9 billion variables" [SM §9, p. 566].
- **Production load balancing.** A ZippyDB deployment of 12K machines keeps "P99 CPU utilization under 80%". "LB is a continuous-optimization process as load constantly changes in production." [SM §8.4, p. 566]

#### 7.3.6 Control-plane scale-out (mini-SMs) and control-plane fault tolerance

- **The control plane is itself sharded.** "The SM control plane in Figure 10 is not scalable enough to manage millions of servers and billions of shards. We divide SM's control plane into multiple mini-SMs so that each mini-SM manages a subset of servers and shards ... In other words, we shard SM's control plane" [SM §6.1, p. 562].
- **Partitions.** Large applications are divided "into non-overlapping partitions, where each partition typically comprises thousands of servers and hundreds of thousands of shard replicas".
  - "The replicas of a shard are always placed on servers that belong to the same partition."
  - Because partitions are large, "the average load in different partitions does not diverge much. In rare occasions that they do diverge, we use tools to initiate server or shard migration across partitions. As an application grows organically, new partitions can be added to scale it out." [SM §6.1, pp. 562-563]
- **Global components** [SM §6.1, p. 563]:
  - a stateless frontend;
  - an application registry that assigns applications to application managers;
  - a partition registry that assigns partitions to mini-SMs;
  - a shard scaler that changes replica counts with load;
  - a read service.
- **Fault tolerance** [SM §6.2, p. 563]:
  - "Every component in Figure 14 has multiple replicas running in different regions. The frontend is stateless. Other components are stateful and use a primary-secondary setup."
  - Releases go out as "a staged rollout ... over multiple days to prevent a bug from bringing down the entire SM control plane instantaneously."
  - "Even if all SM control-plane components are down, application clients can continue to send requests to application servers, although new shard assignments would not be generated."
- **Composability** [SM §7, p. 563]. Custom-sharded mega-stores keep their own orchestrators but adopt SM components: the TaskControl protocol, service discovery, and a derived allocator called "Data Placer". "about 100" legacy applications adopted the generic shard TaskController alone.

#### 7.3.7 Evaluation numbers

- **Deployment sizes** [SM §8.1, p. 564]. The largest deployments use "≈19K servers and ≈2.6M shards", and "14% of the deployments use 1,000 or more servers".
- **Mini-SM fleet.**
  - Counts: "139 and 48 mini-SMs that manage regional and geo-distributed deployments".
  - The largest mini-SMs manage "≈50K servers and ≈1.3M shards".
  - Each runs "on an 18-core 64GB RAM machine, with P90 CPU utilization at ≈30% and P90 memory utilization at ≈38%".
- **Totals.** "nearly 100M shards hosted on over one million servers"; "billions of requests per second"; "millions of machine and network maintenance events per month" [SM §8.1, p. 564].
- **Geo experiment** [SM §8.3, p. 564]:
  - Setup: 1,000 shards with two replicas each across FRC, PRN and ODN (30 servers per region), of which 400 shards prefer FRC.
  - At 90 s the FRC servers fail, and requests and replicas move to PRN and ODN.
  - At 450 s, FRC recovers and "SM migrates one replica of each EC shard back to FRC".
- **AdEvents** moved from static regional deployments to SM geo-distributed deployments; SM "helped reduce their machine usage by 67%" [SM §2.5, p. 558].

#### 7.3.8 Side-by-side (facts from the three papers)

| | Centrifuge (NSDI '10) | Slicer (OSDI '16) | Shard Manager (SOSP '21) |
|---|---|---|---|
| Keyspace | Flat hashed namespace. Consistent hashing with 64 virtual nodes per Owner [CEN §2, p. 3] | 63-bit hashed slice keys, contiguous ranges [SLI §2.1, p. 741] | Application keys and application-defined shards [SM §3.1, p. 558] |
| Who sets shard boundaries | Manager, via consistent-hashing ranges [CEN §2] | Framework: split hot slices, merge cold ones [SLI §4.4] | Application [SM §3.1] |
| Exclusive ownership | 60 s manager-granted leases on ranges, 15 s renewals [CEN §2.1.2, §2.3] | Default affinity mode with overlapping assignments. The optional job/guard/bridge Chubby leases are "not yet deployed by customers in production" [SLI §4.5] | Primary-only: "no two servers serve the same shard at the same time" [SM §2.2.3]. The mechanism is not described (see §7.3.10) |
| When the control plane is down | "all leases expire and no RPCs flow" [SLI §6, about CEN] | Requests flow on cached assignments. New tasks need the Backup Distributor to initialize [SLI §4.3] | Clients keep sending. Servers read assignments from ZooKeeper at startup [SM §3.2, §6.2] |
| Load-balancing algorithm | Move virtual nodes when >10% from the mean. Not in production [CEN §2.1.2] | Greedy weighted-move: benefit/churn, 9% churn budget [SLI §4.4.1] | Local-search constraint solver: hard constraints plus prioritized soft goals, time and move budgets [SM §5] |
| Hot-key replication | None; exclusive leases only [CEN §2, §2.5] | Asymmetric key redundancy [SLI §2.1] | Shard scaling of replica counts [SM §3.4, §6.1] |
| Planned maintenance | Not addressed | Not addressed. SM says Slicer "cannot orchestrate live migration" [SM §4.3] | TaskController caps plus graceful primary migration [SM §4] |
| Control-plane scope | One cluster of ≤1000 machines [CEN §2.4] | One job in one datacenter at a time [SLI §4.1] | Partitions of thousands of servers, one mini-SM each [SM §6.1] |

---

#### 7.3.9 Implications for mantle (Slicer, Centrifuge, Shard Manager)

All bullets are **INFERENCE** for mantle unless marked otherwise. Each cites the facts it rests on.

1. **One logical assigner per cell, with durable, generation-numbered, compare-and-swapped assignments.**
   - Evidence:
     - Slicer makes several Assigners act "as a single logical process" through conditional writes of generation-numbered assignments. The previous assignment is an input to the next, which minimizes churn [SLI §4.1, Fig. 5].
     - Centrifuge confines Paxos to leader election plus a state store and keeps the complex logic in a non-deterministic leader [CEN §2.1.1].
     - SM's control-plane components are replicated primary-secondary, with persistent state in ZooKeeper [SM §3.2, §6.2].
   - For mantle:
     - The placement driver should be a leader-elected process whose only durable state is an assignment record with a monotonic generation. The record lives in a Raft-replicated system range (note 06 §A4.2-A4.4 describes the Spanner/TiDB placement-driver analogues).
     - Every decision should commit by CAS on the generation before it is acted on.
     - This keeps the allocator out of the deterministic Raft state machine, as Centrifuge does. That fits the sans-IO Raft core recommended in note 06 C.a.
2. **The data plane must never wait on the assigner (static stability), and restarts must not need it either.**
   - Evidence:
     - Slicer keeps serving on cached assignments when the whole service is down [SLI §4.3, §5.2.2].
     - But in Slicer's service-independent mode, "new or restarted application client tasks are unable to initialize" [SLI §4.3].
     - SM closes that gap: servers read their assignment from ZooKeeper at startup "without dependency on the SM control plane" [SM §3.2, §6.2].
     - Centrifuge shows the failure mode to avoid: manager down means all leases expire [SLI §6].
   - For mantle:
     - The range directory and the node address map must be readable by any gateway or storage node straight from the replicated meta ranges and from local caches, with no placement-driver involvement.
     - A placement-driver outage should freeze only rebalancing and repair *scheduling*.
     - Caveat: repair of lost redundancy is also control-plane work. Static stability protects *serving*, not durability over time, so the outage must stay bounded, as Slicer's "stopgap until an oncall engineer restores" wording implies [SLI §4.3].
3. **Ownership of mutable metadata comes from the Raft group's own lease, not from a central lease manager.**
   - Evidence:
     - Slicer rejected per-key central leases because cost scales with key count and "hundreds of millions of keys per sharded job are common".
     - Its three-lease Chubby design still pays "complete application unavailability" during guard-lease recall (median 2.6 s) unless bridged, and it was never deployed in production [SLI §4.5].
     - Centrifuge's leases tie serving to manager liveness [SLI §6] and need a clock-*rate* bound (65 s per 60 s) [CEN §2.3.1].
   - For mantle:
     - A metadata range's leaseholder is decided inside its Raft group: a leader lease, plus a lease sequence checked at apply time as in CockroachDB (note 06 §A4.3).
     - The placement driver only *requests* membership changes and leadership transfers through Raft; it never grants ownership.
     - This is the peer-reviewed reason not to copy Centrifuge's manager-granted leases.
     - **DERIVED:** ZippyDB, whose primary SM elects [SM §2.2.3, §2.5], sits closer to the Centrifuge/SM model of external leader assignment. mantle's in-band Raft election is a deliberate departure from Tectonic's stack. The NON-PEER-REVIEWED ZippyDB post lists in-band failure detection as future work (note 01 §5.2).
4. **Fence every ownership-dependent action with a monotonic token, and check it at the point of externalization.**
   - Evidence:
     - Centrifuge passes lease numbers to downstream services, which accept only numbers "greater than or equal to any previously seen lease number" (Chubby sequencers) [CEN §3.2].
     - Slicer and Centrifuge both check ownership *continuously up to* externalizing a result, because begin/end checks miss an unassign-reassign in between [SLI §2.2; CEN §3.2].
   - For mantle:
     - (a) Requests routed by the range directory carry the range generation or epoch, and a node rejects stale ones (TiDB's region epoch, note 06 §A4.4).
     - (b) Chunk writes carry the block's layout epoch. mantle's chunk records already store "epoch (layout generation of the block)" (docs/design/chunk-store.md §3.1).
       - Storage nodes should reject an append whose epoch is below the highest they have seen for that block.
       - The Block-layer commit should CAS on the epoch, in the same spirit as Tectonic's write token (note 01 §1.7).
     - (c) A leaseholder re-verifies its lease at apply and ack time, not only at request start.
5. **Make planned moves graceful, because planned events outnumber failures by about 1000×.**
   - Evidence:
     - SM measured ≈100% success with graceful handling, ≈98% without graceful primary migration, and <90% without either [SM §8.2].
     - Planned stops are "≈1000 times more frequent than unplanned failures" [SM §1.1].
   - For mantle, a leadership move should mirror SM's five-step protocol with Raft primitives:
     - the target catches up as a learner or follower;
     - Raft leadership transfer runs, and the lease moves under a new sequence number;
     - during the handoff window, the old leaseholder answers in-flight requests with a redirect that carries the new leaseholder hint, or forwards them. It never silently drops them;
     - the directory update is published;
     - the old replica is removed only after it goes idle.
   - Nodes due for maintenance are drained of leaderships first. SM applications usually drain primaries, not secondaries [SM §2.2.5].
6. **Gate operator and automation actions with a TaskController-style safety check.**
   - Evidence: SM approves container operations only within a global concurrency cap and a per-shard unavailable-replica cap, and counts replicas already down from unplanned failures [SM §4.1].
   - For mantle, a single *operation gate* in the placement driver approves restarts, decommissions and disk drains only if:
     - every Raft group keeps a quorum;
     - every erasure-coded block keeps at least its tolerated-loss margin.

     Both conditions count nodes and disks that are *already* failed.
   - This gate is what makes scaling a cell *down* safe.
   - On a laptop (one node, one replica of everything), every restart is what SM calls a non-negotiable event [SM §4.2]. The gate cannot keep data online there, so it only records the planned unavailability. It becomes useful once a cell has enough nodes to keep quorum through a restart.
7. **Use an explicit, centrally computed assignment, not consistent hashing or CRUSH.**
   - Evidence:
     - Slicer measured load-aware consistent hashing against weighted-move: its hottest task was 50% above the mean, it had more churn, and it produced fragmented assignments. Its statelessness also disappears once load balancing starts [SLI §4.4.4, §5.1.2, §5.2.1].
     - Tectonic independently chose explicit chunk→disk maps (note 01 §1.10).
     - SM's census shows static sharding ≈3× as common as consistent hashing [SM §2.2.1].
   - For mantle: store range→node and chunk→disk assignments explicitly. Compute them with a greedy or local-search optimizer that starts from the current assignment.
8. **Balance with a churn budget, measured in the unit that costs mantle money.**
   - Evidence:
     - Slicer ranks moves by imbalance reduction per unit of key churn and stops at a 9% per-round budget. Merges are capped at 1%. Splits need at least 2× mean slice load. Rebalancing is suppressed below 25% CPU [SLI §4.4.1-§4.4.2].
     - SM caps concurrent moves per server, per application and per shard, and gives local search a "time and move budget" [SM §5.1, §5.3].
   - For mantle:
     - A metadata-range move copies a snapshot, and a chunk move copies bytes, so the budget should be in **bytes moved per round**, with leadership transfers budgeted separately because they are nearly free.
     - The objective should be max/mean utilization per resource: disk bytes, disk time (Tectonic's accounting unit [TEC §6.2], note 01 §0 item 8), and metadata QPS.
     - Slicer's and SM's constants (9%, 1%, 2×, 50-150 slices per task, 25%, 90%, 10%) are cited starting values only. Slicer says it has "not measured sensitivity rigorously" [SLI §4.4.1], and SM's 90% and 10% are examples [SM §5.1]. Under CLAUDE.md rule 4, mantle must measure its own.
9. **Separate an emergency repair mode from periodic optimization.**
   - Evidence: SM's emergency mode restores unavailable shards fast under hard constraints and may temporarily worsen soft goals. The periodic mode must not worsen them [SM §5.1].
   - For mantle: this matches Tectonic's split between repair and rebalancer (note 01 §1.10). Durability repair gets its own higher-priority loop that never waits for a balance optimization.
10. **Handle read-hot, immutable data with Slicer-style asymmetric redundancy; never do this for mutable state.**
    - Evidence:
      - Slicer adds replicas for hot slices only when the application tolerates overlapping owners, for example read-only data [SLI §2.1]. The measured gain in balance came with only a small increase in churn [SLI §5.2.1].
      - SM's shard scaler changes per-shard replica counts with load [SM §6.1].
    - For mantle:
      - Sealed objects' chunks and sealed metadata (note 01 M6) may be given extra read replicas or cache copies without coordination.
      - The live Name-layer mapping may not. Hot mutable ranges are handled by split-for-heat and leader balancing instead.
11. **Keep key order where S3 needs it; hash only above that level.**
    - Evidence:
      - Slicer names the cost of hashing as "lost locality" and lists range sharding as future work [SLI §2.1-§2.2]. SM puts it more bluntly: Slicer's approach "destroys key locality" [SM §3.1].
      - SM keeps application keys because prefix scans are 9% of Laser's ~1B QPS [SM §3.1].
    - For mantle: `ListObjects` needs ordered scans within a bucket or prefix. That supports note 01 M3 and note 06 §A4.5: hash at most the directory or bucket component, and keep the object-name order inside it.
12. **Route on cached directory entries and treat them as hints.**
    - Evidence:
      - Centrifuge's Lookups cache the whole table (32 B per range), update it incrementally by LSN with a snapshot fallback, and treat answers as "hints" that the owner validates [CEN §2.2-§2.2.1, §3.1].
      - Slicer's misroute rate was 0.004%, and 95% of assignments arrived within 2 s [SLI §5.1.1, §5.1.4].
    - For mantle:
      - Gateways cache range descriptors with generations and learn of changes by pull or stream.
      - On a stale route, the node rejects the request with a redirect hint, and the gateway refreshes that entry (Bigtable's lazy invalidation, note 06 §A4.1).
      - **DERIVED sizing:** at Centrifuge's 32 B per entry, 1M ranges is about 32 MB of directory. Beyond that, gateways should cache a working set rather than the whole table.
13. **Shard the control plane the same way as the data (cells all the way up).**
    - Evidence:
      - SM splits its control plane into mini-SMs that own partitions of "thousands of servers and hundreds of thousands of shard replicas". A shard's replicas never span partitions, and cross-partition moves are rare and tool-driven [SM §6.1].
      - Slicer's scope is one job in one datacenter, and it "could run in every customer cell" [SLI §4.1, §4.3].
    - For mantle: a *storage cell* is a set of nodes plus the ranges and copysets placed on them, owned by exactly one placement-driver instance. Replicas and EC stripes never span cells. Capacity grows by adding cells, and inter-cell moves are explicit, rare operations.
14. **Keep internal services stateless or soft-state; keep the one consensus-backed store as the only home for durable state.**
    - Evidence: at Meta, the Paxos library found "only one use case, i.e., ZippyDB". Strongly consistent applications "almost always prefer ... a primary replica accessing external databases" [SM §2.4].
    - For mantle: the placement driver, garbage collectors, repair and rebalancer should keep their durable state in mantle's own metadata ranges. They should not embed their own consensus.
    - The laptop case falls out naturally: one cell, one replica per range, an operation gate with nothing to negotiate, and a static assignment. SM's census suggests static sharding is often enough [SM §2.2.1].
15. **Apply the Distributor lesson to mantle's own rollouts.**
    - Evidence:
      - Slicer built its Backup Distributor because a shared code base risks correlated failure, though it had "yet to experience such a correlated failure" [SLI §4.3].
      - SM rolls out control-plane releases in stages over multiple days [SM §6.2].
    - For mantle: keep the directory-read path, which gateways and nodes use at startup, small and slowly changing, and separate from the placement driver's code. Roll out the placement driver in stages.

#### 7.3.10 UNVERIFIED / not found (§7.1-§7.3)

- **Slicer's assignment store.** Described only as "optimistically-consistent storage". The system is not named [SLI §4.1]. **UNVERIFIED.**
- **Slicer's lease durations.** No Chubby lease durations are given, only measured recall times [SLI §4.5]. **UNVERIFIED.**
- **Slicer's consistent mode in production.** No production data exists: "not yet deployed by customers in production" [SLI §4.5].
- **Slicer's "10^4" figures** (Distributors per Assigner, subscribers per Distributor). They read "104" in the text layer, and the 10^4 reading is **DERIVED**.
- **Slicer's Event Pipeline 2 reduction.** The text layer reads "achieved a 45 reduction" [SLI §3.3, p. 743]. The multiplier or unit symbol is missing, so the magnitude is **UNVERIFIED**.
- **Slicer's Figure 11.** No numeric values are stated in prose, only the qualitative ranking [SLI §5.2.1]. We did not read numbers off the plot.
- **How SM enforces primary-only exclusivity.** The mechanism behind "SM guarantees that no two servers serve the same shard at the same time" (leases, fencing or ZooKeeper sessions) is **not described** in the paper [SM §2.2.3]. ZooKeeper ephemeral nodes are said to be used for failure detection [SM §3.2]. How exclusivity survives partitions is **UNVERIFIED**.
- **SM timings.** No propagation latency for the service-discovery tree and no duration for a graceful migration are given [SM §3.2, §4.3]. **UNVERIFIED.**
- **Tectonic and SM.** Whether Tectonic's own services (as opposed to ZippyDB) use SM is **not stated**; Tectonic is not mentioned in SM. Only this chain is **DERIVED**: ZippyDB is Tectonic's metadata store [TEC §3.3, note 01] and ZippyDB runs on SM [SM §2.5]. Haystack appears among SM applications only as a citation, "a blob store [8]" [SM §1.2]. The date and extent of its use are **UNVERIFIED**.
- **Centrifuge's load management and state migration** were implemented but not in the production version, so there is no production evidence for them [CEN §2.1.2].
- **Centrifuge's proceedings page numbers** do not exist in the USENIX PDF. Page tags are the PDF's own 1-16.

### 7.4 Windows Azure Storage (Calder et al., SOSP '11): a peer-reviewed cell design for an object store

**Why it is here.** WAS is the only peer-reviewed paper in this section's scope that describes a complete multi-tenant blob store built from cells ("storage stamps"). The stamps sit behind a thin global router (DNS plus a Location Service). Inside a stamp, range partitions split, merge and move automatically, and tenants migrate between stamps. It answers most of the owner's cell questions with published mechanisms, a decade before AWS said anything comparable about S3.

#### 7.4.1 The cell: a storage stamp, its size and its utilization targets

- **Shape.** "A storage stamp is a cluster of N racks of storage nodes, where each rack is built out as a separate fault domain with redundant networking and power. Clusters typically range from 10 to 20 racks with 18 disk-heavy storage nodes per rack. Our first generation storage stamps hold approximately 2PB of raw storage each. Our next generation stamps hold up to 30PB of raw storage each." [WAS §3.2, p. 144]. §8 gives "20-30PB" for the new ones [WAS §8, p. 155].
- **Scale at publication:** "70 petabytes of raw storage in production", with "a few hundred more petabytes" being provisioned for 2012 [WAS §3.2, p. 144].
- **Utilization policy.** "Our goal is to keep a storage stamp around 70% utilized in terms of capacity, transactions, and bandwidth. We try to avoid going above 80% because we want to keep 20% in reserve for (a) disk short stroking ... and (b) to continue providing storage capacity and availability in the presence of a rack failure within a stamp. When a storage stamp reaches 70% utilization, the location service migrates accounts to different stamps using inter-stamp replication" [WAS §3.2, p. 145].
- **Adding capacity** means adding cells. Each location (data center) "holds multiple storage stamps". To grow, "we deploy one or more storage stamps in the desired location's data center and add them to the LS". The LS "can then allocate new storage accounts to those new stamps ... as well as load balance (migrate) existing storage accounts from older stamps to the new stamps" [WAS §3.2, p. 145].
- **The control plane's memory bounds the cell.**
  - Intra-stamp replication at the stream layer "allows the amount of information that needs to be maintained to be scoped by the size of a single storage stamp. This focus allows all of the meta-state for intra-stamp replication to be cached in memory" [WAS §3.4, p. 146].
  - The layers "are co-designed so that they will not use more than 50 million extents and no more than 100,000 streams for a single storage stamp given our current stamp sizes. This parameterization can comfortably fit into 32GB of memory for the SM" [WAS §4.1, p. 147].
  - The RangePartition high watermark (about ten times the number of partition servers, §7.4.7) "was chosen based on how big we can allow the stream and extent metadata to grow for the SM, and still completely fit the metadata in memory for the SM" [WAS §5.5, p. 151].
- **A tenant must fit in one cell.**
  - "We currently limit the amount of storage for an account to be no more than 100TB. This constraint allows all of the storage account data to fit within a given storage stamp".
  - Customers who need more "use more than one account". The authors call this "a reasonable tradeoff" for large customers but note that it "does require large services to have account level partitioning logic", and they "plan to increase" the limit [WAS §8, pp. 155-156].
- **Compute lives in separate stamps.** "we separate computation and storage into their own stamps (clusters) within a data center since this separation allows each to scale independently and control their own load balancing" [WAS §6, p. 152; also §8 "Scaling Computation Separate from Storage", p. 153].

#### 7.4.2 The thin router: the account name in DNS, and the Location Service

- **Namespace.** Every object is addressed as `http(s)://AccountName.<service>.core.windows.net/PartitionName/ObjectName` [WAS §2, p. 144].
  - "The AccountName DNS translation is used to locate the primary storage cluster and data center where the data is stored. This primary location is where all requests go to reach the data for that account."
  - "In conjunction with the AccountName, the PartitionName locates the data once a request reaches the storage cluster." [WAS §2, p. 144]
- **Location Service (LS).** It "manages all the storage stamps. It is also responsible for managing the account namespace across all stamps. The LS allocates accounts to storage stamps and manages them across the storage stamps for disaster recovery and load balancing. The location service itself is distributed across two geographic locations for its own disaster recovery." [WAS §3.2, p. 145]
- **Account allocation, quoted.** The application specifies "the location affinity for the storage (e.g., US North). The LS then chooses a storage stamp within that location as the primary stamp for the account using heuristics based on the load information across all stamps (which considers the fullness of the stamps and other metrics such as network and transaction utilization). The LS then stores the account metadata information in the chosen storage stamp, which tells the stamp to start taking traffic for the assigned account. The LS then updates DNS to allow requests to now route from the name https://AccountName.service.core.windows.net/ to that storage stamp's virtual IP (VIP, an IP address the storage stamp exposes for external traffic)." [WAS §3.2, p. 145]
- **DERIVED:** the per-request router holds no state: it is DNS plus one VIP per stamp. The LS is a control plane, consulted at account creation, failover and migration, never per request. The order is: tell the cell first, then publish the route.

#### 7.4.3 Moving a tenant between cells: inter-stamp replication and failover

- **Two replication engines** [WAS §3.4, p. 145]:
  - Intra-stamp (stream layer) replication is synchronous and "on the critical path of the customer's write requests".
  - Inter-stamp (partition layer) replication is asynchronous and works "at the object level, where either the whole object is replicated or recent delta changes are replicated for a given account".
- **Uses:** "Inter-stamp replication is used for (a) keeping a copy of an account's data in two locations for disaster recovery and (b) migrating an account's data between stamps. Inter-stamp replication is configured for an account by the location service and performed by the partition layer." [WAS §3.4, p. 145]
- **Why the split.** Intra-stamp replication guards against frequent hardware failures and needs low latency. Inter-stamp replication guards against rare geo-disasters and optimizes WAN bandwidth "while achieving an acceptable level of replication delay" [WAS §3.4, pp. 145-146].
- **Lag:** "changes are geo-replicated and committed on the secondary stamp within 30 seconds on average after the update was committed on the primary stamp" [WAS §5.6, p. 152].
- **Migration is a clean failover.** "For disaster recovery, we may need to perform an abrupt failover where recent changes may be lost, but for migration we perform a clean failover so there is no data loss. In both failover scenarios, the Location Service makes an active secondary stamp for the account the new primary and switches DNS to point to the secondary stamp's VIP. Note that the URI used to access the object does not change after failover." [WAS §5.6, p. 152]
- **Not described:** the steps of a clean failover. The paper does not say how writes at the old primary are stopped and drained before DNS flips, or how clients holding cached DNS answers are handled. **UNVERIFIED.**

#### 7.4.4 Inside the cell: three layers

- **Stream layer** "stores the bits on disk", as a "distributed file system layer within a stamp" of append-only "streams" made of "extents" [WAS §3.3, p. 145].
- **Partition layer** provides the object namespace, ordering and strong consistency. Objects "are broken down into disjointed ranges based on the PartitionName values and served by different partition servers" [WAS §3.3, p. 145].
- **Front-ends** are "stateless servers". An FE authenticates the request and routes it "to a partition server in the partition layer (based on the PartitionName)". "The FE servers cache the Partition Map and use it to determine which partition server to forward each request to." [WAS §3.3, p. 145]
- **Co-location:** partition servers and stream servers run on every storage node [WAS §3.3, p. 145].

#### 7.4.5 Stream layer: extents, append-only writes, sealing (the cell's data plane)

- **Units.** A block, up to "N bytes (e.g. 4MB)", carries one checksum that is verified on every read [WAS §4, p. 146]. Extents are "the unit of replication", three replicas per stamp by default, with a target size of 1 GB [WAS §4, p. 146].
- **Immutability.** "Only the last extent in the stream can be appended to. All of the prior extents in the stream are immutable." [WAS §4, p. 146]
- **Stream Manager (SM).** It is "a standard Paxos cluster ... off the critical path of client requests". It creates and assigns extents, performs "lazy re-replication", garbage-collects unreferenced extents and schedules erasure coding [WAS §4.1, p. 146].
- **Placement.** New extents go to ENs chosen "to randomly spread the replicas across different fault and upgrade domains while considering extent node usage (for load balancing)" [WAS §4.3.1, p. 147].
- **No leases for the write primary.**
  - "The primary EN and the location of the three replicas never change for an extent while it is being appended to (while the extent is unsealed). Therefore, no leases are used to represent the primary EN for an extent" [WAS §4.3.1, p. 147].
  - The primary chooses offsets, orders concurrent appends, and acknowledges only "after a successful append has occurred to disk for all three extent nodes" [WAS §4.3.1, p. 147].
- **Failure handling: seal and move on.**
  - On a write failure the client contacts the SM, the extent "is sealed by the SM at its current commit length", and a new extent is allocated on available ENs.
  - "This process of sealing by the SM and allocating the new extent is done on average within 20ms. A key point here is that the client can continue appending to a stream as soon as the new extent has been allocated, and it does not rely on a specific node to become available again." [WAS §4.3.1, pp. 147-148]
- **Seal length.**
  - "When sealing the extent, the SM will choose the smallest commit length based on the available ENs it can talk to. This will not cause data loss since the primary EN will not return success unless all replicas have been written to disk for all three ENs."
  - ENs that become reachable later are forced to the chosen length, so that "all its available replicas ... are bitwise identical" [WAS §4.3.2, p. 148].
- **Retries create duplicates, handled a layer up.** "the client needs to expect the same block to be appended more than once". Sequence numbers or unreferenced-garbage collection absorb the duplicates [WAS §4.2, p. 147].
- **Partition load.** Loading a partition first runs a "check for commit length", which seals the last extent if its replicas disagree [WAS §4.3.3, p. 148].
- **Other data-plane mechanisms**, outside this section's scope: erasure coding of sealed extents at "1.3x – 1.5x" [WAS §4.4, p. 148], read deadlines [WAS §4.5, p. 148], spindle anti-starvation [WAS §4.6, p. 149], and a journal drive per node [WAS §4.7, p. 149]. The numbers are in the appendix.

#### 7.4.6 Partition layer: Object Tables, RangePartitions, PM, PS, Lock Service, Partition Map

- **Object Tables and RangePartitions.** Object Tables (OTs) "can grow to several petabytes" and are "dynamically broken up into RangePartitions (based on traffic load to the table)". A RangePartition is "a contiguous range of rows in an OT from a given low-key to a high-key". The RangePartitions of an OT "are non-overlapping, and every row is represented in some RangePartition" [WAS §5.1, p. 149].
- **System OTs per stamp:** the Account, Blob, Entity, Message, Schema and **Partition Map** tables [WAS §5.1, p. 149].
  - The Partition Map Table "keeps track of the current RangePartitions for all Object Tables and what partition server is serving each RangePartition. This table is used by the Front-End servers to route requests" [WAS §5.1, p. 149].
  - The Blob, Entity and Message tables are keyed by (AccountName, PartitionName, ObjectName) [WAS §5.1, p. 149]. For blobs, "the full blob name is the PartitionName" [WAS §2, p. 144]. **DERIVED:** the blob index is range-partitioned on (account, blob name).
- **Partition Manager (PM).** The PM splits OTs into RangePartitions, assigns each to a partition server (PS), and records the assignment in the Partition Map Table. It "ensures that each RangePartition is assigned to exactly one active partition server at any time, and that two RangePartitions do not overlap". Several PM instances "contend for a leader lock" in the Lock Service [WAS §5.2, p. 150].
- **Partition Server (PS).**
  - "The system guarantees that no two partition servers can serve the same RangePartition at the same time by using leases with the Lock Service." A PS "serves on average ten RangePartitions at any time" [WAS §5.2, p. 150].
  - When a PS fails, the PM reassigns its N RangePartitions to "N (or fewer) partition servers, based on the load on those servers" and updates the Partition Map Table. A new PS serves "for as long as the PS holds its partition server lease" [WAS §5.2, p. 150].
- **Lock Service:** "A Paxos Lock Service", used for PM leader election and PS leases. The details are deferred to Chubby [WAS §5.2, p. 150].
- **Where RangePartition state lives.** Each RangePartition is an LSM tree whose state lives in its own streams [WAS §5.3.1, p. 150]:
  - The **metadata stream** is the root: "The PM assigns a partition to a PS by providing the name of the RangePartition's metadata stream". The PS "also writes in the metadata stream the status of outstanding split and merge operations".
  - The other streams are the commit log stream, the row data stream and, for the Blob Table, the blob data stream.
  - "the underlying extents can be pointed to by multiple streams in different RangePartitions due to RangePartition splitting".
- **DERIVED:** all RangePartition state is in the replicated stream layer, so a PS is a stateless owner of a range. Moving a range moves ownership, not data.

#### 7.4.7 The three operations: load balance, split and merge (exact procedures)

- **Definitions** [WAS §5.5, p. 151]:
  - Load Balance "reassigns one or more RangePartitions to less loaded partition servers".
  - Split "splits the RangePartition into two or more smaller and disjoint RangePartitions, then load balances (reassigns) them across two or more partition servers".
  - Merge "merges together cold or lightly loaded RangePartitions that together form a contiguous key range within their OT. Merge is used to keep the number of RangePartitions within a bound proportional to the number of partition servers in a stamp."
- **Partition-count bounds.**
  - "WAS keeps the total number of partitions between a low watermark and a high watermark (typically around ten times the partition server count within a stamp). At equilibrium, the partition count will stay around the low watermark." Near the high watermark, "the system will increase the merge rate" [WAS §5.5, p. 151].
  - "a storage stamp has a few hundred partition servers". "Keeping many more RangePartitions than partition servers enables us to quickly distribute a failed PS or rack's load across many other PSs" [WAS §5.5, p. 151].
- **Rate:** "For each stamp, we typically see 75 splits and merges and 200 RangePartition load balances per day." [WAS §5.5, p. 151]
- **Load balance (a move)** [WAS §5.5.1, p. 151]:
  - Procedure: "the PM sends an offload command to the PS, which will have the RangePartition write a current checkpoint before offloading it. Once complete, the PS acks back to the PM that the offload is done. The PM then assigns the RangePartition to its new PS and updates the Partition Map Table to point to the new PS. The new PS loads and starts serving traffic for the RangePartition. The loading of the RangePartition on the new PS is very quick since the commit log is small due to the checkpoint prior to the offload."
  - Loading a partition means reading the metadata stream, locating the checkpoints and replaying the commit log.
- **Split** [WAS §5.5.2, p. 152]:
  - **Triggers:** "too much load as well as the size of its row or blob data streams".
  - **Who decides:** "The PM makes the decision to split, but the PS chooses the key (AccountName, PartitionName) where the partition will be split."
  - **Split key:** for size, the PS keeps "the split key values where the partition can be approximately halved in size". For load, it uses "Adaptive Range Profiling" to track "which key ranges in a RangePartition have the most load".
  - **Procedure, verbatim:**
    1. "The PM instructs the PS to split B into C and D."
    2. "The PS in charge of B checkpoints B, then stops serving traffic briefly during step 3 below."
    3. "The PS uses a special stream operation "MultiModify" to take each of B's streams (metadata, commit log and data) and creates new sets of streams for C and D respectively with the same extents in the same order as in B. This step is very fast, since a stream is just a list of pointers to extents. The PS then appends the new partition key ranges for C and D to their metadata streams."
    4. "The PS starts serving requests to the two new partitions C and D for their respective disjoint PartitionName ranges."
    5. "The PS notifies the PM of the split completion, and the PM updates the Partition Map Table and its metadata information accordingly. The PM then moves one of the split partitions to a different PS."
- **Merge** [WAS §5.5.3, p. 152]:
  - **Choice:** the PM picks "two RangePartitions C and D with adjacent PartitionName ranges that have low traffic".
  - **Procedure, verbatim:**
    1. "The PM moves C and D so that they are served by the same PS. The PM then tells the PS to merge (C,D) into E."
    2. "The PS performs a checkpoint for both C and D, and then briefly pauses traffic to C and D during step 3."
    3. "The PS uses the MultiModify stream command to create a new commit log and data streams for E. Each of these streams is the concatenation of all of the extents from their respective streams in C and D." C's extents come first, in order, then D's.
    4. "The PS constructs the metadata stream for E, which contains the names of the new commit log and data stream, the combined key range for E, and pointers (extent+offset) for the start and end of the commit log regions in E's commit log derived from C and D, as well as the root of the data index in E's data streams."
    5. "At this point, the new metadata stream for E can be correctly loaded, and the PS starts serving the newly merged RangePartition E."
    6. "The PM then updates the Partition Map Table and its metadata information to reflect the merge."
- **DERIVED observations:**
  - None of the three operations copies data. Split and merge rewrite pointer lists over immutable extents, and a move is a checkpoint plus a reassignment. Bigtable's split shares SSTables the same way (note 06 §A4.1).
  - **Split in place, publish, then move.** The split children stay on the parent's PS until the PM has updated the Partition Map, and only then is one of them moved. A front-end whose cached map still shows B therefore reaches the PS that now serves C and D. Merge mirrors this: co-locate first, merge in place, update the map last.
  - The paper does not give the pause length for split or merge ("briefly"). **UNVERIFIED.**
  - It does not say how an interrupted split or merge is completed or rolled back; it says only that the status is written to the metadata stream. **UNVERIFIED.**

#### 7.4.8 How the PM decides (from WAS §5.5.1 and §8)

- **Metrics.** For each RangePartition and each PS, the PM tracks "(a) transactions/second, (b) average pending transaction count, (c) throttling rate, (d) CPU usage, (e) network usage, (f) request latency, and (g) data size", carried on PM-PS heartbeats. A hot RangePartition is split. A hot PS with no single hot partition has partitions moved off [WAS §5.5.1, p. 151].
- **A single scalar did not work.** "We first tried the product of request latency and request rate ... it did not correctly capture high CPU utilization that can occur during scans or high network utilization. Therefore, we now take into consideration request, CPU, and network loads to guide load balancing. However, these metrics are not sufficient to correctly guide splitting decisions." [WAS §8, p. 154]
- **Split triggers** are separate "hints", for example "request throttling, request timeouts, the size of a partition" [WAS §8, p. 154].
- **Cadence.** "Every N seconds (currently 15 seconds) the PM sorts all RangePartitions based on each of the split triggers. ... the PM picks a small number to split for this quantum". A balance pass then sorts PSs by request, CPU and network load and preferentially moves a recently split partition from a heavy PS to a light one [WAS §8, p. 154].
- **The policy is replaceable at runtime.** "The core load balancing algorithm can be dynamically "swapped out" via configuration updates", and scripting can customize split triggers [WAS §8, p. 154].

#### 7.4.9 Throttling and isolation

- **Per-tenant tracking.** Each PS tracks request rates per AccountName and PartitionName with a Sample-Hold algorithm (Estan and Varghese, SIGCOMM 2002, WAS ref. [7]; the hyphen falls at a line break in the PDF) to keep "the request rate history of the top N busiest AccountNames and PartitionNames" [WAS §8, p. 154].
  - This decides whether an account is well-behaved, i.e. "whether the traffic backs off when it is throttled".
  - Under overload the PS will "selectively throttle the incoming traffic, targeting accounts that are causing the issue", using a per-account throttling probability.
- **Load that cannot be balanced** ("high traffic to a single PartitionName, high sequential access traffic, repetitive sequential scanning, etc.") is throttled: "the system throttles requests of such traffic patterns when they are too high" [WAS §8, p. 154].

#### 7.4.10 Design choices and lessons (WAS §8)

**All §8 headings, in order** [WAS §8, pp. 153-156]:

1. Scaling Computation Separate from Storage
2. Range Partitions vs. Hashing
3. Throttling/Isolation
4. Automatic Load Balancing
5. Separate Log Files per RangePartition
6. Journaling
7. Append-only System
8. End-to-end Checksums
9. Upgrades
10. Multiple Data Abstractions from a Single Stack
11. Use of System-defined Object Tables
12. Offering Storage in Buckets of 100TBs
13. CAP Theorem
14. High-performance Debug Logging
15. Pressure Point Testing

**The lessons that bear on cells and moves:**

- **Range Partitions vs. Hashing** [WAS §8, p. 154].
  - WAS chose range partitioning because "range-based partitioning makes performance isolation easier since a given account's objects are stored together within a set of RangePartitions (which also provides efficient object enumeration). Hash-based schemes have the simplicity of distributing the load across servers, but lose the locality of objects for isolation and efficient enumeration."
  - The cost is sequential keys, where "all of the writes go to the very last RangePartition in the customer's table". The stated remedy is on the customer's side: "a customer can always use hashing or bucketing for the PartitionName".
  - Contrast: Tectonic hash-partitioned its metadata specifically to avoid hotspots (note 01 §0 item 1, §1.5).
- **Automatic Load Balancing** [WAS §8, p. 154]: it was "crucial to have efficient automatic load balancing of partitions that can quickly adapt"; tuning the metrics and the algorithm took a long time.
- **Separate Log Files per RangePartition** [WAS §8, p. 155].
  - WAS chose this for performance isolation, unlike Bigtable's one log per server, so that loading a partition is limited "to just the recent object updates in that RangePartition".
  - **INFERENCE:** note 06 §C.c recommends one shared multi-group WAL per disk for mantle. Under Raft, a moved replica catches up by snapshot and log from the leader, so WAS's reason does not carry over directly.
- **Append-only System** [WAS §8, p. 155]: "Having an append-only system and sealing an extent upon failure have greatly simplified the replication protocol and handling of failure scenarios". The cost is garbage-collection I/O.
- **Upgrades** [WAS §8, p. 155].
  - Servers of every layer are spread evenly across fault domains and upgrade domains. Losing a fault domain removes "at most 1/X of the servers for a given layer", and an upgrade takes down "at most 1/Y" at a time (X and Y are the numbers of fault and upgrade domains).
  - "Before taking down an upgrade domain, the upgrade process notifies the PM to move the partitions out of that upgrade domain and notifies the SM to not allocate new extents in that upgrade domain. Furthermore, before taking down any servers, the upgrade process checks with the SM to ensure that there are sufficient extent replicas available for each extent outside the given upgrade domain."
  - Validation tests run before moving on to the next domain.
- **Offering Storage in Buckets of 100TBs**: the rule that a tenant fits in one cell (§7.4.1).
- **CAP Theorem** [WAS §8, p. 156]: WAS claims strong consistency and high availability "within a storage stamp" for the network partitions it sees there (node failures and top-of-rack switch failures). The claim rests on layering: the stream layer stays available by moving to other racks while the partition layer reassigns RangePartitions.
- **Pressure Point Testing** [WAS §8, p. 156]: a programmable interface triggers operations such as "checkpoint a RangePartition ..., split/merge/load balance RangePartitions, erasure code or unerasure code an extent, crash each type of server in a stamp, inject network latencies, inject disk latencies", in chosen orders or at random during stress runs.

---

### 7.5 Amazon Aurora (SIGMOD '17, '18): replacing replicas with quorum sets and epochs instead of consensus

**Why it is here.** Aurora's storage is a multi-tenant fleet in which every 10 GB piece of every volume is its own six-way protection group. Replicas move constantly for repair, heat management and upgrades, and none of it uses Paxos or Raft membership changes: epochs carried on every request do the job. mantle's data plane needs exactly this to move the chunk replicas of blocks that are still being written.

#### 7.5.1 Segments and protection groups

- **Layout.** "Segments are small, currently representing no more than 10GB of addressable data blocks in the database volume. Segments are replicated into protection groups, using V = 6, Vw = 4, and Vr = 3. These six copies are spread across three AZs, with two copies in each of the three AZs." [AUR18 §2.1, p. 790]
- **Why six copies.** Aurora tolerates "AZ+1" [AUR18 §1, p. 789; AUR17 §2.1, p. 1042]:
  - it can lose an entire AZ plus one more node without losing data;
  - it can lose an entire AZ without losing write availability.

  2/3 quorums were judged "inadequate", because an AZ failure that coincides with background failures breaks them [AUR17 §2.1, p. 1042].
- **Why small segments.** The goal is a short MTTR, not a long MTTF.
  - "We instead focus on reducing MTTR to shrink the window of vulnerability to a double fault. We do so by partitioning the database volume into small fixed size segments, currently 10GB in size." [AUR17 §2.2, pp. 1042-1043]
  - "A 10GB segment can be repaired in 10 seconds on a 10Gbps network link." [AUR17 §2.2, pp. 1042-1043]
  - AUR18 restates it: "Assuming a 10 second window to detect and repair a segment failure, it would require two independent segment failures as well as an AZ failure in the same 10 second period to lose the ability to repair a quorum." [AUR18 §2.1, p. 790]
- **Scale of the unit:** "a 64TB volume has 38,400 segments" [AUR18 §4, p. 794]. Segments "are placed with high entropy across the various storage nodes" [AUR17 §3.3, p. 1044].

#### 7.5.2 The invariant that makes consensus unnecessary

- "While the redo log is segmented and spread across storage nodes, the Log Sequence Number (LSN) space is common across the database volume, monotonically increasing, and allocated by the database instance. This is the key invariant that allows Aurora to avoid distributed consensus for most operations." [AUR18 §2.1, p. 790]
- **Storage nodes do not vote on writes:** "storage nodes do not have a vote in determining whether to accept a write, they must do so" [AUR18 §2.3, p. 791].
- **Durability points are bookkeeping.** The durable points are SCL per segment, PGCL per protection group and VCL per volume. "No consensus is required to advance SCL, PGCL, or VCL" [AUR18 §2.3, p. 791].
- **Scope:** "We limit our discussion to single-writer databases with read replicas." [AUR18 §1, p. 790]

#### 7.5.3 Volume epochs: fencing an old writer without leases

"Once the volume is available for reads and writes, Aurora increments an epoch in its storage metadata service and records this volume epoch in a write quorum of each protection group comprising the volume. The volume epoch is provided as part of every read or write request to a storage node. Storage nodes will not accept requests at stale volume epochs. This boxes out old instances with previously open connections from accessing the storage volume after crash recovery has occurred. Some systems use leases to establish short term entitlements to access the system, but leases introduce latency when one needs to wait for expiry. Aurora, rather than waiting for a lease to expire, just changes the locks on the door." [AUR18 §2.4, p. 792]

#### 7.5.4 Crash recovery

- **Steps** [AUR18 §2.4, p. 792]:
  1. The instance must reach "at least a read quorum for each protection group".
  2. It recomputes the PGCLs and the VCL from the segments' SCLs.
  3. It "snips off the ragged edge of the log by recording a truncation range that annuls any log records beyond the newly computed VCL". Writes still in flight that complete later are therefore ignored.
  4. New LSNs are allocated above the truncation range.
- **Epoch-versioned truncations.** The truncation ranges "are versioned with epoch numbers, and written durably to the storage service so that there is no confusion over the durability of truncations in case recovery is interrupted and restarted" [AUR17 §4.3, p. 1047].
- **Repair on open:** "If Aurora is unable to establish write quorum for one of its protection groups, it initiates repair from the available read quorum to rebuild the failed segments." [AUR18 §2.4, p. 792]

#### 7.5.5 Replacing a member with overlapping quorum sets and membership epochs

- **The problem:** traditional membership changes "cause I/O stalls", are "generally intolerant of additional failures during the membership change process", and most are "intolerant of readmitting previously fenced-out members" [AUR18 §4, p. 794].
- **The example, verbatim:** "Figure 5 illustrates how we replace segment F with segment G. Rather than attempting to directly transition from ABCDEF to ABCDEG, we make our transition in two steps. First, we add G to our quorum, moving the write set to 4/6 of ABCDEF AND 4/6 of ABCDEG. The read set is therefore 3/6 of ABCDEF OR 3/6 of ABCDEG. If F comes back, we can make a second membership change back to ABCDEF. ... If F continues to be down once G has completed hydrating from its peers, we can make a membership change to ABCDEG. ... We do not discard any durable state until back to a fully repaired quorum." [AUR18 §4.1, p. 794]
- **Figure 5's three states** [AUR18 Fig. 5, p. 794]:
  1. "Epoch 1: All node [sic] healthy"
  2. "Epoch 2: Node F is in suspect state; second quorum group is formed with node G; both quorums are active"
  3. "Epoch 3: Node F is confirmed unhealthy; new quorum group with node G is active"
- **A second failure during the change.** If E fails while F is being replaced, and E is to be replaced by H, the write set becomes "((4/6 of ABCDEF AND 4/6 of ABCDEG) AND (4/6 of ABCDFH AND 4/6 of ABCDGH))". In both the single- and double-failure cases, "simply writing to the four members ABCD meets quorum" [AUR18 §4.1, p. 794].
- **Membership epochs** [AUR18 §4.1, p. 794]:
  - "Each membership change to a protection group is associated with a membership epoch, which is monotonically incremented with each change. Membership changes do not block either reads or writes."
  - Every read or write from an instance, and every gossip request from a peer segment, carries the epoch. Requests with a stale epoch are rejected, and the sender must refresh its membership.
  - "An epoch increment requires a write quorum to be met, just as any other write does."
  - Membership epochs let Aurora "update membership without complex consensus, fence out others without waiting for lease expiry, and operate using the same failure tolerance as quorum reads and writes themselves."
- **Correctness claim:** "Using Boolean logic, we can prove that each transition is correct, safe, and reversible, whatever the sequence of errors and repairs may be. Transitions require only the single epoch update to the write quorum of a protection group." [AUR18 §4.1, pp. 794-795] The proof is not in the paper; how it was done is **UNVERIFIED**.
- **Volume growth and quorum changes** use a "volume geometry epoch that increments with each protection group added to the volume". This "can also be used to change the quorum model itself, for example, when moving from a 4/6 write quorum to 3/4 to handle the extended loss of an AZ" [AUR18 §4.1, p. 795].

#### 7.5.6 Unlike members: full and tail segments

- **Composition:** a protection group is "three full segments, which store both redo log records and materialized data blocks, and three tail segments, which contain redo log records alone". The cost is "closer to three copies of the data rather than a full six" [AUR18 §4.2, p. 795].
- **Quorums become Boolean expressions:** "Our write quorum is 4/6 of any segment OR 3/3 of full segments. Our read quorum is therefore 3/6 of any segment AND 1/3 of full segments." [AUR18 §4.2, p. 795]
- **Conclusion:** "The combination of epochs and quorum sets make changes reversible and non-blocking, making membership change decisions inconsequential." [AUR18 §6, p. 796]

#### 7.5.7 Rebalancing uses the repair path; the control plane

- **Heat management is repair:** "heat management is straightforward. We can mark one of the segments on a hot disk or node as bad, and the quorum will be quickly repaired by migration to some other colder node in the fleet." [AUR17 §2.3, p. 1043]
- **Upgrades** run "one AZ at a time" and ensure "no more than one member of a PG is being patched simultaneously" [AUR17 §2.3, p. 1043]. Aurora "implements quorum membership changes to handle unexpected failures, heat management, as well as planned software upgrades" [AUR18 §1, p. 789].
- **Control plane:** "The storage control plane uses the Amazon DynamoDB database service for persistent storage of cluster and storage volume configuration, volume metadata, and a detailed description of data backed up to S3. For orchestrating long-running operations, e.g. a database volume restore operation or a repair (re-replication) operation following a storage node failure, the storage control plane uses the Amazon Simple Workflow Service." [AUR17 §5, p. 1047]

---

### 7.6 CockroachDB: rebalancing and lease transfer (beyond note 06 §A4.3)

Note 06 §A4.3 already covers these topics, so they are not repeated here:

- ~64 MiB ranges, and splits and merges driven by size and load;
- leaseholder vs Raft leader;
- node-liveness leases vs expiration leases, and lease acquisition by compare-and-swap through Raft;
- the lease-disjointness invariant and its two clock safeguards, including lease-sequence checks at apply;
- snapshot vs log catch-up, and gossip;
- heartbeat coalescing and quiescence;
- joint consensus for replica moves (CRDB §7.1.2).

The additions follow.

#### 7.6.1 From the SIGMOD '20 paper

- **All membership events use one mechanism.** "Nodes can be added to or removed from running CRDB clusters, and can fail temporarily or even permanently. CRDB treats all of these scenarios similarly: they all cause load to be redistributed across the new and/or remaining live nodes." For longer-term failures, "CRDB automatically creates new replicas of under-replicated Ranges (using the unaffected replicas as sources)" [CRDB §2.2.2, p. 1495].
- **Placement signals, as far as the paper goes.**
  - Automatic placement "spreads replicas across failure domains (while adhering to the specified constraints and preferences), to tolerate varying severities of failure modes (disk, rack, data center, or region failures). CRDB also uses various heuristics to balance load and disk utilization." [CRDB §2.2.3, p. 1495] The heuristics are not specified.
  - On load-based splitting the paper says only: "Ranges also split based on load to reduce hotspots and imbalances in CPU usage" [CRDB §2.1.3, p. 1495].
- **Leaseholder placement is a separate knob from replica placement.** Under "Geo-Partitioned Leaseholders", leaseholders "can be pinned to the region of access with the remaining replicas pinned to the remaining regions" [CRDB §2.3, p. 1496]. In the TPC-C multi-region failure test, "only geo-partitioned leaseholders is tolerant to region-wide failures" [CRDB §6.2, p. 1503].
- **Adaptive lease placement did not earn its keep.** ""Follow the Workload" is a mechanism we built to automatically move leaseholders physically closer to users accessing the data. ... we've found it to be rarely used in practice. CRDB's manual controls over replica placement prove sufficient for most operators ... Adaptive techniques in databases are difficult to get right for a general purpose system, and are either too aggressive or too slow to respond. Operators favor consistency in performance; the unpredictability in this dynamic scheme hindered adoption." [CRDB §7.5, p. 1505]
- **Hot spots under range partitioning.**
  - "CRDB also supports hash indexes, which can help avoid hot spots by distributing load across multiple Ranges" [CRDB §5.1, p. 1501].
  - The comparison with Slicer (covered earlier in §7): "Slicer performs range partitioning of hashed keys, and splits/merges ranges based on load. ... CRDB range-partitions based on the original keys, resulting in better locality for range scans than Slicer, but susceptibility to hot spots. To alleviate hot spots, it can also partition on hashed keys. Like Slicer, CRDB splits, merges, and moves Ranges to balance load." [CRDB §8, p. 1506]
- **Lease-transfer safety.** The paper states this only at the level already summarized in note 06: disjointness "is enforced on cooperative lease handoff with causality transfer through the HLC and is enforced on non-cooperative lease acquisition through a delay equal to the maximum clock offset between lease intervals" [CRDB §4.1, p. 1499], plus the lease-sequence check at apply [CRDB §4.3, p. 1500].
- **Not in the paper:** learner (non-voting) replicas; the sequence of a replica move (add a learner, send a snapshot, enter a joint configuration, remove the old replica); what triggers a lease transfer; and the load and disk heuristics. **UNVERIFIED** from the peer-reviewed source.

#### 7.6.2 From CockroachDB's documentation (all NON-PEER-REVIEWED; "stable" docs = v26.3, retrieved 2026-09-28)

- **Leaseholder rebalancing** [CRDB-DOCS-REPL, "Leaseholder rebalancing"]:
  - "Periodically (every 10 minutes by default in large clusters, but more frequently in small clusters), each leaseholder considers whether it should transfer the lease to another replica by considering the following inputs: Number of requests from each locality[;] Number of leases on each node[;] Latency between localities".
  - The leaseholder "tracks how many requests it receives from each locality as an exponentially weighted moving average".
- **Replica moves** [CRDB-DOCS-REPL, "Membership changes: rebalance/repair"]: "Rebalancing is achieved by using a snapshot of a replica from the leaseholder, and then sending the data to another node over [gRPC]. After the transfer has been completed, the node with the new replica joins that range's Raft group; it then detects that its latest timestamp is behind the most recent entries in the Raft log and it replays all of the actions in the Raft log on itself." The page does not mention learners or joint configurations.
- **Load-based rebalancing (v26.3)** [CRDB-DOCS-REPL, "Load-based lease and replica rebalancing"]:
  - A "multi-metric allocator (MMA)" runs beside the legacy allocator. It is "distributed and heuristic rather than a centralized global optimizer".
  - It models "CPU usage, write bandwidth, and disk utilization", but by default "only CPU overload initiates rebalancing".
  - It will "Prefer lease transfers for CPU overload because they are cheaper and more reversible than replica moves".
  - It filters targets by "placement constraints, locality diversity, health, disk-utilization thresholds, and the existing replica layout".
  - It will "Make a small number of changes and reassess".
  - "Repair, replication, placement, survivability, and count requirements can take precedence over load balancing."
- **Leases today** [CRDB-DOCS-REPL, "Leader leases"; "Co-location with Raft leadership"]:
  - "Leader leases" supersede "the former system of having different epoch-based and expiration-based lease types".
  - "The range lease is always colocated with Raft leadership via the Leader leases mechanism, except briefly during lease transfers."
  - This supersedes the 2020 paper's lease description summarized in note 06 §A4.3.
- **Load-based splitting** [CRDB-DOCS-LBS, "Control load-based splitting threshold"; "How load-based splitting works"; "Metrics"]:
  - A range becomes eligible when it exceeds `kv.range_split.load_qps_threshold` ("defaults to 2500" QPS).
  - The split point is chosen by a "Balance factor" heuristic ("would there be a balance of load on both sides of the split?") and a "Split crossing" heuristic ("how many queries would have to cross this new range boundary?").
  - Metrics count failures to find a split key: because of a popular key (it "occurs in >= 25% of the samples"), or because of a clear access direction ("greater than 80%" in one direction).

#### 7.6.3 Range merges tech note (NON-PEER-REVIEWED primary source, 2019)

Note 06 §A4.4 only sketches the merge safety mechanisms, for TiDB. This note spells them out for CockroachDB:

- **Aligned replica sets** [CRDB-MERGE-TN, "Overview"; "Safety recap"]. A merge requires "that the set of stores with replicas of the LHS exactly matches the set of stores with replicas of the RHS". That reduces the merge to "a metadata update, albeit a tricky one". The merge queue aligns the replica sets first, and the merge transaction verifies alignment again. "We explored an alternative merge implementation that did not require aligned replica sets, but found it to be unworkable." [CRDB-MERGE-TN, "Preconditions"]
- **Freezing the subsumed range** [CRDB-MERGE-TN, "Overview"; "Safety recap"]. The right-hand range "is prohibited from serving any additional read or write traffic". The coordinator commits only after "an acknowledgement from _every_ replica of the RHS". The freeze is tied to the lifetime of the merge transaction and survives lease transfers and leaseholder restarts.
- **Unanimity, by choice** [CRDB-MERGE-TN, "Unanimity"]. "There is no theoretical reason that merges need unanimous consent, but the complexity of the implementation quickly skyrockets without it." The mitigation offered is that merges are never urgent: "a situation where a merge is critical to the health of a cluster is difficult to imagine".
- **Generation counter against ABA** [CRDB-MERGE-TN, "Range descriptor generations"]. A rebalance racing a merge followed by a split at the same key would see an unchanged range descriptor. So the descriptor gained a `generation` that "is incremented on every split and every merge". "It is no longer possible for a range descriptor to be unchanged by a sequence of splits and merges".

---

### 7.7 Dynamo (DeCandia et al., SOSP '07): partitioning strategies

#### 7.7.1 Consistent hashing with virtual nodes [DYN §4.1-§4.2, pp. 209-210]

- **The ring.** Keys are MD5-hashed to "a 128-bit identifier" [DYN §4.1, p. 209]. On a consistent-hashing ring, "departure or arrival of a node only affects its immediate neighbors" [DYN §4.2, pp. 209-210].
- **Its problem:** random node positions lead "to non-uniform data and load distribution" [DYN §4.2, p. 210].
- **Virtual nodes ("tokens").** Giving each node many ring positions [DYN §4.2, p. 210]:
  - means a failed node's load "is evenly dispersed across the remaining available nodes";
  - lets a new node take "a roughly equivalent amount of load from each of the other available nodes";
  - lets the token count follow a node's "capacity, accounting for heterogeneity".

#### 7.7.2 Three strategies, and what production taught [DYN §6.2, pp. 215-217]

- **Measurement** [DYN §6.2, p. 215].
  - Over 24 hours, in 30-minute windows, a node is out of balance if its request load deviates from the average by more than 15%.
  - The out-of-balance fraction ("imbalance ratio") was "as high as 20%" at low load and "close to 10%" at high load.
  - The design assumption: "even where there is a significant skew in the access distribution there are enough keys in the popular end of the distribution so that the load of handling popular keys can be spread across the nodes uniformly through partitioning".
- **Strategy 1: T random tokens per node, and partition by token value.** This was "the initial strategy deployed in production". Ranges vary in size and change as nodes join and leave. "The fundamental issue with this strategy is that the schemes for data partitioning and data placement are intertwined"; "Ideally, it is desirable to use independent schemes for partitioning and placement." [DYN §6.2, p. 216]
- **Strategy 2: T random tokens per node, and equal-sized partitions** [DYN §6.2, p. 216].
  - The hash space is split into Q equal partitions, with "Q >> N and Q >> S*T". The tokens only build the placement function.
  - **Problems found in production:**
    - A joining node must "steal" ranges, and the donor nodes must scan their stores at the lowest priority. As a result, "during busy shopping season ... the bootstrapping has taken almost a day to complete".
    - Merkle trees must be recomputed for the changed ranges.
    - Archiving the whole key space was inefficient.
- **Strategy 3: Q/S tokens per node, and equal-sized partitions** [DYN §6.2, p. 216].
  - There are Q fixed, equal partitions, and "each node is assigned Q/S tokens where S is the number of nodes in the system".
  - A leaving node's tokens "are randomly distributed to the remaining nodes such that these properties are preserved". A joining node "steals" tokens in the same way.
- **Result** (S=30, N=3, equal budget for membership metadata, Fig. 8) [DYN §6.2, pp. 216-217].
  - Load-balancing efficiency is defined as mean requests per node divided by the requests served by the hottest node.
  - "strategy 3 achieves the best load balancing efficiency and strategy 2 has the worst".
  - "Compared to Strategy 1, Strategy 3 achieves better efficiency and reduces the size of membership information maintained at each node by three orders of magnitude."
- **Operational advantages of Strategy 3:** "(i) Faster bootstrapping/recovery: Since partition ranges are fixed, they can be stored in separate files, meaning a partition can be relocated as a unit by simply transferring the file (avoiding random accesses needed to locate specific items)" and "(ii) Ease of archival" [DYN §6.2, p. 217].
- **Cost:** "The disadvantage of strategy 3 is that changing the node membership requires coordination in order to preserve the properties required of the assignment." [DYN §6.2, p. 217]
- **Not stated:** the production values of Q and T. **UNVERIFIED.**

#### 7.7.3 Client-side routing tables [DYN §6.4, pp. 217-218]

- **Pull, with immediate refresh on failure.**
  - In client-driven coordination, "A client periodically picks a random Dynamo node and downloads its current view of Dynamo membership state".
  - "Currently clients poll a random Dynamo node every 10 seconds for membership updates. A pull based approach was chosen over a push based one as the former scales better with large number of clients and requires very little state to be maintained at servers".
  - Membership can be up to 10 s stale. A client that detects staleness, "for instance, when some members are unreachable", refreshes immediately [DYN §6.4, p. 218].
- **Measured (Table 2)** [DYN §6.4, Table 2, p. 218]:

  | Coordination | 99.9th pct read (ms) | 99.9th pct write (ms) | Avg read (ms) | Avg write (ms) |
  |---|---|---|---|---|
  | Server-driven | 68.9 | 68.5 | 3.9 | 4.02 |
  | Client-driven | 30.4 | 30.4 | 1.55 | 1.9 |

---

#### 7.7.4 Implications for mantle (WAS, Aurora, CockroachDB, Dynamo)

These are inputs to §9. Every item is **INFERENCE** unless tagged otherwise, and each cites the facts it rests on.

**Cells and routing**

1. **A mantle cluster is a WAS stamp: the regional cell.**
   - Each cluster owns a data plane: the chunk stores, which play the role of WAS's stream layer and extent nodes.
   - It has its own intra-cell control plane: placement and repair (the SM's job) and range assignment (the PM's job).
   - It owns its own metadata ranges (the partition layer's job) [WAS §3.2-§3.4, pp. 144-146].
   - Tectonic already has this shape: a datacenter-local cluster with its own Metadata Store, Chunk Store and background services, with geo-replication left to tenants (note 01 §1.2).
   - What WAS adds: an explicit layer above the cells (the LS), and tenant migration between cells [WAS §3.2, p. 145; §5.6, p. 152].
   - On a laptop there is one cell and a one-entry location table.
2. **The router is a location record plus DNS, not a proxy tier.**
   - WAS's route is DNS keyed on the tenant name in the host name, published by the LS *after* the stamp has been told to accept the account [WAS §2, p. 144; §3.2, p. 145].
   - mantle's analogue of AccountName is the bucket in a virtual-hosted-style host name (`bucket-name.s3.region-code.amazonaws.com`, note 05 §14). A location service can publish `bucket.<endpoint>` as that cell's VIP.
   - Two cases do not fit: path-style requests, and dotted bucket names over HTTPS. "the SSL wildcard certificate" does not match dotted names, so they fall back to path-style (note 05 §14). Path-style requests carry the bucket only in the path, so they need an L7 redirect or a proxy hop. That is a separate decision, because WAS never faced it: its account name is always in the host name.
   - Order of operations: the cell accepts the bucket, then the location record is committed, then DNS is published. Removal runs in reverse.
3. **Size each cell by its control plane, and state the bound.**
   - WAS bounds a stamp by the SM's in-memory metadata (50 M extents and 100 K streams in 32 GB) and derives its range watermark from that bound [WAS §4.1, p. 147; §5.5, p. 151].
   - mantle should state its cell bound in terms of its own control plane: placement-map entries, ranges per node, and range-directory size. Reaching the bound is a typed refusal (CLAUDE.md rule 2).
   - WAS's 70% target and 80% ceiling are specific to HDD short-stroking and rack failure [WAS §3.2, p. 145]. mantle should derive its own headroom from the measured size of the largest failure domain (CLAUDE.md rule 4).
   - The rule that a bucket lives in exactly one cell (WAS's 100 TB account rule [WAS §8, p. 155]) keeps routing to a single lookup. With Tectonic-sized cells, the size cap will rarely bind.
4. **Migrating a bucket between cells: replicate, fence, flip.**
   - WAS migrates by asynchronous object-level replication followed by a "clean failover" and a DNS switch, and URIs do not change [WAS §3.4, p. 145; §5.6, p. 152]. The fencing step is not published (**UNVERIFIED**), so mantle must specify its own:
     1. The location record enters `migrating(source→target, epoch e)`.
     2. The target catches up by object-level replication.
     3. The source refuses writes at e+1 with a typed redirect and drains its tail.
     4. The record flips to the target at e+1.
     5. DNS and redirects are updated.
   - Stale clients that reach the source get a redirect carrying the new location. This is the same stale-route rejection as Bigtable and TiDB (note 06 §A4.1, §A4.4).
   - Writes are unavailable between steps 3 and 4. That window needs a stated bound and a measurement.
5. **Routing caches: pull on a stated interval, and refresh immediately on a stale-route error.**
   - WAS front-ends cache the Partition Map [WAS §3.3, p. 145].
   - Dynamo clients pull membership every 10 s and refresh as soon as they detect staleness. Client-side routing halved 99.9th-percentile latency (68.9 → 30.4 ms for reads) [DYN §6.4, Table 2, p. 218].
   - mantle gateways should cache range descriptors together with their generation (item 7).

**Moving ranges inside a cell (metadata plane)**

6. **Split in place, publish the directory, then move.**
   - WAS splits on the parent's server and moves a child only after the Partition Map is updated [WAS §5.5.2, p. 152]. TiDB applies the split as one Raft command, and the rightmost child keeps the Raft group (note 06 §A4.4).
   - For mantle's multi-Raft ranges (note 06 §C.a):
     1. A split is a Raft command in the parent's log, and both children stay on the parent's replica set.
     2. The range directory is updated next.
     3. Only then may the balancer move a child.
   - A gateway holding a stale directory entry still reaches a replica that serves one of the children, and gets a precise redirect.
7. **Range descriptors carry a generation and a membership epoch.**
   - The generation is bumped on every split and merge [CRDB-MERGE-TN, NON-PEER-REVIEWED].
   - The membership epoch is bumped on every change to the replica set: TiDB's epoch (note 06 §A4.4) and Aurora's membership epoch [AUR18 §4.1, p. 794].
   - Every request and every rebalance decision carries the descriptor version it was based on, and replicas reject stale ones.
   - The generation catches the merge-then-split-at-the-same-key ABA case that comparing (start, end) alone would miss [CRDB-MERGE-TN].
8. **Merges are rare, co-located, frozen and unanimous.**
   - Every system surveyed co-locates first: WAS on the same PS [WAS §5.5.3, p. 152]; TiDB co-locates replicas (note 06 §A4.4); CockroachDB requires aligned replica sets, a freeze and unanimity [CRDB-MERGE-TN].
   - mantle should merge only when:
     - the two replica sets are identical;
     - the right-hand range is frozen by a Raft command that survives leader changes;
     - every right-hand replica has acknowledged.
   - Merges are background work and may always be abandoned.
9. **Keep about ten times more ranges than servers, bounded by watermarks.**
   - WAS keeps partitions between watermarks at about 10× the partition-server count, so a failed server's load spreads widely [WAS §5.5, p. 151]. Tectonic sizes shards so that "each metadata node can host several shards" (note 01 §1.5).
   - mantle should use low and high watermarks on ranges per node, with the merge rate rising near the high watermark [WAS §5.5, p. 151] (CLAUDE.md rule 2).
10. **Split triggers include saturation signals, and each cycle has a bound.**
    - What the sources do:
      - WAS balances on request, CPU and network load and has separate split triggers (throttling, timeouts, size). It splits "a small number" per 15-second quantum [WAS §8, p. 154].
      - CockroachDB's docs add a split-key choice that balances the two halves and minimizes boundary crossings, and they give up when one key dominates [CRDB-DOCS-LBS, NON-PEER-REVIEWED].
    - A mantle split-for-heat decision should need all three of:
      - (a) a saturation signal at the range, such as throttling or missed deadlines;
      - (b) a split key that balances load without cutting a hot directory listing into many cross-range scans;
      - (c) a per-cycle cap on splits and moves.
    - Splitting cannot fix a single hot object key. It is throttled, as in WAS [WAS §8, p. 154], or served from a cache.
11. **Prefer predictable, explicit placement.**
    - CockroachDB's automatic Follow-the-Workload was "rarely used", and "Operators favor consistency in performance" [CRDB §7.5, p. 1505]. CockroachDB's current allocator prefers lease transfers to replica moves because they are "cheaper and more reversible" [CRDB-DOCS-REPL, NON-PEER-REVIEWED].
    - mantle should balance leader and lease *counts* first and move replicas second. It should not chase request locality by default.

**Moving chunk replicas (data plane)**

12. **Use Aurora's epochs for replica moves in the data plane, not Raft.**
    - Aurora avoids consensus because one writer allocates monotonically increasing LSNs [AUR18 §2.1, p. 790]. Tectonic blocks have the same property: a single writer per file, and appends only by the block's creator (note 01 §1.7).
    - So the replica set of an *unsealed* block can change Aurora-style:
      1. Add G under epoch e+1, with a write quorum drawn from both the old and the new set.
      2. Hydrate G.
      3. Drop F at e+2.
    - The change is reversible and non-blocking, and needs no Raft group per block. The epoch is written to a write quorum of the block's chunk stores, every append carries it, and stale epochs are refused [AUR18 §4.1, p. 794].
    - mantle's chunk record already carries an "epoch (layout generation of the block)", and its index key is (block id, epoch, chunk index) (docs/design/chunk-store.md §3.1, §5). The hook exists. Refusing appends below the highest epoch seen for a block still needs a persisted per-block high-water mark, which is design work not yet specified.
    - Metadata ranges have many writers, so they keep Raft joint consensus (note 06 §A4.3; CRDB §7.1.2, pp. 1504-1505).
13. **Seal-and-move-on is simpler, but the seal-length rule must match the acknowledgement rule.**
    - WAS seals a failed extent and continues on a new one within about 20 ms [WAS §4.3.1, pp. 147-148].
    - WAS's "smallest commit length" rule is safe only because every append is acknowledged after *all three* replicas persist it [WAS §4.3.2, p. 148].
    - **DERIVED:** Tectonic acknowledges quorum appends at 2 of 3 (note 01 §0 item 3). Suppose A and B acknowledged a write and the sealer can reach only B and C. min(B, C) then drops the acknowledged write. The safe seal length for Tectonic-style appends is therefore the size committed to block metadata before the acknowledgement (note 01 §0 item 3, §1.9). Replicas longer than that are truncated, and shorter ones are repaired.
    - mantle's chunk store already absorbs WAS's duplicate-append problem at the store. It accepts an exact repeat of a durable fragment and refuses non-contiguous appends (docs/design/chunk-store.md §4).
14. **Fence writers without leases.**
    - Aurora records a volume epoch in a write quorum of each protection group and checks it on every request, "rather than waiting for a lease to expire" [AUR18 §2.4, p. 792].
    - This fills the gap note 01 §6.2 item 2 left open (Tectonic's write token has no lease or expiry). A new writer, or recovery, bumps the block's epoch at a write quorum of its chunk replicas before its first append. The old writer's later appends then fail.
15. **Move sealed blocks by copy, verify, compare-and-swap, then delete after a grace period.**
    - Sealed blocks are immutable, so no quorum-set dance is needed:
      1. Copy the chunk.
      2. Verify its checksum.
      3. Compare-and-swap the Block-layer location entry in the block's shard (note 01 §1.5).
      4. Delete the old chunk after the chunk store's grace period (docs/design/chunk-store.md §8).
    - WAS's sealed, immutable extents play the same role [WAS §4, p. 146; §4.3.2, p. 148].
16. **Repair, rebalance, drain and upgrade are one mechanism.**
    - In the sources:
      - Aurora rebalances heat by marking a segment bad and letting repair migrate it [AUR17 §2.3, p. 1043].
      - WAS drains an upgrade domain by moving partitions out, stopping extent allocation there, and checking replica counts outside it before shutdown [WAS §8, p. 155].
      - Tectonic's rebalancer and repair service "work in tandem" (note 01 §1.10).
      - CockroachDB treats adding, removing and failing nodes the same way [CRDB §2.2.2, p. 1495].
    - For mantle: the rebalancer only *chooses* what to move, and repair *moves* it. Draining means stopping new allocations on the target, then evacuating it, followed by a redundancy check before the machine is released. This applies directly to STATUS.md planned item 4 (retiring failing disks).

**Partitioning scheme**

17. **Use both fixed and dynamic partitions, chosen per layer.**
    - What the sources show:
      - Dynamo's Strategy 3 uses fixed equal hash partitions with placement decoupled, and moves a partition as a file. It balanced better than random tokens and cut membership metadata by three orders of magnitude [DYN §6.2, pp. 216-217]. Note 01's M3 (vshard = H(key) mod V) is the same idea.
      - WAS and CockroachDB split dynamically on the original keys. That gives ordered enumeration and per-tenant locality, but exposes them to sequential-key hotspots [WAS §8, p. 154; CRDB §8, p. 1506].
      - Slicer and CockroachDB's hash option combine the two: range partitioning over hashed keys, with load-based split and merge [CRDB §8, p. 1506].
    - Proposal for the parent's mapping:
      - The File and Block layers never need ordered scans across hash buckets. They use a hashed key space with ranges over it.
      - The Name/listing layer, which S3 must enumerate in key order (note 05 §6), is range-partitioned on (bucket, key) with split-for-heat.
    - WAS is peer-reviewed production evidence that a range-partitioned blob index works [WAS §5.1, p. 149; §8, p. 154], along with its documented weakness for sequential keys. This revisits note 01 §6.2 item 1 with new evidence.
18. **Testing.** WAS's pressure points (split, merge and load balance; erasure code and un-erasure code; crash each server type; inject latencies; in chosen or random orders) [WAS §8, p. 156] are the operations mantle's deterministic simulator must be able to trigger for cells and ranges (note 06 §C.d).

#### 7.7.5 UNVERIFIED / not found

| Item | Status |
|---|---|
| WAS: how long a split or merge pauses traffic | Not given ("briefly") [WAS §5.5.2-§5.5.3, p. 152] |
| WAS: how front-ends detect and repair stale Partition Map entries | Not described |
| WAS: Lock Service and partition-server lease parameters | Deferred to Chubby [WAS §5.2, p. 150]; not given |
| WAS: steps of the "clean failover" used for migration (write fencing, drain, DNS TTLs) | Not described [WAS §5.6, p. 152] |
| WAS: weights of the LS stamp-selection heuristics | Only "fullness ... network and transaction utilization" [WAS §3.2, p. 145] |
| WAS: how an interrupted split or merge is resumed or rolled back | Only that its status is written to the metadata stream [WAS §5.3.1, p. 150] |
| WAS: whether any of this reflects Azure Storage after 2011 | Not reviewed |
| Aurora: how storage nodes persist volume and membership epochs, and whether the "storage metadata service" is the DynamoDB-backed control plane | Not described. AUR17 §5 (p. 1047) says only that the storage control plane uses DynamoDB |
| Aurora: the Boolean-logic proof of the quorum-set transitions | Claimed, not included [AUR18 §4.1, pp. 794-795] |
| Aurora: failure-detection time | "Assuming a 10 second window" is an analysis assumption [AUR18 §2.1, p. 790] |
| CockroachDB: learner replicas, and the learner → snapshot → joint configuration → remove sequence for a move | Not in the paper, and not on the v26.3 replication-layer page |
| CockroachDB: the paper's "various heuristics" for load and disk balance | Not specified [CRDB §2.2.3, p. 1495]; the MMA description is docs-only (NON-PEER-REVIEWED, version-specific) |
| Dynamo: production values of Q and T; whether Strategy 3 remains in use | Not stated (2007 paper) |

### 7.8 Akkio: Meta's µ-shard migration between ZippyDB replica sets (Annamalai et al., OSDI '18)

Note 01 used Akkio only for ZippyDB's replication facts. This subsection covers the part relevant here: Meta's peer-reviewed protocol for moving small, application-defined units between replica sets online. That is the Tectonic-side counterpart to moving a bucket or tenant between cells. Pages are the printed proceedings pages 445–460, read in full from the text layer.

#### 7.8.1 What moves, and why small [AKKIO Abstract, §1, pp. 445–446]

- **What Akkio is.** "a locality management service layered between client applications and distributed datastore systems. It determines how and when to migrate data to reduce response times and resource usage." It has been in production since 2014 and manages "∼100PB of data" [AKKIO Abstract, p. 445].
- **Shards are too big to move for locality.** Datastore shards are "on the order of one to a few tens of gigabytes", and "multiple shards (10s – 100s) are assigned to a node". Because "the working set size of accessed data tends to be less than 1MB, migrating an entire shard (1-10GB) would be ineffective" [AKKIO §1, p. 446].
- **µ-shards.** They "typically vary from a few hundred bytes to a few megabytes". "It is the application that determines which data is assigned to which µ-shard", and "a µ-shard never spans multiple shards" [AKKIO §1, p. 446]. The Fig. 1 caption gives "Avg. shard size is 2GB; avg. µ-shard size is 200KB" [AKKIO Fig. 1, p. 446].

#### 7.8.2 ZippyDB's own shard management (the Tectonic metadata substrate) [AKKIO §3, pp. 449–450]

- **Replication.** Each ZippyDB shard has a primary and secondaries, and "each replica participates in a shard-specific Paxos group" [AKKIO §3, p. 449].
- **Replica set collections.** A shard's "replication configuration" (replica count and spread over datacenters, clusters and racks) is chosen per service. A "replica set collection" is "the group of all replica sets that have the same replication configuration", identified by a "location handle" [AKKIO §3, p. 450].
- **Shard Manager's role.** "ZippyDB's Shard Manager assigns each shard replica to a specific ZippyDB server while obeying the specified policy rules. The assignment is registered with a Directory Service so that the ZippyDB client library ... can identify the server to send its access requests to. Shard Manager is also responsible for: (i) load balancing, by migrating shards if necessary; and (ii) monitoring the liveliness of ZippyDB servers" [AKKIO §3, p. 450].
- **DERIVED.** Tectonic's metadata store gets placement, load balancing and failover from a *separate, centralized* shard-management control plane with a directory service. This is the Meta analogue of Slicer (§7.1). Shard Manager's own paper is summarized in §7.3.

#### 7.8.3 Location lookup and stale caches [AKKIO §4.1, §4.3–§4.4, pp. 451–452]

- **Only the lookup is synchronous.** "the only operation in the critical path is the µ-shard location lookup needed for each data access" [AKKIO §4.1, p. 451]. Access counting and placement evaluation are asynchronous, and "a µ-shard access never waits for a potential migration to be evaluated or complete" [AKKIO §4.3, pp. 451–452].
- **Where locations live.** The Akkio Location Service stores each µ-shard's location handle in ZippyDB, configured "to have an eventually consistent replica at every datacenter", justified by a read/write ratio "> 500". "distributed in-memory caches are used at every datacenter" [AKKIO §4.4, p. 452].
- **Stale route → reject → bypass the cache.** "It is possible that the distributed cache will serve a stale location mapping ... The target ZippyDB server will determine that the µ-shard is not present from the missing ACL, and will respond accordingly. When that happens, the ZippyDB client library queries the Akkio Location Service again, this time requesting that the cache be bypassed" [AKKIO §4.4, p. 452]. This is the same liveness-only failure mode as Physalia's discovery cache (§1.8).
- **Size.** "each µ-shard requires at most a few hundred bytes of storage" in the location database [AKKIO §4.4, p. 452].

#### 7.8.4 Two migration protocols [AKKIO §4.6–§4.6.3, pp. 452–454; Listings 2–3]

- **Serializing migrations.** The Data Placement Service (DPS) "stores ... locks to prevent multiple concurrent migrations of the same µ-shard, and sufficient information needed to recover the migration should a DPS server fail". It also keeps the "time of last migration to limit migration frequency (to allow the prevention of µ-shards ping-ponging)" [AKKIO §4.6, p. 453]. The sample policy limits each µ-shard to one migration "every 6 hours" [AKKIO §4.6.2, p. 453].
- **Protocol A: write-blocking, for stores with ACLs and transactions (ZippyDB)** [AKKIO Listing 2, p. 453; §4.6.3, p. 454]:
  1. Atomically acquire the µ-shard lock and add the migration to the ongoing-migrations list.
  2. Set the source ACL to read-only.
  3. Read the source.
  4. Atomically write the destination and set the destination ACL to read-only.
  5. Update the location DB.
  6. Delete the source and its ACL.
  7. Set the destination ACL to read-write.
  8. Atomically release the lock and remove the migration from the list.

  Setting the source read-only "effectively blocks writes for the duration of the migration". The client library "will automatically retry the write if the previous attempt was blocked". For ViewState, "writes are retried in 0.007% of all accesses" [AKKIO §4.6.3, p. 454 and footnote 7].
- **Protocol B: double-write, for stores with timestamps but no ACLs (Cassandra)** [AKKIO Listing 3, p. 454; §4.6.3, p. 454]:
  1. Acquire the lock.
  2. Double-write to source and destination.
  3. Wait for the location cache TTL.
  4. Copy the data from before double-writing began, merging by timestamp.
  5. Switch reads to the destination.
  6. Wait for the TTL.
  7. Switch writes to the destination only.
  8. Wait for the TTL.
  9. Remove the source.
  10. Release the lock.

  Two details matter:
  - **Three waits.** "each time the location database is updated, which occurs three times, it is necessary to wait for the location database TTL to expire to ensure no stale accesses go to the wrong destination". The waits could be avoided "if cache entries could be reliably invalidated".
  - **Write order.** If a write succeeds on the source but not the destination, it would be visible on the source but lost after cutover. "We address this by always first writing to the destination, before writing to the source, on double writes."

#### 7.8.5 Removing capacity, and fencing a crashed mover [AKKIO §4.6.4–§4.6.5, pp. 454–455]

- **Drain.** A replica set collection is first "disabled in the configuration, preventing the DPS from selecting" it. Then, "in an off-line process", every µ-shard in it is re-evaluated and migrated away. Adding one is simple: the DPS starts using it [AKKIO §4.6.4, p. 454].
- **Fencing.** "every DPS server instance is assigned a monotonically increasing sequence number (which is obtained from a global Zookeeper deployment). This sequence number is persisted with all state related to pending migrations; e.g., in the per µ-shard lock". A restarted DPS gets a higher number, finds unfinished migrations, and updates their sequence number "to avoid any conflicts with a stale, failed DPS server instance" [AKKIO §4.6.5, pp. 454–455].
- **Resuming.** It then "scans the state of the µ-shard in the source backend and the destination backend to identify which steps of the migration had been completed" and resumes from there. Migrations are "typically retried until they succeed" [AKKIO §4.6.5, p. 455].

#### 7.8.6 Implications for mantle (Akkio)

- **A1. The bucket is mantle's µ-shard for cross-cell moves.** *INFERENCE* [AKKIO §1].
  - A bucket is an application-visible unit whose data is accessed together. Moving whole buckets between regional cells matches Akkio's µ-shard, "Akkio's unit of migration", whose contents the application chooses: "It is the application that determines which data is assigned to which µ-shard" [AKKIO Abstract, p. 445; §1, p. 446].
  - Moving metadata ranges (1-10 GB class, §1) belongs *inside* a cell, under the placement driver.
- **A2. Prefer Protocol A (block writes, client retries) for bucket moves; use cache invalidation, not TTL waits.** *INFERENCE* [AKKIO §4.6.3].
  - mantle controls both the store and the client (gateway). The source can be fenced read-only with a single epoch bump, and gateways retry transparently: S3 clients already retry 503 SlowDown (§6).
  - Double-writing needs timestamp merge and three cache-TTL waits. Akkio used it only where the store lacked ACLs.
- **A3. Fence every mover with a monotonically increasing sequence number persisted in the migration record.** Recover by inspecting both sides [AKKIO §4.6.5]. In mantle the number comes from a Raft-replicated control-plane range, not ZooKeeper. *INFERENCE.*
- **A4. Rate-limit per-unit migrations to stop ping-pong** [AKKIO §4.6, §4.6.2]. Together with Physalia's "don't move faster than the expected rate of change" (§1.6), this is two independent peer-reviewed sources for bounding control-plane churn. *INFERENCE.*
- **A5. Stale-location handling needs no invalidation protocol for correctness.** The owner rejects, and the client re-reads the directory bypassing its cache [AKKIO §4.4]. Correctness requires that the server *can* reject: an ACL in Akkio, an epoch/descriptor check in mantle. *INFERENCE.*

#### 7.8.7 UNVERIFIED / not found (Akkio)

- Whether Tectonic's own ZippyDB deployment uses Akkio: not stated.
- Shard Manager's shard-move protocol for ZippyDB shards: not described in Akkio (see §7.3 for the Shard Manager paper).

### 7.9 Spanner's directories and Movedir (Corbett et al., OSDI '12), with the other range systems in note 06

Note 06 §A4.2 already quotes Spanner's Movedir. This subsection adds page numbers, which were checked against the PDF (pp. 251–264), and the facts about placement roles that matter for cells.

- **Zones are cells.** "Zones are the unit of administrative deployment." "Zones are also the unit of physical isolation: there may be one or more zones in a datacenter, for example, if different applications' data must be partitioned across different sets of servers in the same datacenter." Zones "can be added to or removed from a running system as new datacenters are brought into service and old ones are turned off" [SPN §2, p. 252].
- **Three placement roles** [SPN §2, p. 252]:
  - Each zone has "one zonemaster and between one hundred and several thousand spanservers. The former assigns data to spanservers".
  - "The per-zone location proxies are used by clients to locate the spanservers assigned to serve their data."
  - A singleton "placement driver handles automated movement of data across zones on the timescale of minutes", periodically finding "data that needs to be moved, either to meet updated replication constraints or to balance load".
- **The directory is the unit of placement and movement** [SPN §2.2, p. 253]:
  - A directory is "a set of contiguous keys that share a common prefix. (The choice of the term directory is a historical accident; a better term might be bucket.)"
  - "A directory is the unit of data placement. All data in a directory has the same replication configuration."
  - "Spanner might move a directory to shed load from a Paxos group; to put directories that are frequently accessed together into the same group; or to move a directory into a group that is closer to its accessors. Directories can be moved while client operations are ongoing. One could expect that a 50MB directory can be moved in a few seconds."
- **Movedir: copy in the background, then a short atomic cutover** [SPN §2.2, p. 254]:
  - "Movedir is not implemented as a single transaction, so as to avoid blocking ongoing reads and writes on a bulky data move."
  - "Instead, movedir registers the fact that it is starting to move data and moves the data in the background. When it has moved all but a nominal amount of the data, it uses a transaction to atomically move that nominal amount and update the metadata for the two Paxos groups."
  - Movedir was also used "to add or remove replicas to Paxos groups ..., because Spanner does not yet support in-Paxos configuration changes".
- **Oversized directories.** "Spanner will shard a directory into multiple fragments if it grows too large ... Movedir actually moves fragments, and not whole directories, between groups" [SPN §2.2, p. 254].
- **Load-based resharding was not done in 2012.** Future work lists "automatic load-based resharding" as being implemented [SPN §7, p. 262].

**Other move and split protocols, by cross-reference** (full quotes in note 06 §A4):

| System | Move / split mechanism | Fencing of stale routes and owners | Source |
|---|---|---|---|
| Bigtable | Tablet servers commit splits in METADATA; the master assigns tablets. A planned move runs a minor compaction, stops serving, runs a second compaction, then loads elsewhere "without requiring any recovery of log entries" | Chubby server locks; the master deletes a dead server's lock file; stale client caches are found on a miss (≤6 round trips) | note 06 §A4.1 |
| Spanner | Movedir: background copy, then a transaction moving the "nominal amount" and updating both groups' metadata | Paxos leader leases (10 s) with disjointness | above; note 06 §A4.2 |
| CockroachDB | Size- and load-based splits; merges; joint consensus for replica moves; leases through the Raft log with a lease sequence checked at apply | Lease sequence number per proposal; descriptors in a two-level meta index | note 06 §A4.3; §7.6 below |
| TiDB/TiKV | The split is a Raft command, "applied atomically and synced to disk"; merge needs co-location first; PD schedules moves | Region epoch: "the group of nodes with the most recent epoch wins" | note 06 §A4.4 |
| Akkio (Meta) | µ-shard copy with source read-only (ACLs), or double-write with TTL waits | ACL rejection plus a cache-bypass re-read; DPS sequence numbers | §7.8 above |
| Physalia (AWS) | One node at a time by reconfiguration, with teaching | Configuration in the state machine; forwarding pointers; TLA+-checked stale-cache safety | §1.7–§1.8 above |

---

## 8. Shuffle sharding: the math and its sources

### 8.1 Source audit: is there a peer-reviewed source?

**Result: no peer-reviewed paper was found that analyzes shuffle sharding itself.** Here, shuffle sharding means per-tenant, overlapping k-of-n subsets of workers, with the client failing over inside its subset. The technique and its numbers come only from NON-PEER-REVIEWED AWS material: SS-BLOG (2014), BL-SHUFFLE (2019), the INFIMA source and the CELL-WP FAQ (§5.12).

Searched on 2026-09-28: Crossref bibliographic queries, the Semantic Scholar API, the USENIX site, and general web search for "shuffle sharding" with arXiv, USENIX, ACM and "et al.". The dblp API refused the connection and Google Scholar was not queried directly, so the search is **not exhaustive**.

Peer-reviewed work found, most relevant first:

1. **CYBERSTAR (USENIX ATC '24): uses shuffle sharding in production, but proves nothing about it.**
   - Alibaba's network-function platform "adopts Shuffle sharding [67], an effective method for segregating tenants' workloads by distributing traffic across multiple instances with minimal overlap" [CYBERSTAR §3.1 "Design Rationale", p. 231; §7.1, p. 235]. Reference [67] is SS-BLOG.
   - Its placement ILP writes the isolation requirement as two constraints [CYBERSTAR Appendix A.1, Table 2, Eqs. (5)-(6), p. 244]:
     - Shard size G_min ≤ G_i ≤ G_max, "to control the incidence caused by user 'poison' requests" (Eq. 5).
     - A pairwise overlap bound Σ_e x_ei·x_ej ≤ O for all tenants i ≠ j: "The number of shared ECSs between two tenants should not exceed O. The value of O is determined by operators." (Eq. 6).
   - The paper proves no isolation property.
2. **CATCHME (DSN 2014) and MTDDOS (Computer Communications 2014): a rigorous combinatorial model of *shuffling*, for a different scheme.** Each client is assigned to exactly one replica or proxy. Attacked replicas are replaced and their clients re-shuffled over many rounds.
   - The core formula is hypergeometric. Replica i holds x_i of N clients. The probability that it holds none of the M persistent bots is p_i = C(N − x_i, M) / C(N, M). The expected number of benign clients saved is E(S) = Σ_i p_i·x_i [CATCHME §IV-A "Theoretical Problem Modeling", Eq. 1, PDF p. 6].
   - MTDDOS derives the same formula "According to simple combinatorics" [MTDDOS §4.3 "Problem Modeling", Eq. 4.1, author-copy PDF p. 5].
   - CATCHME finds the optimal assignment by dynamic programming [§IV-B, PDF p. 6]; MTDDOS uses a greedy algorithm [§5.1].
   - **Relevance:** this is the peer-reviewed precedent for the hypergeometric reasoning in §8.2. Its model, a partition refined adversarially over rounds, is not AWS's static overlapping subsets.
3. **MOTAG (ICCCN 2013)** is the predecessor proxy-shuffling paper. Only the bibliographic record was confirmed; its content is **UNVERIFIED**.
4. **COPYSETS (USENIX ATC '13)** gives the *dual* combinatorics: how the number of distinct replica sets drives the probability of loss under random simultaneous failures. It is covered in note 04 §A6.1 and extended to EC stripes in note 04 §A6.3. The contrast is in §8.4.
5. **SFQ-BLOG (NON-PEER-REVIEWED)** combines shuffle sharding with McKenney's 1990 Stochastic Fairness Queuing and best-of-two:
   - "Map each customer to a subset of the queues (that subset could be only two)".
   - Put each request "in the shortest queue in their subset".
   - "Periodically perturb the subsets".

   It claims "strong isolation of noisy neighbors from other customers (assuming only a relatively small portion of customers are noisy neighbors)". McKenney's paper was not reviewed. Best-of-two is covered in note 04 §A8.

**Conclusion.** The math of shuffle sharding is elementary combinatorics: counting k-subsets and the hypergeometric distribution. It is derived below (**DERIVED**) and checked against AWS's published numbers. AWS's parameters and claims stay NON-PEER-REVIEWED.

### 8.2 The model (DERIVED)

**Setup.**
- There are n workers: gateways, queues, rate-limiter slots.
- Each tenant t has a shard S_t ⊂ {1..n} with |S_t| = k.
- *Stateless* shards are modeled as independent, uniformly random k-subsets. Infima's hash-seeded shuffle approximates this [INFIMA].
- A **poison tenant** P makes every worker in S_P unavailable, through a crash-inducing request, a flood or a hot key.
- For any other tenant V, let X = |S_V ∩ S_P|.

**Formulas.**

1. **Identical shards.** P(S_V = S_P) = 1 / C(n,k). Among T tenants, the expected number sharing P's exact shard is (T − 1) / C(n,k).
2. **Overlap distribution.** X is hypergeometric: P(X = m) = C(k,m)·C(n−k, k−m) / C(n,k) for m = 0..k, with E[X] = k²/n.
3. **What V experiences:**

   | Overlap | V's state |
   |---|---|
   | X = 0 | unaffected |
   | 1 ≤ X ≤ k−1 | partially degraded; survives only if its client retries elsewhere in S_V |
   | X = k | fully down; this is the "1/C(n,k)" of BL-SHUFFLE and the INFIMA README |

4. **Retry budget.** A client with r retries tries r + 1 distinct endpoints of S_V in random order.
   - All of them fail with probability C(X, r+1) / C(k, r+1) when r + 1 ≤ X, and 0 otherwise.
   - So r + 1 = k ("try every endpoint in a Shuffle Shard" [SS-BLOG]) is exactly the condition under which every V with X < k survives. SS-BLOG's sizing, "With 3 retries ... we can use four instances in total per shuffle shard", is this rule.
   - With k > r + 1, a tenant with X ≥ r + 1 can spend its whole budget on failed endpoints. A larger k also raises E[X], so more tenants are partially degraded.
5. **Several poison tenants.** Let U be the union of their shards. V is fully down iff S_V ⊆ U, with probability C(|U|, k) / C(n,k). The worst case is j disjoint poison shards, |U| = jk.
6. **Overlap guarantee.** This is Infima's stateful sharder and BL-SHUFFLE's Route 53 promise: |S_a ∩ S_b| ≤ t for every assigned pair, with t < k.
   - **Protection.** No single poison tenant can fully cover another tenant. It takes at least ⌈k/t⌉ poison tenants, and more if their shards overlap each other. For Route 53's k = 4, t = 2 that is 2.
   - **Capacity cost.** Infima records each shard's (t+1)-subsets and forbids reusing them. Each shard uses up C(k, t+1) of the C(n, t+1) available (t+1)-subsets, so at most **C(n, t+1) / C(k, t+1)** shards can ever be assigned. This is the packing bound. A greedy backtracking search can stop sooner: Infima's Javadoc example stops at 2 shards where the bound allows 3.
   - **Link to block designs.** For t = 1 the bound is n(n−1) / (k(k−1)). A family that reaches it covers every pair of workers exactly once: a balanced incomplete block design with λ = 1. COPYSETS §7.1 uses the same object [p. 47]: an "(N, R, λ) BIBD" in which "Every pair of nodes is contained in exactly λ copysets", with (7,3,1) and (9,3,1) examples. There it is the *minimum* number of copysets that still gives full scatter width.
   - So one λ = 1 design is both the largest shuffle-shard family with pairwise overlap ≤ 1 and the smallest copyset family with full scatter width.

### 8.3 Numbers (DERIVED; computed with Python's `math.comb`)

**Checks of the published figures:**

| Published figure | Source | Check |
|---|---|---|
| 28 shards of 2 from 8 workers; impact "1/28th"; "7 times better" than 4 plain shards | BL-SHUFFLE p. 6 | C(8,2) = 28; 28/4 = 7. ✓ |
| "56 potential shuffle shards" (2 of 8); impact "1/1680" (4 of 8) | SS-BLOG, "Shuffle Sharding" | These are the ordered counts P(8,2) = 56 and P(8,4) = 1,680. The unordered counts that govern isolation are 28 and 70. ✗ |
| "730 billion possible shuffle shards" (4 of 2048) | BL-SHUFFLE p. 6 | C(2048,4) = 730,862,190,080. ✓ |
| "over 300,000" four-card hands from 52 cards + 2 jokers; P(exactly 1 match) "less than a 1/4"; P(2) "less than a 1/40"; P(3) "much less than a 1/1000" | SS-BLOG, introduction | C(54,4) = 316,251. The three probabilities are 0.2479, 0.0232 and 0.00063. ✓ (the last is below 1/1000 by only 1.6×) |
| No two Route 53 domains share more than 2 of their 4 servers | BL-SHUFFLE p. 6 | Packing bound C(2048,3)/C(4,3) = 357,389,824 domains at most, against 7.3×10¹¹ unconstrained shards |

**Isolation for pool sizes relevant to mantle.** Shards are uniform and stateless, with one poison tenant. The last column assumes two poison tenants with disjoint shards.

| n | k | C(n,k) | P(X = k) = 1/C(n,k) | P(X ≥ 1): degraded or worse | P(X ≥ k−1) | E[X] = k²/n | P(V covered by 2 poison shards) |
|---|---|---|---|---|---|---|---|
| 8 | 2 | 28 | 3.6×10⁻² | 0.464 | 0.464 | 0.50 | 0.214 |
| 8 | 4 | 70 | 1.4×10⁻² | 0.986 | 0.243 | 2.00 | 1 |
| 16 | 2 | 120 | 8.3×10⁻³ | 0.242 | 0.242 | 0.25 | 0.050 |
| 16 | 4 | 1,820 | 5.5×10⁻⁴ | 0.728 | 0.027 | 1.00 | 0.038 |
| 64 | 2 | 2,016 | 5.0×10⁻⁴ | 0.062 | 0.062 | 0.06 | 0.003 |
| 64 | 4 | 635,376 | 1.6×10⁻⁶ | 0.233 | 3.8×10⁻⁴ | 0.25 | 1.1×10⁻⁴ |
| 256 | 4 | 174,792,640 | 5.7×10⁻⁹ | 0.061 | 5.8×10⁻⁶ | 0.06 | 4.0×10⁻⁷ |

**Full overlap distributions,** P(X = 0), …, P(X = k):

| n | k | Distribution |
|---|---|---|
| 8 | 2 | 0.536, 0.429, 0.036 |
| 8 | 4 | 0.014, 0.229, 0.514, 0.229, 0.014 |
| 16 | 4 | 0.272, 0.484, 0.218, 0.026, 0.0005 |
| 64 | 4 | 0.767, 0.215, 0.017, 0.0004, 1.6×10⁻⁶ |

**What the tables show:**

- **1/C(n,k) counts only the tenants that go fully down.** In BL-SHUFFLE's own example (n = 8, k = 2), 46% of tenants lose one of their two workers when the poison pair fails. The "1/28th" holds only because their clients fail over.
- **Small pools give weak isolation.** With 8 workers and k = 4:
  - two poison tenants can cover every tenant;
  - one poison tenant degrades 99% of tenants.
- **The combinatorics start to pay at dozens of workers.** At n = 64, k = 4:
  - one poison tenant fully covers about 1.6 tenants per million;
  - two cover about 1 in 10,000 (disjoint worst case);
  - one tenant in four still has a degraded shard.

### 8.4 The dual problem: copysets (short contrast; details in note 04)

- **The two techniques pull in opposite directions.**
  - Shuffle sharding *maximizes* the number of distinct sets. The failure it isolates is *correlated and tenant-induced*: the poison tenant takes down exactly its own set.
  - Copysets *minimize* the number of distinct sets, to cut the probability of *any* loss under *random simultaneous* failures. The price is rarer but larger losses, and less scatter width, which means slower rebuilds [COPYSETS; note 04 §A6.1]. The same holds for EC stripe sets (note 04 §A6.3).
- **Tectonic takes the copyset side for placement.** It keeps about 100 consistent disk shuffles and cuts copysets from contiguous disks (note 01 §1.10; note 04 §A7.2).
- **The two do not conflict, because they govern different resources:**
  - Chunk placement faces *random* correlated failures, such as power events and disk failures. It wants few, fixed, failure-domain-aware copysets.
  - Request-serving resources (gateways, queues, rate limiters) face *tenant-correlated* failures. They want many overlapping shards.
  - The one mathematical object they share is the λ = 1 block design of §8.2.

#### 8.4.1 Overlap-limited replica sets for quorum stores (patent evidence; NON-PEER-REVIEWED)

The two techniques meet in one place: a store whose replicas answer by quorum. An Amazon patent describes exactly that case.

- **Source and status.** Amazon Technologies, Inc., "Cell-based storage system with failure isolation" [AMZN-PAT]. A patent discloses a method. It is not evidence that any AWS service uses it, and the text names no service: "DynamoDB" and "S3" do not occur in it.
- **The store it describes.** "storage resources used to implement a distributed, multi-tenant data store may be partitioned such that a failure for one partition may not make other partitions inaccessible. the data store may store tables, and tables may include partitions." Each partition has "a set of replicas (e.g., three replicas)", and "a quorum consistency model may be used to determine the results of access requests from clients (e.g., using two of three replicas)."
- **The technique.** "storage nodes may be assigned to partitions or other data objects using a shuffle sharding technique 110 to implement a cell-based architecture ... if three nodes are selected for a given partition, then the overlap may be limited to no more than one storage node in common with any other given partition."
- **The claimed benefit.** "if a first partition becomes "hot" and begins to experience a very large quantity of access requests, but the partition has only one storage node in common with a second partition based on shuffle sharding, then the second partition may remain accessible using its other storage nodes based on the quorum consistency model."
- **Claim 1** selects a second node subset "based at least in part on a membership of the first subset", with at least one node the first subset lacks. Requests are routed "using the one or more partition maps".

**DERIVED: the quorum rule.** Take replica sets of size k and a q-of-k quorum. Bound the pairwise overlap between distinct sets by t ≤ k − q. Then even if every node of one set fails, every other set keeps at least q live members.

- For k = 3, q = 2, the bound is t ≤ 1, the patent's example.
- For k = 5, q = 3, the bound is t ≤ 2.

**DERIVED: the capacity cost.** The packing bound of §8.2 item 6 caps the number of distinct sets at C(n, t+1)/C(k, t+1). For n = 100 nodes and k = 3, t = 1, that is 1,650 sets.

- A store with more ranges than that must put several ranges on the same set. Ranges on one set share its fate.
- So the blast radius of a poison or hot range is the ranges placed on its own set. No other set drops below quorum.

**DERIVED: reconciling this with copysets.**

- For durability, copysets want *few* distinct sets (§8.4). For isolation, the overlap bound wants no two sets to share more than t nodes. A family can satisfy both at once: a small family of 3-node sets with pairwise overlap ≤ 1 is a partial Steiner triple system. The λ = 1 design of §8.2 item 6 is the case with full scatter width.
- Tiered Replication's greedy copyset builder already applies a "didNotAppear" check that minimizes pairwise overlap between copysets (note 04 §A6.2).

### 8.5 Implications for mantle (INFERENCE)

1. **Where shuffle sharding belongs:** stateless or interchangeable request-serving resources inside a cell [CELL-WP p. 49]:
   - bucket → k of the gateway processes;
   - tenant → k of n admission queues in the gateway and metadata nodes, with each request going to the shortest queue in the subset [SFQ-BLOG; note 04 §A8];
   - tenant → k of n rate-limiter or token-bucket slots.

   With virtual-hosted-style addressing (note 05 §14), a bucket's DNS name can resolve to its own shard of gateway addresses, which is SS-BLOG's per-customer DNS pattern. Path-style requests share a single hostname, so DNS cannot shard them.
2. **Where it does not belong:**
   - **Chunk placement** uses copysets across failure domains (§8.4; note 04 §A6-A7).
   - **Metadata ranges.** Each key has exactly one authoritative Raft group, so ownership cannot be shuffle-sharded. The *placement* of each range's replica set can still be overlap-limited, so that one range's failure cannot take another range below quorum (§8.4.1). Tenant isolation there comes from per-tenant admission control (Tectonic's TrafficGroups, note 01 §1.11) and from splitting hot ranges (§3, §7). CELL-WP's own caveat: it is "trickier for stateful components" [p. 49].
3. **Size k from the retry budget:** k = retries + 1 [SS-BLOG; §8.2 item 4]. The client's bounded retry loop (CLAUDE.md rule 2) must walk *distinct* endpoints of its shard and never retry the same one.
4. **Choose n from the table, not intuition.** Below about 16 workers the isolation is weak (§8.3). A laptop or small cluster with one gateway gets nothing from shuffle sharding; there it is a no-op, like the router.
5. **Prefer stateless sharding.**
   - Use stateless hashing (Infima's Simple sharder) by default. It needs no shared state, any gateway or client can compute it, and a per-deployment secret seed defeats targeted collisions [INFIMA Javadoc].
   - Add a stateful maximum-overlap guarantee only for small n, where 1/C(n,k) is not negligible, and within the packing bound C(n,t+1)/C(k,t+1).
   - Keep shard assignments in the control plane's versioned map, next to the bucket → cell map. They are static-stable data.
6. **Pair shuffle sharding with quarantine.** An overlap bound t protects against one poison tenant, not against ⌈k/t⌉ of them (§8.2 item 6). When a poison bucket is detected, use the override table to move it to a quarantine shard or cell [CELL-WP p. 27], as Route 53 moves an attacked domain to dedicated attack capacity [BL-SHUFFLE p. 6].

### 8.6 UNVERIFIED / not found

- **Peer-reviewed analysis of shuffle sharding itself:** none found. The search is not exhaustive (§8.1): dblp was unreachable and Google Scholar was not queried.
- **Unread papers:** MOTAG's content and McKenney's 1990 SFQ paper.
- **AWS production practice** is **UNVERIFIED**:
  - Route 53's assignment algorithm;
  - the mapping from virtual to physical name servers;
  - whether and where S3 uses shuffle sharding.
- **Limits of the §8.2 model.** It assumes shards are uniformly random and independent, and that a poison tenant fails exactly its own shard. Real overload can also spill onto shared downstream dependencies, which the model does not capture.

---

## 9. Mapping to mantle: a proposed cell design

Everything in this section is **INFERENCE**: a proposal for `docs/design/`, not a decision. Each item cites the facts it rests on, either by this note's section number or by source key. Where a proposal goes beyond every source, it says so.

**The goal it serves.** STATUS.md's planned item 5, "Cells", is: "A replicated map of which cell owns each key range, routing from cached copies of it with redirects after a range moves, moving a range between cells while it is read and written, and adding and retiring cells. Done when ranges move between cells under a mixed workload with no lost write and no stale read, and failing or upgrading one cell leaves the requests of every other cell unaffected."

### 9.1 What "cell" means in the sources, and in mantle

The sources use one word for two different units.

| Unit | System | What it is | Size, as stated | Where |
|---|---|---|---|---|
| Consensus cell | Physalia | One Paxos group per partition key (an EBS volume). Cells never coordinate. | 7 nodes per cell; "millions" of cells | §1.2-§1.3 |
| Workload cell | AWS guidance | "A complete workload, with everything needed to operate independently", behind a thin router | "a fixed maximum size", per service | §5.2, §5.7 (NON-PEER-REVIEWED) |
| Storage stamp | WAS | Racks of storage nodes with a stream layer, a partition layer and front-ends. A Location Service maps accounts to stamps. | 10-20 racks × 18 nodes; 2 PB, later 20-30 PB | §7.4.1-§7.4.2 |
| Partition (mini-SM) | Shard Manager | A set of servers and the shards placed on them. "The replicas of a shard are always placed on servers that belong to the same partition." | "thousands of servers and hundreds of thousands of shard replicas" | §7.3.6 |
| Job in a datacenter | Slicer | Scope of one assignment; a global load balancer picks the datacenter | per job | §7.1.1 |
| Zone | Spanner | "the unit of administrative deployment" and "the unit of physical isolation" | 1 zonemaster, 100 to several thousand spanservers | §7.9 |
| Cluster | Tectonic | "the top-level Tectonic deployment unit", datacenter-local, with no layer above it | "a single cluster can span an entire datacenter" | [TEC §1, p. 217; §3.1, p. 218] |
| (none named) | S3, DynamoDB | S3 names Regions and AZs as isolation units. DynamoDB's paper never uses "cell". | not published | §6.4, §3.1 |

**Proposal: mantle's vocabulary.**

- A **range** is one Raft group over a contiguous span of one metadata layer. It is Physalia's cell: the unit of consensus, placement and failure isolation (§1.11 P1).
- A **cell** is STATUS item 5's unit: a complete mantle stack that owns a set of key ranges. It is a WAS stamp, an AWS workload cell, or an SM partition.
- The word "cell" should not be used for ranges.

### 9.2 The central trade-off: one exabyte cluster, or many cells

**The peer-reviewed case against cells.**

- Tectonic replaced "tens of HDFS clusters per datacenter" with one cluster per datacenter. Its "exabyte scale eliminated the bin-packing and dataset-splitting problems" [TEC §2.2, p. 218].
- It names WAS as a federated design and states the cost: "Federated systems still have the operational complexity of bin-packing datasets (§2). Also, migrating or sharing data between instances, e.g., to load-balance or add storage capacity, requires resource-heavy data copying among namespaces" [TEC §7, p. 228].
- Tectonic isolates tenants inside one cluster with TrafficGroups and disk-time accounting (note 01 §1.11), not with separate stacks.

**The case for cells.**

- AWS's guidance is qualitative. Its benefits are "Workload isolation", size caps that can be tested, and deployment waves. Its stated targets are "excessive load of resources and deployments with problems or bugs". It reports no data (§5.3, NON-PEER-REVIEWED).
- Physalia's peer-reviewed argument is about consensus groups, not deployment stacks. Many small, independent, well-placed groups limit a single failure's reach (§1.4). Colors limit a deployment's reach (§1.6).
- WAS runs stamps in production. It migrates accounts when a stamp reaches 70% utilization, and it requires an account to fit in one stamp (100 TB) (§7.4.1).
- SM makes partitions large enough that "the average load in different partitions does not diverge much". Moves between partitions are rare and driven by tools (§7.3.6).

**Proposal.** The two cases address different failures, so mantle should take something from each.

1. **Make cells large.** A cell should be closer to a Tectonic cluster or an SM partition than to a 2011 WAS stamp. Bin-packing and cross-cell copying are then rare events, not routine operations [TEC §7].
2. **Build the cell map and the cross-cell move from day one.** "Start with a cell migration mechanism from day one" [CELL-WP p. 47]. STATUS item 5 requires both.
3. **Get most blast-radius control inside a cell,** with mechanisms that cost no capacity:
   - independent ranges with anti-correlated replica sets (§1.4);
   - deterministic commands that resist poison pills (§1.6);
   - per-tenant admission control (§3.6);
   - shuffle-sharded request resources (§8.5);
   - deployment colors or waves (§1.6, §5.10).
4. **Add cells when a cell reaches its stated bound (§9.3), or when a policy requires separation:** a dedicated tenant (§5.7) or an AZ-scoped offering (§6.5).

### 9.3 Structure and cell size

**Three levels.**

1. **Region:** a set of cells, the cell map, and the endpoint (DNS plus a thin router).
2. **Cell:** gateways, metadata ranges for the Name, File and Block layers (note 01 M1), chunk stores, and the cell's control plane: a placement driver ("assigner"), repair, rebalancer and GC.
   - Raft replicas and EC stripes never span cells. SM keeps a shard's replicas inside its partition (§7.3.6). WAS scopes intra-stamp replication to the stamp (§7.4.3). CELL-WP forbids shared resources between cells (§5.2).
3. **Range:** a Raft group over a contiguous key span of one metadata layer (note 06 §A4).

**State the cell's bound in the units its control plane pays for, and refuse beyond it** (CLAUDE.md rule 2). The sources size cells by their control plane:

- WAS keeps a stamp's stream metadata within "50 million extents and no more than 100,000 streams", which fits in 32 GB. It derives its partition watermark from that bound (§7.4.1).
- SM's largest mini-SM manages about 50K servers and 1.3M shards on one 18-core, 64 GB machine. Its solver took 205 s for 375K shards on 5K servers (§7.3.5, §7.3.7).
- CELL-WP requires that a cell be "Small enough to test at full scale" (§5.7).

For mantle, the bound should cover:

- ranges per cell;
- placement-map entries (chunks, which Tectonic records explicitly, note 01 §1.10);
- the placement driver's solve time, measured;
- the size at which mantle can still load-test a whole cell.

A further input is the **repair budget.** ShardStore treats crash consistency as protection against fleet-wide repair traffic (§2.3). A cell's repair bandwidth must rebuild its largest failure domain within the durability target (note 04 §A5-§A6).

**The laptop is "cell zero"** [CELL-WP p. 47]: one cell, one node, and a cell map with one entry covering the whole key space. The same code runs, and the router and the cross-cell mover have nothing to do.

### 9.4 The cell map

**Key it by range, and default to one range per bucket.**

- STATUS item 5 asks for "which cell owns each key range".
- WAS keys by account and caps account size (§7.4.1-§7.4.2).
- CELL-WP keys by a partition key found in every request, and adds "a second dimension" for tenants that outgrow a cell (§5.5, §5.7).
- Spanner moves prefix-defined directories, and moves *fragments* of directories that grow too large (§7.9).
- S3's index partitions are ranges of `bucket/key` (§6.2).

Proposal:

- The cell map is a range map over the ordered Name-layer key space (bucket, key).
- The common entry covers a whole bucket. That keeps WAS's per-tenant isolation and a single-lookup route for almost every request.
- A bucket's range is split across cells only when the bucket outgrows a cell. This is CELL-WP's second dimension and Spanner's fragments, and it removes WAS's 100 TB cap.

**Use an explicit table with an override table, not hashing.**

- Full mapping gives "More control over distribution to control hot cells and to perform a cell migration" (§5.5).
- Slicer replaced load-aware consistent hashing after 18 months (§7.1.7).
- Dynamo's best strategy decoupled partitioning from placement (§7.7.2).
- The map stays small. Centrifuge stores 32 B per range (§7.2.3); Akkio uses at most a few hundred bytes per µ-shard (§7.8.3).

**Versioned, compare-and-swapped, and stored outside the object path.**

- Each entry is (range, cell, epoch), and the map has a version.
- Only the control plane changes it, by compare-and-swap on the version. Slicer's Assigners converge the same way (§7.1.4).
- It lives in a small replicated control-plane range, never in objects served through mantle's own S3 path. That avoids FIB's circular dependency (§5.13 item 5) and DynamoDB's metadata-in-itself bootstrap problem (§3.12 I13).

**Distribute it with constant work, and keep the last copy.**

- Routers and gateways receive full snapshots on a fixed loop (BL-CONSTANT, BL-SMALLER; §5.4). DynamoDB's partition-map cache refreshes on every hit so that metadata load does not depend on cache state (§3.5).
- The last snapshot keeps serving while the control plane is down (§5.4; Slicer §7.1.5).
- It must also be readable at *startup* without the control plane. Slicer's service-independent mode leaves restarted tasks unable to initialize. SM closes that gap by reading assignments from ZooKeeper (§7.3.9 item 2). mantle should persist the last snapshot locally and be able to read the map range directly.

**Cross-cell operations** go through the router, never cell to cell (§5.5; §5.13 item 2):

- ListBuckets;
- CopyObject and UploadPartCopy whose source is in another cell;
- LIST over a bucket split across cells.

S3's LIST is ordered (note 05 §6.1), so a listing that crosses a cell boundary proceeds range by range, and each page is served by one cell.

### 9.5 Routing, level by level

| Hop | Mechanism | On a stale route | Evidence |
|---|---|---|---|
| Client → cell | Virtual-hosted-style: per-bucket DNS name → the cell's gateway addresses, optionally a shuffle shard of them. Path-style, dotted bucket names, and buckets split across cells: a thin L7 router that parses the bucket (and the key when needed) from an in-memory map, with no SigV4 checks and no metadata reads. | The cell answers with a redirect carrying the owning cell and the map epoch. The router refreshes and retries once, within a bounded budget. | WAS DNS + LS (§7.4.2); CELL-WP router rules (§5.6); SS-BLOG per-customer DNS (§5.12); S3 DNS + TemporaryRedirect (§6.4); note 05 §14 |
| Gateway → range | Cached range descriptors carrying a generation and a membership epoch; `floor(key)` lookup; constant-work refresh. | The replica answers with its newer descriptor or a typed stale-route error, never with data. | DynamoDB MemDS (§3.5); TiKV epoch (note 06 §A4.4); CRDB generation (§7.6.3); Physalia (§1.8); Centrifuge "hints" (§7.2.3); Akkio (§7.8.3) |
| Range → chunks | Explicit Block-layer locations; an epoch for unsealed blocks. | The chunk store refuses appends at a stale epoch. | Tectonic (note 01 §1.10); Aurora (§7.5.5); §7.7.4 items 12, 14 |

**The one invariant across all three hops:** stale routing may cost liveness but never correctness. Physalia model-checked exactly this for its discovery cache (§1.8). Every owner must be able to reject: a replica checks the epoch, the source cell keeps a tombstone, the chunk store checks the block epoch.

### 9.6 Moving data

| What moves | Why | Protocol | Fencing token | Evidence |
|---|---|---|---|---|
| Range leadership (lease) | balance, drain, deploy | Raft leadership transfer. During the handoff the old leaseholder forwards or redirects in-flight requests. The directory is published, and the old role is dropped when idle. | lease sequence | SM graceful migration: ≈100% success vs ≈98% without it (§7.3.4); DDB relinquish-before-deploy (§3.10); note 06 §A4.3 |
| Range replica | repair, balance, drain | Add a learner, catch up by snapshot and log, run joint consensus, remove the old replica. One replica at a time. | membership configuration in the log | Physalia one node at a time (§1.7); note 06 §A1.7; CRDB's exact sequence is not in the paper (§7.6.1) |
| Range split | size, or sustained heat at an observed split key | A Raft command in the parent's log. Both children stay on the parent's replicas, the directory is updated, and only then may a child move. Skip single-key and sequential-key heat. | descriptor generation + 1 | WAS (§7.4.7); TiKV (note 06 §A4.4); DDB (§3.8); S3 (§6.2.5) |
| Range merge | cold ranges near the high watermark | Align replica sets, freeze the right-hand range, require every right-hand replica to acknowledge. Rare, and always abandonable. | generation + 1 | CRDB merge tech note (§7.6.3, NON-PEER-REVIEWED); WAS (§7.4.7) |
| Unsealed block's chunk replicas | repair, heat | Aurora-style overlapping quorum sets under a membership epoch; reversible and non-blocking. | block epoch at a write quorum | §7.5.5; §7.7.4 item 12 |
| Sealed chunk | balance, drain | Copy, verify the checksum, compare-and-swap the Block-layer entry, delete after the grace period. | Block-entry CAS | §7.7.4 item 15; chunk-store.md §8 |
| Key range between cells | cell full, hot cell, retiring a cell | Below | cell-map epoch | CELL-WP (§5.9); WAS (§7.4.3); Spanner Movedir (§7.9); Akkio (§7.8.4-§7.8.5) |

**Moving a key range between cells: a proposed protocol.**

It combines four sources:

- CELL-WP's four phases: clone, flip, redirect, forget (§5.9);
- Spanner's background copy followed by an atomic cutover of the "nominal amount" (§7.9);
- WAS's "clean failover" with no data loss (§7.4.3);
- Akkio's fenced, resumable mover (§7.8.4-§7.8.5).

No source publishes the fencing step, so the steps below go beyond all of them. They need a TLA+ model before code (§9.10).

1. **Record.** The control plane writes a migration record: range, source cell, target cell, epoch e, and a mover sequence number. The record serializes movers, and a restarted mover takes over by bumping the sequence number, as Akkio's DPS does (§7.8.5).
2. **Prepare.** The target cell creates non-authoritative ranges for the span.
3. **Copy in the background while the source serves.** Object metadata and chunk data are copied into the target's own placement, and deltas are repeated until the remainder is small.
   - Chunks get target-cell locations. Block and File rows are re-created in the target, because no replica may span cells.
4. **Freeze.** The source range commits `frozen(e+1)` in its own Raft log. It refuses writes to the span with a retryable 503 and keeps serving reads. Reads stay correct because no writes are accepted anywhere.
5. **Final catch-up.** The target copies the last delta.
6. **Hand off.** The source commits `handed_off(e+1, target)`. From then on it refuses reads and writes for the span with a redirect, and keeps that tombstone (§5.9 phase 3; Physalia's forwarding pointers, §1.8).
7. **Flip.** The control plane compare-and-swaps the cell-map entry to the target at e+1. The target starts serving only once it has observed the new entry.
8. **Forget.** The source deletes the span after a grace period (§5.9 phase 4; chunk-store.md §8).

**DERIVED from the steps:**

- **No lost write.** The source accepts no writes after step 4, and the target accepts none before step 7.
- **No stale read.** The source serves no reads after step 6, and between steps 4 and 6 its state equals the target's.
- **Writes are unavailable from step 4 to step 7, reads from step 6 to step 7.** Both windows need a stated bound and a measurement. WAS's split and merge pause only "briefly", and the length is **UNVERIFIED** (§7.4.7).

**Operating rules for every mover** (Physalia's "don't move faster than the expected rate of change" and big red button, §1.6; Akkio's per-unit rate limit, §7.8.4; S3's "Control plane limits", §6.7):

- a per-range rate limit;
- a global rate limit;
- an operator stop switch.

AWS's model checking found a design bug in DynamoDB's data-migration feature (§4.3). Model this protocol first.

### 9.7 Balancing, scaling up and scaling down

**Balancing inside a cell.**

- **Compute explicit assignments starting from the current one,** with a churn budget in *bytes moved per round*. Slicer spends 9% of the keyspace per round, ranking moves by imbalance reduction per unit of churn (§7.1.6). SM runs a local search with time and move budgets (§7.3.5).
- **Keep two modes.** An emergency mode repairs lost redundancy under hard constraints and may worsen balance. A periodic mode optimizes and must not worsen it (SM, §7.3.5).
- **Let nodes propose; let one placement authority decide** (DynamoDB, §3.7).
- **Suppress rebalancing when no node is at risk.** Slicer uses 25% CPU, "an arbitrary threshold" (§7.1.6). mantle measures its own.
- **Move leadership before replicas,** because it is cheap and reversible (CockroachDB's current allocator, §7.6.2, NON-PEER-REVIEWED).
- **Do not chase request locality by default.** CockroachDB's Follow-the-Workload was "rarely used" (§7.6.1).
- **Balance heat, not only bytes.** Use Tectonic's disk-time accounting (note 01 §1.11) and S3's HDD arithmetic (§6.7, NON-PEER-REVIEWED).
- **Ranges per node: low and high watermarks, derived from measured per-group overhead** (note 07 §7.3).
  - WAS keeps about 10 partitions per partition server so that a failed server's load spreads widely (§7.4.7).
  - DynamoDB nodes host "thousands" of replicas (§3.7).
  - Slicer targets 50-150 slices per task (§7.1.6).
  - These are cited starting points, not measurements.

**Scaling up.**

- **Nodes join a cell** and are filled under the churn budget. Moving whole ranges and chunks as units avoids Dynamo's "almost a day" bootstrap, where donors had to scan for keys (§7.7.2).
- **A new cell** is provisioned, load-tested and registered in the map. New buckets are allocated to it (WAS's LS allocation, §7.4.2). Some ranges may be moved to it when a cell passes its utilization target: WAS's trigger is 70% (§7.4.1), and mantle must state its own.

**Scaling down.**

- **An operation gate approves every restart, drain and decommission** only if:
  - every Raft group keeps a quorum;
  - every EC block keeps its tolerated-loss margin;
  - both counts include replicas that have *already* failed (SM TaskController, §7.3.4).

  On a laptop the gate refuses any operation that would take the only copy offline.
- **Drain a node** in order: move leaderships gracefully; stop new allocations there; evacuate through the repair path; verify redundancy; release. Sources: WAS upgrades (§7.4.10), Aurora heat-as-repair (§7.5.7), §7.7.4 item 16.
- **Retire a cell** in order: disable it for new allocations; move all its ranges out (Akkio's removal of a replica set collection, §7.8.5); drain it; delete it.

**Headroom.**

- Pre-provision for the largest failure domain rather than scaling on failure: +50% for three AZs (BL-STATIC, §5.4, NON-PEER-REVIEWED); WAS's 70% target and 80% ceiling (§7.4.1).
- mantle derives its own figure from the measured size of its largest failure domain (CLAUDE.md rule 4).

### 9.8 Isolation inside a cell

- **Admission control per tenant at the gateway**, with GAC-style vended, time-limited tokens (§3.6) and Tectonic's TrafficGroups (note 01 §1.11).
  - Per-range and per-node caps stay as ceilings (§3.6).
  - Over budget, or while a range is splitting, the answer is 503 SlowDown, which S3 clients already retry with backoff (§6.3).
  - A range whose proposal pipeline is full returns a typed `Busy` (Physalia, §1.6 item 6).
- **Shuffle sharding for request-serving resources:** gateways, admission queues and rate-limiter slots (§8.5).
  - Meaningful isolation needs n of 16-64 or more. Shard size is k = retries + 1 (§8.3).
  - Use stateless hashing with a secret seed.
  - Quarantine a poison bucket through the override table (§8.5 item 6; §5.5).
  - Metadata ranges and chunk placement are *not* shuffle-sharded (§8.5 item 2).
- **Overlap-limited range placement.** Place metadata ranges on a bounded family of replica sets whose pairwise overlap is at most k − q nodes: at most 1 for 3 replicas with 2-of-3 quorums.
  - A poison or hot range can then take down only the ranges on its own set. No other set drops below quorum (§8.4.1, patent evidence NON-PEER-REVIEWED; Physalia's "different mix of cells", §1.4).
  - The same family can be the small copyset family that durability wants (§8.4; note 04 §A6.2). Its size is capped by the packing bound: 1,650 sets for 100 nodes (§8.4.1).
- **Deterministic, fully specified commands.** A replica refuses a command version it does not understand (Physalia, §1.6 item 4; DDB's read-new-then-write-new, §3.10).
- **Deployment blast radius:**
  - canary cell first, then cell by cell (§5.10);
  - within a cell, one failure domain or color at a time, on different days (§1.6, §5.4);
  - the control plane in staged rollouts (SM, §7.3.6).

### 9.9 Control plane and data plane

- **The data plane** is gateways, range replicas and chunk stores. **The control plane** is the cell-map service, each cell's placement driver, the repair and rebalance schedulers, and bucket-configuration APIs. FIB classifies bucket creation and configuration as control plane (§5.4).
- **Rules:**
  1. The data plane never waits on the control plane, including at startup (§9.4).
  2. The control plane fails closed. The data plane serves on stale hints, and owners fence (CELL-WP's CP/AP split, §5.4; Physalia, §1.8).
  3. The smaller fleet sets the pace: snapshots, or long-lived connections the control plane can refuse (BL-SMALLER, §5.4).
  4. Configuration propagates with constant work: a full, fixed-size snapshot on a loop (BL-CONSTANT, §5.4; DynamoDB, §3.5).
  5. Every control-plane action has a rate limit and a stop switch (§1.6, §6.7, §7.1.6, §7.8.4).
  6. The control plane keeps its durable state in mantle's own Raft ranges, in a small root range, not in its own consensus library. At Meta the Paxos library found "only one use case", ZippyDB (§7.3.2).

### 9.10 What to verify before building

- **Formal models (TLA+ or P) first**, as AWS did for DynamoDB replication and migration (§4.3), Physalia for stale-cache safety (§1.9), and S3 for its consistency protocol (§6.6):
  1. range split and merge with descriptor generations and cached routes;
  2. leadership and lease transfer with graceful handoff;
  3. joint-consensus replica moves (focal-raft already ships a TLA+ model of its fast track, note 07 §1.6);
  4. replica-set changes for unsealed blocks under membership epochs;
  5. the cross-cell move of §9.6, including the ABA case of a range leaving a cell and returning (Physalia, §1.8; CRDB generations, §7.6.3);
  6. liveness: moves and splits terminate. An unchecked liveness property let a bug through at AWS (§4.7).
- **Deterministic simulation** that can trigger every pressure point on demand: split, merge, move, crash each role, inject latency (WAS, §7.4.10). The harness pattern is Physalia's SimWorld (§1.9), and ShardStore-style model-based tests cover the chunk store (§2.9).
- **The end-to-end acceptance test is STATUS item 5's done criterion:** move ranges between cells under a mixed workload and check the recorded histories with the linearizability checker planned in note 06 §A6.8.

### 9.11 Decisions the design record must make

1. **Name-layer partitioning.** The options are ordered (bucket, key) ranges, hashed directories, or both behind a bucket-type flag.
   - This note adds evidence to note 01 §6.2 item 1. S3 general-purpose buckets use an ordered, range-partitioned keymap with split-for-heat (§6.2, NON-PEER-REVIEWED). WAS's blob index is range-partitioned in peer-reviewed production (§7.4.10). S3 added hierarchical, unsorted directory buckets as a second type (§6.5).
   - Tectonic's hotspot argument for hashing (note 06 §A4.5) still holds for sequential keys (§6.2.3).
2. **The cell bound:** its units and values, derived from control-plane limits, the repair budget and full-scale testability (§9.3).
3. **Cell-map granularity:** bucket entries only, or sub-bucket ranges from the start. STATUS item 5 implies ranges (§9.4).
4. **Client → cell routing:** per-bucket DNS, an L7 router, or redirects only, and how path-style requests are handled (§9.5).
5. **Multi-AZ or single-AZ cells.** CELL-WP prefers multi-AZ unless the service is zonal (§5.11). S3's directory buckets are zonal (§6.5).
6. **Replicas per metadata range.** Physalia uses 7 for tiny data (§1.3). DynamoDB uses 3 plus log replicas (§3.3). Aurora uses 6 for AZ+1 (§7.5.1).
7. **Log-only (witness) replicas for fast quorum restoration,** as DynamoDB does (§3.3, §3.12 I8). This needs a specification first.
8. **The seal-length rule for unsealed blocks under 2-of-3 acknowledgement** (§7.7.4 item 13).
9. **Shuffle-sharding parameters** for gateways and queues: n, k and the overlap bound (§8.3).
10. **The replica-set family for metadata ranges:** its overlap bound (k − q) and size, and whether it is the same family as chunk copysets (§8.4.1, §9.8).

---

## 10. Consolidated list: what is not known

Each section ends with its own UNVERIFIED list. These are the gaps that matter most for mantle's design, grouped by what they block.

**What AWS has not published about S3** (§6.9, §5.14):

- partition boundaries, split thresholds, split latency, and whether partitions ever merge;
- how index partitions are replicated;
- how the consistency witness is sharded, replicated and recovered;
- how directory buckets partition directories;
- where object-to-disk locations live;
- whether S3 uses anything it calls a cell.

The AWS cell whitepaper names no service for any pattern it describes, and reports no data for its availability claims.

**Mechanisms that no source publishes in full:**

- the fencing step of a cross-cell move:
  - WAS's "clean failover" steps (§7.7.5);
  - CELL-WP's "This will be system-dependent" (§5.9).
- how long WAS's split and merge pause traffic ("briefly", §7.4.7);
- how Shard Manager guarantees at most one primary per shard (§7.3.10);
- CockroachDB's learner → snapshot → joint-configuration sequence for a move, which is in neither the paper nor the current docs (§7.6.1).

§9.6 proposes a cross-cell protocol, and it must be model-checked before code.

**Tuning constants with no measured basis:**

- Slicer's 9% churn budget, 1% merge budget, 50-150 slices per task and 25% suppression threshold ("not measured sensitivity rigorously", §7.1.6);
- SM's 90% and 10% goals, which are examples only (§7.3.5);
- WAS's 70% utilization target, which is specific to its hardware (§7.4.1);
- DynamoDB's split and move thresholds, which are not published (§3.13).

mantle must measure its own (CLAUDE.md rule 4).

**Shuffle sharding:**

- no peer-reviewed analysis of the technique itself was found, and the search was not exhaustive (§8.1, §8.6);
- Route 53's production assignment algorithm is not published;
- whether any AWS service uses the patented overlap-limited replica sets is unknown (§8.4.1).

**Other open items:**

- Physalia's cell counts, color assignment and discovery-cache parameters (§1.12);
- ShardStore's shard and extent sizes, acknowledgement point and checksum scheme (§2.10);
- whether DynamoDB is cellular, how its splits and moves execute, and its GAC internals (§3.13);
- what S3's two TLA+-specified algorithms do (§4.9);
- Aurora's epoch persistence and its quorum-transition proof (§7.7.5);
- the production values of Dynamo's Q and T (§7.7.5);
- whether Tectonic's own services, beyond ZippyDB, use Shard Manager (§7.3.10);
- Akkio's relation to Tectonic's ZippyDB deployment (§7.8.7).

---

## Appendix A: quantitative quick reference

Numbers exactly as the cited sources state them, grouped by section. Labels from the section apply: rows marked NON-PEER-REVIEWED or DERIVED carry that status.

### §1 Physalia, §7.8 Akkio, §7.9 Spanner, §8.4.1, and Tectonic cluster scope

| Quantity | Value | Source |
|---|---|---|
| Physalia cell size (EBS) | 7 nodes per cell, Paxos; "durable to at least four disks"; durability "around 5000x higher" than 2-replication | [PHY §2.2, p. 466] |
| Physalia reconfiguration pipeline window α | "typically 3" log positions | [PHY §2.4, p. 467] |
| Physalia cell move | one node at a time; "typically allowing movement to complete within a minute" | [PHY §2.4, p. 467] |
| Physalia lease clock assumption | incorrect only if "the fastest node clock is advancing at more than three times the rate of the slowest clock" | [PHY §2.3, p. 467] |
| Physalia placement heuristic (simulation) | 20 candidates per cell; up to 4x lower probability of losing availability vs a single-point database | [PHY §5.2, p. 473] |
| Physalia deployment | "over 60 availability zones"; AZ-scale deployments "routinely serve thousands of requests per second" | [PHY §5.1, p. 472] |
| Physalia latency (typical installation) | reads p99 < 10 ms; writes typically < 50 ms | [PHY §5.1, p. 472; Fig. 10] |
| Physalia availability improvement | p = 7.7x10^-5 (Fig. 8); internal error-rate goal 0.05% (Fig. 9) | [PHY §5.1, p. 472] |
| Physalia load under agg failures (simulation) | rises linearly to a maximum of 29%, then falls | [PHY §5.2.1, p. 473] |
| 2011 EBS event that motivated Physalia | 13% of EBS volumes in one AZ unavailable | [PHY §1.1, p. 464] |
| SimWorld test size | < 10 lines of Java, < 100 ms per packet-loss test; "hundreds" of tests | [PHY §4.1, p. 471] |
| Akkio managed data | "∼100PB"; in production since 2014 | [AKKIO Abstract, p. 445] |
| Akkio unit sizes | shards 1–10s of GB (avg. 2 GB in Fig. 1); µ-shards a few hundred bytes to a few MB (avg. 200 KB) | [AKKIO §1, p. 446; Fig. 1] |
| Akkio location DB | read/write ratio > 500; ≤ a few hundred bytes per µ-shard | [AKKIO §4.4, p. 452] |
| Akkio migration rate limit (sample policy) | once every 6 hours per µ-shard | [AKKIO §4.6.2, p. 453] |
| Akkio write retries during ACL-based migration (ViewState) | 0.007% of accesses | [AKKIO §4.6.3, p. 454, footnote 7] |
| Akkio migration rate, ViewState | ~5% of reads and writes remote; ~20,000 migrations/s | [AKKIO §5.2.1, p. 455] |
| Akkio migration rate, AccessState | ~0.4% of reads remote; ~1,000 migrations/s | [AKKIO §5.2.2, p. 456] |
| Spanner zone size | one zonemaster and 100 to several thousand spanservers | [SPN §2, p. 252] |
| Spanner directory move | "a 50MB directory can be moved in a few seconds" | [SPN §2.2, p. 253] |
| Tectonic cluster scope | one cluster "can span an entire datacenter"; exabytes per cluster | [TEC §1, p. 217; §3.1, pp. 218-219] |
| Overlap-limited replica sets (patent example) | 3 replicas, 2-of-3 quorum, pairwise overlap ≤ 1 node (NON-PEER-REVIEWED) | [AMZN-PAT] |
| Distinct 3-node sets with pairwise overlap ≤ 1 on 100 nodes | at most C(100,2)/C(3,2) = 1,650 (DERIVED) | §8.4.1 |

### §2 ShardStore

| Quantity | Value | Source |
|---|---|---|
| ShardStore implementation size | 44,048 lines of Rust ("over 40,000") | SS Fig. 6, p. 847; §1, p. 836 |
| Unit and integration tests | 19,540 lines; "31%" of the code base excluding validation (DERIVED 30.7%) | SS Fig. 6, §8.2, pp. 846-847 |
| Reference models | 450 lines; "1% of the implementation" (DERIVED 1.02%) | SS Fig. 6; §1, p. 837 |
| Checks: functional / crash / concurrency | 4,860 / 2,661 / 901 lines | SS Fig. 6, p. 847 |
| Total code base | 72,460 lines | SS Fig. 6, p. 847 |
| Validation share | text: 13% of total and 20% of implementation; §1: 12%; DERIVED: 12.2% and 20.1% | SS §8.2, p. 846; §1, p. 837 |
| Issues prevented | 16: 5 functional, 5 crash consistency, 6 concurrency | SS Fig. 5, p. 847 |
| Random sequences before each deployment | "tens of millions" | SS §4.2, p. 841 |
| Minimization example (#9) | 61 ops (9 crashes, 14 writes, 226 KiB) → 6 ops (1 crash, 2 writes, 2 B) | SS §4.3, p. 842 |
| Extents per disk | "tens of thousands" | SS §2.1, p. 837 |
| Data stored during rollout (2021) | "hundreds of petabytes" | SS §1, p. 836 |
| S3 durability design target | eleven nines | SS §2.2, p. 839 |
| Chunk framing | 2-byte magic plus a random UUID at both ends | SS §5, pp. 843-844 |
| Loom scaling limit | small test: tens of thousands of atomic steps; largest: over 1 M | SS §6, p. 844 |
| Formal-methods effort | 2 experts full-time for 9 months, plus 1 for 3 months | SS §8.2, p. 846 |
| Adoption by engineers | 18% of harness lines last edited by non-experts; 3 engineers >100 lines each; 4 wrote model-checking harnesses | SS §8.2, pp. 846-847 |
| Formal-verification overhead, for comparison | 3–10×; VeriBetrKV 7 proof lines per implementation line | SS §8.2, p. 846; §9, p. 848 |
| HDD random I/O budget | "about 120 operations per second" (NON-PEER-REVIEWED) | WARFIELD23 |
| Shuttle | 0.9.4 (2026-09-22), Apache-2.0, active (NON-PEER-REVIEWED) | SHUTTLE, observed 2026-09-28 |
| Loom | 0.7.2 (2024-04-23), MIT, last commit 2026-02-20 (NON-PEER-REVIEWED) | LOOM, observed 2026-09-28 |

### §3–§4 DynamoDB and formal methods

| Quantity | Value | Source |
|---|---|---|
| Peak DynamoDB request rate, Prime Day 2021 (66 h) | 89.2 M requests/s | [DDB Abstract, p. 1037] |
| Availability SLA | 99.99 (regional), 99.999 (global tables) | [DDB §1, p. 1038] |
| RCU / WCU definition | 1 strongly consistent read/s of ≤4 KB; 1 write/s of ≤1 KB | [DDB §4, p. 1040] |
| Burst capacity retention | up to 300 s | [DDB §4.1.1, p. 1041]; also DDB-DOCS (NON-PEER-REVIEWED) |
| Adaptive capacity effect | eliminated >99.99% of skew throttling | [DDB §4.1.2, p. 1042] |
| GAC token replenish interval | "in the order of few seconds" | [DDB §4.2, p. 1042] |
| Replicas per storage node (latest generation) | "thousands" | [DDB §4.3, p. 1042] |
| Split-for-consumption duration | "usually ... in the order of minutes" | [DDB §4.4, p. 1043] |
| On-demand instant headroom | up to 2x previous peak | [DDB §4.5, p. 1043] |
| Unarchived WAL per replica | "a few hundred megabytes" | [DDB §5.1, p. 1043] |
| Heal a storage replica / add a log replica | "several minutes" / "only a few seconds" | [DDB §5.1, p. 1043] |
| Write quorum | 2 of 3 replicas in different AZs | [DDB §6.1, p. 1045] |
| Paxos groups per Region | "millions" | [DDB §6.1, p. 1045] |
| Lease wait after election | "a couple of seconds" | [DDB §6.2, p. 1045] |
| Availability measurement window | 5 minutes | [DDB §6.3, p. 1045] |
| PITR window; backup cross-partition consistency | 35 days; "up to the nearest second" | [DDB §5.5, p. 1044] |
| Old router cache hit rate; metadata spike | ~99.75%; "up to 75 percent" (base unstated) | [DDB §6.6, p. 1046] |
| YCSB setup | 900-byte items, uniform keys, 100 K to 1 M total ops/s, production, "North Virginia region" | [DDB §7, p. 1047] |
| Allocation example | cap 1000 WCU; 3200 → 4×800; 3600 → 4×900; 6000 → 8×750; 5000 → "675" (DERIVED: 625) | [DDB §4, p. 1041] |
| Per-partition throughput maximum | 3,000 RCU and 1,000 WCU | DDB-DOCS (NON-PEER-REVIEWED) |
| TLA+/PlusCal spec sizes | S3 804 and 645 PlusCal; DynamoDB 939 TLA+; EBS 102 PlusCal; lock manager 223 PlusCal, 318 TLA+ | [FM p. 69] |
| DynamoDB data-loss bug trace | shortest trace 35 high-level steps | [FM pp. 70-71] |
| Model-checking cluster | 10 × cc1.4xlarge (8 cores + HT, 23 GB RAM each) | [FM p. 70] |
| TLA+ adoption (at writing) | 10 systems, 7 teams, 2-3 weeks to learn | [FM p. 68] |
| S3 scale cited by FM | 1 T objects (six years after the 2006 launch), 2 T objects and 1.1 M req/s <1 year later | [FM p. 66], from AWS blog posts (NON-PEER-REVIEWED underneath) |

### §5 and §8 AWS cell guidance and shuffle sharding

| Quantity | Value | Source |
|---|---|---|
| CELL-WP publication date | 2023-09-20 (only revision) | CELL-WP p. 52 |
| Blast-radius example | 10 cells: 90% of requests unaffected when one cell fails | CELL-WP p. 5 |
| "When to use" thresholds | RPO < 5 s; RTO < 30 s | CELL-WP p. 14 |
| Logical buckets in the two-level mapping | "tens of thousands" (example) | CELL-WP p. 26 |
| Example cell capacity | 10K TPS (illustrative) | CELL-WP pp. 37, 40 |
| Statically stable overprovisioning | 3 AZs: +50%; each AZ at 66% of its load-tested level | BL-STATIC pp. 4-5 |
| Same, instance example | 6 instances needed → 9 deployed across 3 AZs | FIB p. 12 |
| Chance of avoiding an impaired AZ | regional→regional 4/9; N regional hops (2/3)^N; zonal stays 2/3 | BL-STATIC p. 8 |
| Data plane vs control plane fleet size | "frequently by a factor of 100 or more"; UDP-initiated connections at ≥ 1000× | BL-SMALLER pp. 1, 7 |
| Constant-work result set | 10,000 results sent even when 10 checks are configured (example) | BL-CONSTANT p. 4 |
| Route 53 shuffle sharding | 2048 virtual name servers; k = 4; C(2048,4) = 730,862,190,080; ≤ 2 shared per pair | BL-SHUFFLE p. 6; DERIVED |
| Packing bound for Route 53's guarantee | C(2048,3)/C(4,3) = 357,389,824 | DERIVED |
| BL-SHUFFLE example | 8 workers, k = 2: 28 shards; 1/28 fully impacted; ~46% degraded | BL-SHUFFLE p. 6; DERIVED |
| SS-BLOG counts | "56" and "1/1680" are ordered counts; unordered 28 and 1/70 | SS-BLOG; DERIVED |
| Shuffle sharding in a peer-reviewed system | pairwise overlap Σ_e x_ei·x_ej ≤ O; shard size G_min..G_max | CYBERSTAR p. 244 |
| n = 64, k = 4 isolation | fully impacted 1.6×10⁻⁶; degraded 0.233; covered by 2 poison tenants 1.1×10⁻⁴ | DERIVED |

### §6 S3 internals

| Quantity | Value | Source |
|---|---|---|
| Documented per-prefix request floor (general purpose) | ≥3,500 PUT/COPY/POST/DELETE/s and ≥5,500 GET/HEAD/s "per partitioned Amazon S3 prefix"; no limit on prefix count | S3UG-PERF; S3-WN18 (Jul 17, 2018) |
| Documented example | 10 prefixes → 55,000 read requests/s | S3UG-PERF |
| Rate that may draw 503 on few objects | "typically sustained rates of over 5,000 requests per second to a small number of objects" | S3UG-PATTERNS |
| Documented split example | 4 leaf partitions × 5,500 GET/s = 22,000 TPS | STG314-23, slide 38 |
| Pre-2018 thresholds | Key-naming guidance above 100 PUT/LIST/DELETE/s or 300 GET/s; support case for rapid growth above 300 PUT/LIST/DELETE/s or 800 GET/s | S3-RRPC17 (2017 snapshot) |
| 2012 planning figure per partition | "100 operations per second and 20 million stored objects" | S3-BLOG12 |
| 2012 split frequency | "dozens of times a day all over S3" | S3-BLOG12 |
| Retry guidance | <512 KB: retry after 2 s, then after another 4 s; >128 MB: retry slowest 5%; fixed-size: slowest 1% | S3UG-PATTERNS |
| Typical latency and bandwidth (general purpose) | "roughly 100–200 milliseconds" small-object latency; single-instance transfer "up to 100 Gb/s" | S3UG-PERF |
| Directory bucket default TPS | 200,000 reads/s and 100,000 writes/s per bucket | S3UG-XPERF; S3UG-DIRB |
| Directory bucket raised TPS | up to 2 million reads/s and 200,000 writes/s | S3UG-XPERF |
| Directory buckets per account | 100 (default) | S3UG-DIRB |
| Directory bucket inactivity | ≥90 days idle → inactive; reactivation "typically within a few minutes", 503 meanwhile | S3UG-DIRB |
| CreateSession token lifetime | five minutes | S3-BLOG23X |
| Strong consistency launch | 01 DEC 2020 | S3-BLOG20 |
| Per-object update rate claimed | "hundreds of times per second" | S3-BLOG20 |
| Objects / request rate (Apr 2021) | "well over 100 trillion" / "tens of millions of requests every second" | VOGELS21 |
| Objects / request rate (as of 24 Jul 2023) | ">280 trillion" / "over 100 million requests per second" (average) | WARFIELD23 (stats table image) |
| Checksum computations | "over 4 billion ... per second" (2023) | WARFIELD23 (stats table image) |
| Objects / index request rate (Dec 2023) | 350 trillion / 100+ million per second | STG314-23, slide 29 |
| Front-end traffic peak (2023) | "over 1PB/sec" | STG314-23, slide 12 |
| Microservices | "hundreds" (2023 post); "350+" (Dec 2023) | WARFIELD23; STG314-23, slide 9 |
| Objects / request rate / footprint (Mar 2026) | ">500 trillion" / ">200 million requests per second" / "hundreds of exabytes", 123 AZs, 39 Regions | S3-BLOG26 |
| Drive count | "Millions" (2023); "tens of millions" (2026) | WARFIELD23; S3-BLOG26 |
| HDD random IOPS | "about 120 operations per second" | WARFIELD23 |
| I/O density at 200 TB drives | "1 I/O per second per 2TB of data on disk" | WARFIELD23 |
| ShardStore executable model size | "about 1% of the size of the real system" | WARFIELD23 |
| S3 at launch (2006) | ~1 PB, ~400 storage nodes, 15 racks, 3 data centers, 15 Gbps | S3-BLOG26 |
| Durability design goal | 11 nines (99.999999999%) | S3-BLOG26; STG314-23, slide 48 |

### §7.1–§7.3 Slicer, Centrifuge, Shard Manager

| Quantity | Value | Source |
|---|---|---|
| Slicer production request rate | 2-7M req/s (median 2 Mreq/s, peaks >7 Mreq/s over a week); 6M req/s in the SLI §5.1.3 table | [SLI Abstract, Fig. 1, p. 739; §5.1.3, p. 749] |
| Slicer resource saving vs static sharding | median workload uses 63% fewer resources; hottest-task load reduced by median 63%, up to 99.3% | [SLI Abstract, p. 739; §5.1.2, p. 749] |
| Slicer fleet | 22 services, 263 jobs, 11,387 tasks, 113,338 Clerks, 662 assignments/hour, 180 MBps assignment traffic, 4% key churn/hour | [SLI §5.1.3, p. 749] |
| Slicer hashed key width | 63 bits | [SLI §2.1, p. 741] |
| Slicer imbalance metric | max task load / mean task load; worst case n/r | [SLI §4.4, p. 745] |
| Weighted-move constants | merge while >50 slices/task and ≤1% keyspace moved; move budget 9% of keyspace; split slices ≥2× mean slice load while <150 slices/task | [SLI §4.4.1, p. 746] |
| Rebalancing suppression | max task CPU load <25% ("an arbitrary threshold") | [SLI §4.4.2, p. 746] |
| Load-aware consistent hashing | ~1000 virtual nodes per task needed; 18 months in service; hottest task 50% hotter than mean | [SLI §4.4.4, pp. 746-747; §5.1.2, p. 748] |
| Production max/mean load | 1.3×-2.8× | [SLI §5.1.2, p. 748; §7, p. 752] |
| Keyspace moved per hour | median hour <20% in every job; Cloud DNS up to 40%; Flywheel 16% | [SLI Fig. 7, §5.1.2, p. 748] |
| Routing success (client side) | 99.98% of 260 billion task selections | [SLI §5.1.1, p. 748] |
| Misrouted requests (server side) | 11.6 million of 272 billion (0.004%) | [SLI §5.1.1, p. 748] |
| Distributor probe success | 99.75% of 329,978 | [SLI §5.1.1, p. 748] |
| Assignment propagation | 95% <1.7 s, 99% <5.9 s, 99.9% <9.0 s | [SLI Fig. 10, §5.1.4, p. 749] |
| Assignment computation | 64th percentile 17 ms; max a few seconds | [SLI §5.1.5, p. 749] |
| Slicer control-plane cost | 6 Assigners × 3 cores; median 0.13 core, p99 2.34 cores; whole service 0.3% CPU and 0.2% RAM of sliced services | [SLI §5.1.3, p. 749] |
| Assigner failover | 17.1 s (σ = 2.7 s) | [SLI §5.2.2, p. 750] |
| Load-shift reaction | median 480 s (1 min monitoring delay + 5 min observation window); 99% <719 s | [SLI §5.2.3, Fig. 13, p. 750] |
| Central-authority saturation (benchmark) | 5 Kreq/s | [SLI §5.2.4, p. 751] |
| Guard-lease recall period | median 2.6 s, p99 4.1 s | [SLI §4.5, p. 747] |
| Bridge-lease benefit | 99.85% vs 99.19% requests satisfied over three days | [SLI §5.2.5, p. 751] |
| Slicelet handle calls | getSliceKeyHandle 153 µs; isAssignedContinuously 94 µs | [SLI §5.2.5, p. 751] |
| Centrifuge lease / renewal / clock-rate bound | 60 s / 15 s / Manager ≤65 s per 60 s of Owner time | [CEN §2.1.2, p. 4; §2.3, p. 5; §2.3.1, p. 6] |
| Centrifuge Lookup poll; change-log retention | 30 s; 5 min | [CEN §2.2.1, p. 5; §2.1.2, p. 4] |
| Centrifuge table size | 64 virtual nodes/Owner, 32 B/range, ~200 KB for 100 Owners; 2 KB per lease message | [CEN §2, p. 3; §2.2, p. 5; §2.3, p. 6] |
| Centrifuge Manager deployment | 5 Paxos servers + 3 standbys | [CEN §2.1.1, p. 4] |
| Centrifuge scale target; production size | cluster ≤1000 machines; ~130 Owners, ~1,000 Lookups | [CEN §2.4, p. 7; §5, p. 11] |
| Centrifuge unplanned lease loss | 10 losses / 130 Owners / 1.5 months; per-Owner mean time 19.5 months | [CEN §5.1.1, p. 12] |
| SM adoption | ~54% of Facebook sharded apps; >1M servers; nearly 100M shards; billions req/s | [SM Abstract, p. 553; §8.1, p. 564] |
| Planned vs unplanned container stops | ≈1000× | [SM §1.1, p. 554] |
| Static sharding vs consistent hashing popularity | ≈3× | [SM §2.2.1, p. 555] |
| Custom-sharded stores | 1% of apps, 27% of server usage | [SM §2.2.1, p. 555] |
| Apps draining shards before restart | about 70% | [SM §2.3, p. 557] |
| Laser | nearly 1B queries/s at peak; 9% prefix scans | [SM §3.1, p. 558] |
| ZippyDB on SM | since 2013; most deployments 1 primary + 2 secondaries; LB on CPU, storage, shard count | [SM §2.5, p. 558] |
| SM partition size | thousands of servers, hundreds of thousands of shard replicas | [SM §6.1, p. 562] |
| Mini-SMs | 139 regional + 48 geo-distributed; largest ≈50K servers, ≈1.3M shards; 18-core/64 GB; P90 CPU ≈30%, P90 memory ≈38% | [SM §8.1, p. 564] |
| Largest SM deployments | ≈19K servers, ≈2.6M shards | [SM §8.1, p. 564] |
| Graceful-migration experiment | ≈100% success with SM; ≈98% without graceful migration; <90% with neither (800 s vs 1,500 s upgrade) | [SM §8.2, p. 564] |
| Solver scaling | 75K shards/1K servers → 375K/5K: 30 s → 205 s; production P90 ≈10 s, P99 ≈50 s; 1.9 billion variables max | [SM §8.4, p. 565; §9, p. 566] |
| MIP vs requirement | millions of variables in tens of minutes vs billions in tens of seconds | [SM §5.2, p. 562] |
| Example LB goals | utilization ≤90%; ≤10% above average utilization | [SM §5.1, p. 561; §8.4, p. 565] |
| ZippyDB LB in production | 12K machines; P99 CPU <80% | [SM §8.4, p. 566] |

### §7.4–§7.7 WAS, Aurora, CockroachDB, Dynamo

| Quantity | Value | Source |
|---|---|---|
| WAS stamp shape | 10-20 racks × 18 storage nodes per rack | [WAS §3.2, p. 144] |
| WAS stamp raw capacity | ~2 PB (first generation); up to 30 PB ("20-30PB") next generation | [WAS §3.2, p. 144; §8, p. 155] |
| WAS production raw storage (2011) | 70 PB | [WAS §3.2, p. 144] |
| WAS stamp utilization | target ~70%; avoid >80% (20% reserve); migration triggered at 70% | [WAS §3.2, p. 145] |
| WAS SM metadata bound per stamp | ≤50 M extents, ≤100 K streams, fits in 32 GB | [WAS §4.1, p. 147] |
| WAS extent target size; block size | 1 GB; up to N bytes, e.g. 4 MB | [WAS §4, p. 146] |
| WAS seal + new-extent allocation | ~20 ms on average | [WAS §4.3.1, p. 148] |
| WAS erasure-coded overhead | 1.3x-1.5x | [WAS §4.4, p. 148] |
| WAS spindle anti-starvation | no new IO if >100 ms expected pending or any IO pending >200 ms; lockouts up to 2300 ms seen without it | [WAS §4.6, p. 149] |
| WAS commit-log append latency | 30 ms without journal → 6 ms with journal | [WAS §4.7, p. 149] |
| WAS RangePartitions per PS | ~10 on average | [WAS §5.2, p. 150] |
| WAS RangePartition count | high watermark ≈ 10 × PS count; a few hundred PSs per stamp | [WAS §5.5, p. 151] |
| WAS operations per stamp per day | ~75 splits and merges; ~200 load balances | [WAS §5.5, p. 151] |
| WAS PM split pass | every 15 s; "a small number" of splits per quantum | [WAS §8, p. 154] |
| WAS geo-replication lag | within 30 s on average | [WAS §5.6, p. 152] |
| WAS max account size | 100 TB | [WAS §8, p. 155] |
| Aurora segment / protection group | ≤10 GB; V=6, Vw=4, Vr=3; 2 copies × 3 AZs | [AUR18 §2.1, p. 790] |
| Aurora segment repair | 10 GB in 10 s on a 10 Gbps link | [AUR17 §2.2, pp. 1042-1043] |
| Aurora segments per 64 TB volume | 38,400 (DERIVED check: 64 TB / 10 GB × 6 = 38,400) | [AUR18 §4, p. 794] |
| Aurora full/tail quorums | write: 4/6 any OR 3/3 full; read: 3/6 any AND 1/3 full | [AUR18 §4.2, p. 795] |
| CockroachDB leaseholder rebalancing interval | every 10 min by default in large clusters (NON-PEER-REVIEWED) | [CRDB-DOCS-REPL "Leaseholder rebalancing"] |
| CockroachDB load-split threshold | 2500 QPS default (NON-PEER-REVIEWED) | [CRDB-DOCS-LBS] |
| Dynamo imbalance measurement | 15% threshold; imbalance ratio ~20% at low load, ~10% at high load | [DYN §6.2, p. 215] |
| Dynamo Strategy 3 vs 1 | better efficiency; membership info smaller by three orders of magnitude (S=30, N=3) | [DYN §6.2, pp. 216-217] |
| Dynamo Strategy 2 bootstrapping | "almost a day" in busy season | [DYN §6.2, p. 216] |
| Dynamo client membership refresh | every 10 s (pull), immediately on detected staleness | [DYN §6.4, p. 218] |
| Dynamo 99.9th-pct read latency | 68.9 ms server-driven vs 30.4 ms client-driven | [DYN §6.4, Table 2, p. 218] |
