# R2-F3 TickFlow Free Daily Bar Shadow qualification report

Status: `SHADOW_QUALIFIED / FINAL INDEPENDENT REVIEW PENDING`

This is a sanitized, hash-only review projection of the immutable local sidecar and bundle graph. It contains no provider payload rows, token, request URL, query string or sensitive exception text. The authoritative local report is:

- Path: `/Users/finlay/Library/Application Support/Stock EVA/r2f3-daily-shadow-20260829-rate-limit-v3/audit/final-qualification-report.json`
- SHA-256: `9bade0b8c669cf068a4e94b8cd45cd1e1851367680142995a605535343ead3f7`
- Reviewed code: `ff4b6cd2579726e284ab2db6ddd56b66283d5441`

## Qualification result

- Window: 2026-07-14 through 2026-08-10, 20 consecutive confirmed Shanghai sessions.
- Sidecar: `READY`; epoch: `SHADOW_QUALIFIED`; circuit: `CLOSED`.
- Requests: 640 sequential one-attempt shard requests; 0 failures and 0 rate limits.
- Coverage: 63,800 expected and 63,800 observed session symbols.
- Reconciliation: 255,200/255,200 OHLC price cells within 0.01; maximum observed delta 0.
- Evidence policy: only final successful source evidence is retained.
- Every continuation full-session start was at least 60 seconds after the preceding start; the minimum observed interval was 60.001 seconds. This is a caller-owned safety policy, not a provider quota claim.
- Canonical publication, provider admission, publication selection and automatic failover were not changed.

## Frozen contract

- Descriptor: `0f0ab141f9b7fbd831a6081d8c731c4765db57ebda3a929ebdaaa1f9468c1911`
- Descriptor base: `00d86cb0720f3381c1b41c8bc28f951e6e9feab6e94a829e66e3ba45d538e10f`
- Circuit policy: `b3d4cdfa56b5db6bbc719514b35da4d430af75edf3de2c56c41fb393e8708495`
- Terms evidence: `667916470cdceb743637e8e090513b8603ad8690464e4c537a85dddfe816728c`
- Calendar: `r2f3-calendar-authority-v1-7955ac6f3efb3885` / `5c115f1ea7dd05a7d90a08eeed046bf6ac61b8cb1c4bcc514bb2de0bd55c07dc`
- Version vector: `964f79573fafa8a44734ce64da70ebad4c5eb9485e5616a48d8e1d499eab496d`
- Provider/profile: `tickflow / TICKFLOW_FREE_DAILY_BAR_OHLC_V1`.
- Timezone/frequency/adjustment: `Asia/Shanghai / 1d / none`.

## Per-session immutable identities

| Date | Symbols | Report SHA-256 | Evidence SHA-256 | Candidate SHA-256 | Quality SHA-256 | Reconciliation SHA-256 |
| --- | ---: | --- | --- | --- | --- | --- |
| 2026-07-14 | 3190 | `523d5e6fba4808bd6c60f30cd2428b29a537070a1a42144e93dc8eac39ecdcf9` | `91cb81ceaa2876a891a3886cb0e930da00dcbb51496687680d316ef62719ff9f` | `3dbe9d1c6aa7580038a5b45cf81cc7946cce51b13bf9045add81d9baf66ffed8` | `db234afc70d8c9c6bf973375d119b78d8c48ef0f22c0039b4b5a25965843becc` | `6d0c30ae44db1a5a46e3db3eb43510fe6c0941bc23f82c5100394f311dd71c8a` |
| 2026-07-15 | 3191 | `a8948f3acd0be87b289019be0464013f99f5b3d86884fccb98646e45bda1d1b6` | `03a63d9bad89c8f9c60c02e8c45eb73de267f54e297001bca56143a8dd8601d3` | `beffe8047daeebb1397774cbce334dfc5e2f95eda9880c4f6b4856f18cb68b26` | `749ab010b8a0371684ea85b6d744579d3e32c73a36cc16b42a5bfe9b95314a2b` | `0f8de85b53ae19cb374de9ea66c65076cfc1488c4b0306cfb0af07ffb7b3bad6` |
| 2026-07-16 | 3191 | `cd184f82e6771dd20442be065ff61fbaff6dc0e5f8da45f450b569825f3f0f6f` | `2a14e3404f428e7b269078665c67494864834154e5a96a6846eb8a5df049896b` | `080ef6bb03be4b7fde531c6602ac66eb9a699ebf909d66537b7bd6be1b6bc3bd` | `7cbb25f8b8d96ef64b6bb5f57f39a56ddc7ebd3fec0d0d778ea45c021cee7c48` | `af683291a1aac30a71f4db638cc41a19d9bb1f24c1fdc770c84a4fc5942b8c27` |
| 2026-07-17 | 3189 | `3975fd7de9f8cbc94d323e1ff73ce1913d721443a02e44cb717cd40fd64ec9ce` | `b16aabace3490f02fba59f17c312e54283ac692858d6df324d3650fb8fbd9dbc` | `1d450b9e525ae90da13dd0dd6d4ac839d57c978b78769d37fd615372d7bdef82` | `053d7885f1922016a213a1e58a3e26672c3d530eef7567443af5dd22ca4da082` | `3bff9a880b1304e7506cb7b29302cabbae4529e48d8dd76e38295b865961a6fd` |
| 2026-07-20 | 3191 | `b80588c86b087e3d1eb5858d6140f87798a7746b8d2ab5490978afc5ea18c40d` | `ac8d368bdf9b8a0d2709753f84386e37f0d0b074660fac739c5adbf33147399a` | `03ee1cfc607554d683f5a1f9b10045fccf3357da075e4f9388679c89c06c1f78` | `85168ac1a59ecf50be5ba88efa3fe23d2e37cd78f4191a0ddfb4f0b2fdb36e8e` | `6c29ab680df1334b808bd0da344649c3d7ddf99f819ee97e0b0ba0d490dc5fe0` |
| 2026-07-21 | 3191 | `86d8bfbf6b60dd476b99c42e1acb4313afe22e265cc0067f43ee4dd6d5cb6be5` | `a136541823dbaca369bd011fa0f1721acfe22f074bb7e04b8c641c27cf815977` | `a1b63e76c62460a472bddaa631f12a269c2291f596b8bee8d72e127648e52a0b` | `89345c4bc909f02f236cfc1f34ca660d17d7682cde035892d6a80c3fdf369da6` | `23200d578b8a42af1e1f05f618431ec285e7d17bea734acadb7af520f0858597` |
| 2026-07-22 | 3190 | `c431e0a5a5442d4c57e4e354ced26f53456d7e04a34a9724b1f7310c3e0c80d6` | `a99af06a8579e20ebdeef1c055978fd763f2fd3d21bc1022ca8814481c5ee9e2` | `80bebf2050633ce2ebf3bb2a17aa379ed62c771743312fde815c8fa693982718` | `ccd220b721979e04d397d5fb343effc8344ccc65068182907a964150daf81b35` | `82cfda1f02f0b889402da605607f76a1885b85f8973683b639434757af289e6b` |
| 2026-07-23 | 3190 | `03fdb6bd855f296adef510f8f865bfb51e6db4de472c12e6ab30def4f8044487` | `c2f666585a6f52cffa7e4b2621a65fa57f1da9b02f7364ea43976cbc99bb3b25` | `7d6382c0b8dc7a5773e6773c6502a03438de45f8e261d1bc95901f7275c5a4fd` | `8fb8422ce7bf0a5de74bb7c2c1d3bb795fccbbc5256bb09d378e9e65d8a17072` | `d118104d9ed8a0ba5ab4f9267a2e929b0fcbde1eda8f556ee54236740335ee47` |
| 2026-07-24 | 3190 | `36b9cb4b2a3b68ea0643d4e7f03ae4c7504a87b59c57f098a77838e6e0c54f8c` | `606d99b8425bda5018a82371e45d589f06daadbf9cdc2e8231c8f1a532c162b0` | `3743103e49f036090583268560df293a244970338fcfd50da27661d2cbb12e1b` | `efb0d21d3ca4543b8faedcd0ca540de757dc54bedfd573c8f50a2c7707fbb479` | `625ee0f1dd9072290ccf310dfa74b0d28e3f25eb6a57cdfeb1a2ac1f1af6c531` |
| 2026-07-27 | 3189 | `20e4b198ef75d09aa63b1771fd2d417026807761b246476d5a63406babc99972` | `236e6e495d0228866715a83c14ee2240475605e4500f81dbffc550d2f47ab849` | `604a82a9ce8bbaf36b03b3044aa1806c2cdef50219e20c9caefab625b1bea380` | `612ecd58b6d88933c77ca5f0f7dc461578b3291aad4ada00ecac02f16be5cea0` | `f18e47fe3409d0bcce64821c5798556411a0174fbd9fe579150920f378273e09` |
| 2026-07-28 | 3189 | `943f6c79f8fd988ccff0279d8110ffa6e0b789e06986eb3c6f7c24bd00bbbd04` | `f53767e3a6799a8894aaa991cdf64006be71328b15e3646a9baf5018608374c6` | `c263bf4abbc5cd2bfd3eabea7c22464067fa0e15fe77096b2590eefbf3fa086d` | `fdc018bf4c7466430d95c6507014900fd5989672f8018b54aabcae25ac940a9b` | `a2f8ca1b69531f2ce2b752d9c559e5cbbcf996b2455a01d6b57e1a48d2a24a4a` |
| 2026-07-29 | 3188 | `b4767ed8fb71d91ade6d31d7b6948d492b9a11585d71b17450fc0bfe3132bef6` | `d1089d7ed1535621e935819ac37093057cabd594492eafae695b6f46efe81020` | `302f357e5e8817d25f6a5958fa0e9885fdb5b6a0b2fae3467b7af635aad1d99d` | `efc7fb8e9a573f62589ca864ddbe52debf1a00a2c6b558744926436757ae5e00` | `746b145bfc85ff8095bb4adb15cfc0b0f6d1ca12f62bc4f294263943c7083abe` |
| 2026-07-30 | 3189 | `fdf0defcb13a8b36964472dfdb0a03c9b13b65d49bf8f1fffe5157bbfd97cd11` | `c80706e487cb7c3242b36adb1fd2039aa39de1acb2ec40227dbc4b8b5ca63bc9` | `78cb2d1739b1d02d25191cf71a3ae5ea680ef670eec9ff945f80184d40831ac3` | `c4e9300a888bc2f7002cc672c0cf0ff643cec00eee617bcadc5aedd33da0bfdf` | `b8c375da87cce5c15847ef721c5869b23e340c470b98c3dc80406b74bab4f1a3` |
| 2026-07-31 | 3188 | `b2b87df6f5fc1f6bee8a386df6a563b437923ff192ad8b089107ce393febb5b7` | `97866bb5ea963ae50b0115428058bd9d48b158646e8e275e839905ae18787fe5` | `6d6ac9b5b53697526a7f696f25dde281072c79b4c55a68a49e7f032d2d2409e1` | `7126482a835fea260644fd887c05d86c63207c7debbaed23b379a9014a42f3cb` | `b67f2c1185948f561bfffa0b66b826104c1549f21bae52b51e63efb499cd888e` |
| 2026-08-03 | 3188 | `22a292948771e0f731b50e2693cfeaa3d6884904cd9f8c090e7dc89acfd6072f` | `840567b6cbde05ac2b24dac9ee32998a8dda2d74697fe85c8c651385c2cecc31` | `82bb23b6f18d61455bb6472b87d046392fa9305a936e3ae69b3aa47c2854fdde` | `6973e1ae4e73d962f39c3869ca5d2ee9895739616b23cf0ee84817fdf7d43c3c` | `31785fee18aba8312f6112194d84c1c2edc9d9483aa8bb348f6221b5c507a006` |
| 2026-08-04 | 3189 | `55045c579768402567d3ae4a1f9ffa73b2268959acd947e913ff997087b7f282` | `07199ae5130a433df205d98c92b37b756d0c0061e4d33e837edb65a11eb0ba98` | `453de03e53f5835aff1ab8f4dddd719c26a3a6daa99d16d595adf4a0a0fb3f88` | `ef13929f378aecd7ee180819e1dc3fbef5f6ef4a54fdba4a0abcc48ea8cc72d6` | `8818867d56465c7f233ce79ed541124e6fa4fed3f2e9839e919cd58f052dd418` |
| 2026-08-05 | 3190 | `0e012bee9e9c14972c13edd1ecc8c2814c6f59a88b273f06804de1ba6b5d63d8` | `fdf60cd07debb6f45f5a82931f7a0f4a4ae8246232262de882b4db479570b585` | `a3695f128ead44d2ca721339496c3b15740e242cf40a6ae3f1758ec310888c15` | `ceb9eab7f41b6a8c06b39a8f7cc42c9454bc5efa03e5f08489f001588f2302db` | `92a5ecdf9257eac3cb1ddf5ce4e6d456245f45e42fe7f514abfe519778805c66` |
| 2026-08-06 | 3192 | `3844f3c9abe3338559259ba0b1dd3a9730207a6fb7865e325e8cad41aba09691` | `a4ca1f7ac4280070b5a78429268f68a505551724fb53176f2b6955c2980db583` | `2552aabcd7eb53f782db61bae5643f77d495cdb2a8fc65ab29ece34d599c85b3` | `a4eba2074b1207d983c6ae5ce92038c2a40bf252f9629d31a26c4018e155c872` | `ce1861b2b659afc0eb3fdabfccfca3537bb00d5e54c294faff8c4d2a08f6d52f` |
| 2026-08-07 | 3192 | `2ca4d8f4145cb1273ab8d18dfae5b429bf3f62fbae3fa5e4dd2db23751f79424` | `32ca44b68c325cc4a6ed2193e0fc32bb269d7007c97cd0fe18e0b3d7fb28b8ce` | `b2d82e42f26fbbe0c9f28ac7049d0ca5cd2782915efdefa4b11eac710a4f615d` | `dc3966d550432d8cd2c647c823f1d59c5fa53e2f1f9c42f34e3103a5ea909b0d` | `18b8caf0c245fe96a3421b40b93a07c878545f0fc070d6a1750227567b5cefc2` |
| 2026-08-10 | 3193 | `7075b5b2f63c379270c7d8705f4d99d9f8fad94bc2804b85c9f9b8561e2e706c` | `e29764de4233419a7a83270d48af0105449694e90e33564e51e3abdc12c3e5d1` | `f951aa11885f86520a4b86e27192c60defd90d225562c17c0065cf295a4044c5` | `f235b80075ff063a1dcb579c993e09ce0ecd823e1633e634837760c6d95e9863` | `1e3023e91d4caad4d25f394c16247c5376fea8ed56d099f1dcd92dd39b528990` |

All 20 rows have 32 attempts, attempt number 1 only, exact expected/observed coverage, quality `PASS`, reconciliation `PASS`, no failure class and no partial candidate publication.

## Preserved diagnostic chain

| Runtime | Terminal state | Reports | Attempts | Transport failures | Rate limits | Purpose |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| v1 | `RESET / MISMATCH` | 1 | 32 | 0 | 0 | Proved UTC-versus-Shanghai trade-date defect. |
| v2 | `RESET / FAILURE` | 4 | 125 | 1 | 1 | Proved accelerated-run HTTP 429 and the first-rate-limit circuit defect. |
| v3 | `SHADOW_QUALIFIED` | 20 | 640 | 0 | 0 | Proved the rotated contract under conservative pacing. |

Across all three preserved runtimes there were 797 attempts: 796 successful transport attempts and one rate-limited attempt. This does not establish an official quota or recovery window.

## Protected-state readback

- Canonical manifest SHA-256 remains `052799bddd785c8a4204e0bc3352b16edaa636934d819a156f247bb8393873a7`.
- All 271 manifest-declared Parquet hashes verified after the 20-session run.
- The v1 and v2 runtime aggregates and the Task14 provider-control aggregate remained unchanged throughout every session gate.
- Production provider registry, Daily sidecar and provider-health targets remain absent.
- The third runtime has no private permission violations: directories are 0700 and files are 0600.

## Qualification boundary

`SHADOW_QUALIFIED` applies only to credentialless TickFlow Free, historical unadjusted-requested A-share Daily OHLC, as a whole-session shadow candidate. It does not qualify adjustment factors, units, suspension semantics, raw-retention semantics, realtime quote, minute K-line, publication or automatic failover. Those states remain `UNQUALIFIED`, `UNKNOWN`, forbidden or disabled exactly as specified.

The remaining release gate is the same reviewer’s independent read-only audit of the exact clean commit and the authoritative local evidence graph.
