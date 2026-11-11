# Probabilistic and geographic commands

E06B provides PFADD, PFCOUNT and PFMERGE. Each sketch is an ordinary string with
a private versioned header and 16,384 one-byte registers. It uses the fixed-seed
MurmurHash64A algorithm, the usual HyperLogLog harmonic estimate and linear
counting for small cardinalities. The theoretical standard error is approximately
0.81% for suitable independent inputs, not a per-query guarantee. The estimator
differs from Redis and results can differ. GET/SET can copy a PyRedis sketch, but
the representation is **not compatible with Redis sparse or dense HLL strings**;
unrecognized or invalid sketches are rejected. PFADD preserves TTL; PFMERGE
includes its destination and preserves that destination's TTL. PFCOUNT does not
update a cached value. Multi-key work is bounded by `max_collection_bytes`.

E06C provides GEOADD (NX/XX/CH), GEODIST, GEOHASH, GEOPOS and GEOSEARCH with
FROMMEMBER/FROMLONLAT, BYRADIUS/BYBOX, ASC/DESC, COUNT/ANY and the WITHDIST,
WITHHASH and WITHCOORD reply options. GEOADD stores geohash scores in the existing
sorted-set type, so ZSCORE, ZRANGE and ZREM work on geographic members. Searches
scan the sorted set in O(N), with an O(N log N) sort when requested; they are
limited by configured collection bounds and reply size. GEOSEARCHSTORE is not
implemented. Ordinary geographic scores use 52 interleaved bits; Redis's exact
maximum-coordinate boundary can produce a score beyond that range. Arbitrary
non-geographic sorted-set scores and the full Redis error,
precision and date-line edge matrix have not been exhaustively verified.
The earlier audit claim that every geographic score must fit 52 bits was withdrawn:
Redis 7.4.12 uses scores 0, 2^52, 2^53 and 3*2^52 at the four exact coordinate
corners. Clamping would change Redis-visible sorted-set scores.

Exact dateline GEOSEARCH remains incompatible. With members at -180, +180 and
nearby points, Redis and PyRedis produce the same GEOADD and ZRANGE scores, but
Redis's geohash candidate ranges exclude some points that PyRedis's full linear
scan admits by distance. A search centered at +180 can exclude ordinary points near
-180 as well as the exact endpoint; a search just inside +180 can include those
ordinary points while excluding the extra-high-score +180 member. The behavior also
occurs with BYBOX. This is consistent with the upper-bound quantization's extra
score bits affecting candidate-cell selection, but the complete Redis range and
neighbor calculation has not been reconstructed. Correcting this safely requires a
general, bounded candidate-range implementation rather than filtering an endpoint
or imposing a 52-bit limit. No such correction is claimed here.

Both features use existing ACL key patterns, datastore accounting, transactions,
journal, snapshots and private replication effects. The ACL subset does not have
separate read/write key patterns. Temporary copies are bounded but datastore
accounting is not a Python heap or RSS ceiling. Release audit follow-up remains
open. See [validation](validation-and-limitations.md) for tested behavior.
